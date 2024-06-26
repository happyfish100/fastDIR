/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

//cluster_handler.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "common/fdir_proto.h"
#include "binlog/binlog_producer.h"
#include "binlog/binlog_pack.h"
#include "binlog/binlog_replication.h"
#include "server_global.h"
#include "server_func.h"
#include "dentry.h"
#include "server_binlog.h"
#include "cluster_relationship.h"
#include "replication_quorum.h"
#include "common_handler.h"
#include "cluster_handler.h"

static bool fdir_is_master_callback(void *arg, int64_t *data_version)
{
    FDIRClusterServerInfo *master;
    int64_t my_data_version;

    master = CLUSTER_MASTER_ATOM_PTR;
    my_data_version = FC_ATOMIC_GET(CLUSTER_MYSELF_PTR->
            confirmed_data_version);
    if (CLUSTER_MYSELF_PTR == master) {
        *data_version = my_data_version;
        return true;
    } else {
        if (master == NULL) {
            *data_version = 0;
        } else {
            *data_version = FC_MIN(my_data_version,
                    master->confirmed_data_version);
        }
        return false;
    }
}

int cluster_handler_init()
{
    const int process_interval_ms = 1000;
    const int master_side_timeout = 600;
    int result;

    g_server_global_vars->replication.active_test_interval =
        (CLUSTER_NETWORK_TIMEOUT + 1) / 2;
    if ((result=idempotency_request_metadata_init(&REPLICA_REQ_META_CTX,
                    fdir_is_master_callback, NULL)) != 0)
    {
        return result;
    }

    return idempotency_request_metadata_start(
            process_interval_ms, master_side_timeout);
}

int cluster_handler_destroy()
{   
    return 0;
}

static inline int replica_alloc_reader(struct fast_task_info *task,
        const int server_type)
{
    REPLICA_READER = fc_malloc(sizeof(ServerBinlogReader));
    if (REPLICA_READER == NULL) {
        return ENOMEM;
    }
    SERVER_TASK_TYPE = server_type;
    return 0;
}

static inline void replica_release_reader(struct fast_task_info *task,
        const bool reader_inited)
{
    if (REPLICA_READER != NULL) {
        if (reader_inited) {
            binlog_reader_destroy(REPLICA_READER);
        }
        free(REPLICA_READER);
        REPLICA_READER = NULL;
    }
    SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
}

static inline void desc_task_waiting_rpc_count(
        FDIRReplicaRPCResultEntry *rentry, const int err_no)
{
    FDIRServerTaskArg *task_arg;

    task_arg = (FDIRServerTaskArg *)rentry->waiting_task->arg;
    if (err_no == 0 && REPLICA_QUORUM_NEED_MAJORITY) {
        FC_ATOMIC_INC(task_arg->context.service.rpc.success_count);
    }

    if (__sync_sub_and_fetch(&task_arg->context.service.
                rpc.waiting_count, 1) == 0)
    {
        sf_nio_notify(rentry->waiting_task, SF_NIO_STAGE_CONTINUE);
    }
}

static inline void desc_replication_waiting_rpc_count(
        FDIRSlaveReplication *replication, const int err_no)
{
    FDIRReplicaRPCResultEntry *rentry;
    FDIRReplicaRPCResultEntry *rend;

    if (replication->context.rpc_result_array.count == 0) {
        return;
    }

    rend = replication->context.rpc_result_array.results +
        replication->context.rpc_result_array.count;
    for (rentry=replication->context.rpc_result_array.results;
            rentry<rend; rentry++)
    {
        desc_task_waiting_rpc_count(rentry, err_no);
    }
    replication->context.rpc_result_array.count = 0;
}

int cluster_recv_timeout_callback(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE == FDIR_SERVER_TASK_TYPE_RELATIONSHIP &&
            CLUSTER_PEER != NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster server id: %d, ip: %s, recv timeout",
                __LINE__, CLUSTER_PEER->server->id, task->client_ip);
        return ETIMEDOUT;
    } else if (SERVER_TASK_TYPE == FDIR_SERVER_TASK_TYPE_REPLICA_MASTER &&
            CLUSTER_REPLICA != NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "slave server id: %d, ip: %s, recv timeout",
                __LINE__, CLUSTER_REPLICA->slave->server->id, task->client_ip);
        desc_replication_waiting_rpc_count(CLUSTER_REPLICA, ETIMEDOUT);
        return ETIMEDOUT;
    }

    return 0;
}

void cluster_task_finish_cleanup(struct fast_task_info *task)
{
    SERVER_TASK_VERSION++;
    switch (SERVER_TASK_TYPE) {
        case FDIR_SERVER_TASK_TYPE_RELATIONSHIP:
            if (CLUSTER_PEER != NULL) {
                cluster_relationship_master_quorum_check();
                CLUSTER_PEER = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "CLUSTER_PEER is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        case FDIR_SERVER_TASK_TYPE_REPLICA_MASTER:
            if (CLUSTER_REPLICA != NULL) {
                desc_replication_waiting_rpc_count(CLUSTER_REPLICA, ENOTCONN);
                binlog_replication_rebind_thread(CLUSTER_REPLICA);
                CLUSTER_REPLICA = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "CLUSTER_REPLICA is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            if (TASK_PENDING_SEND_COUNT != 0) {
                TASK_PENDING_SEND_COUNT = 0;
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        case FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE:
            if (TASK_CONSUMER_CTX != NULL) {
                replica_consumer_thread_terminate(TASK_CONSUMER_CTX);
                __sync_bool_compare_and_swap(&REPLICA_CONSUMER_CTX,
                        TASK_CONSUMER_CTX, NULL);
                TASK_CONSUMER_CTX = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "TASK_CONSUMER_CTX is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        case FDIR_SERVER_TASK_TYPE_SYNC_BINLOG:
            replica_release_reader(task, true);
            break;
        default:
            break;
    }

    if (CLUSTER_PEER != NULL) {
        logError("file: "__FILE__", line: %d, "
                "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                "CLUSTER_PEER: %p != NULL", __LINE__, task,
                SERVER_TASK_TYPE, CLUSTER_PEER);
    }

    sf_task_finish_clean_up(task);
}

static int cluster_check_config_sign(struct fast_task_info *task,
        const int server_id, const bool auth_enabled,
        const char *config_sign)
{
    int my_auth_enabled;
    int req_auth_enabled;

    my_auth_enabled = (AUTH_ENABLED ? 1 : 0);
    req_auth_enabled = (auth_enabled ? 1 : 0);
    if (req_auth_enabled != my_auth_enabled) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "server #%d 's auth enabled: %d != mine: %d",
                server_id, req_auth_enabled, my_auth_enabled);
        return EINVAL;
    }

    if (memcmp(config_sign, CLUSTER_CONFIG_SIGN_BUF,
                SF_CLUSTER_CONFIG_SIGN_LEN) != 0)
    {
        char peer_hex[2 * SF_CLUSTER_CONFIG_SIGN_LEN + 1];
        char my_hex[2 * SF_CLUSTER_CONFIG_SIGN_LEN + 1];

        bin2hex(config_sign, SF_CLUSTER_CONFIG_SIGN_LEN, peer_hex);
        bin2hex((const char *)CLUSTER_CONFIG_SIGN_BUF,
                SF_CLUSTER_CONFIG_SIGN_LEN, my_hex);

        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "server #%d 's cluster config md5: %s != mine: %s",
                server_id, peer_hex, my_hex);
        return EFAULT;
    }

    return 0;
}

static int cluster_deal_get_server_status(struct fast_task_info *task)
{
    int result;
    int server_id;
    FDIRProtoGetServerStatusReq *req;
    FDIRProtoGetServerStatusResp *resp;

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoGetServerStatusReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoGetServerStatusReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if ((result=cluster_check_config_sign(task, server_id,
                    req->auth_enabled, req->config_sign)) != 0)
    {
        return result;
    }

    resp = (FDIRProtoGetServerStatusResp *)SF_PROTO_SEND_BODY(task);
    resp->is_master = (CLUSTER_MYSELF_PTR ==
            CLUSTER_MASTER_ATOM_PTR ? 1 : 0);
    resp->master_hint = MYSELF_IS_OLD_MASTER;
    resp->status = __sync_fetch_and_add(&CLUSTER_MYSELF_PTR->status, 0);
    resp->force_election = (FORCE_MASTER_ELECTION ? 1 : 0);
    int2buff(CLUSTER_MY_SERVER_ID, resp->server_id);
    long2buff(DATA_CURRENT_VERSION, resp->data_version);
    int2buff(g_sf_global_vars.up_time, resp->up_time);
    int2buff(CLUSTER_LAST_SHUTDOWN_TIME, resp->last_shutdown_time);
    int2buff(CLUSTER_LAST_HEARTBEAT_TIME, resp->last_heartbeat_time);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerStatusResp);
    RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int cluster_check_server_identity(struct fast_task_info *task,
        FDIRProtoClusterServerIdentity *si, int *server_id,
        FDIRClusterServerInfo **peer)
{
    int result;
    int cluster_id;

    cluster_id = buff2int(si->cluster_id);
    *server_id = buff2int(si->server_id);
    if (cluster_id != CLUSTER_ID) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer cluster id: %d != mine: %d",
                cluster_id, CLUSTER_ID);
        return EINVAL;
    }
    if (*server_id == CLUSTER_MY_SERVER_ID) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d == mine: %d",
                *server_id, CLUSTER_MY_SERVER_ID);
        return EINVAL;
    }

    *peer = fdir_get_server_by_id(*server_id);
    if (*peer == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d not exist", *server_id);
        return ENOENT;
    }

    if ((result=cluster_check_config_sign(task, *server_id,
                    si->auth_enabled, si->config_sign)) != 0)
    {
        return result;
    }

    return 0;
}

static int cluster_deal_join_master(struct fast_task_info *task)
{
    int result;
    int server_id;
    FDIRProtoJoinMasterReq *req;
    FDIRClusterServerInfo *peer;

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoJoinMasterReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoJoinMasterReq *)REQUEST.body;
    if ((result=cluster_check_server_identity(task, &req->si,
                    &server_id, &peer)) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    if (CLUSTER_PEER != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d already joined", server_id);
        return EEXIST;
    }

    peer->last_change_version = -1;  //push cluster info to the slave
    memcpy(peer->key, req->key, FDIR_REPLICA_KEY_SIZE);
    SERVER_TASK_TYPE = FDIR_SERVER_TASK_TYPE_RELATIONSHIP;
    CLUSTER_PEER = peer;

    /*
    {
    char formatted_ip[FORMATTED_IP_SIZE];
    format_ip_address(CLUSTER_GROUP_ADDRESS_FIRST_IP(
                peer->server), formatted_ip);
    logInfo("file: "__FILE__", line: %d, "
                "slave id: %d, %s:%u joined", __LINE__, peer->server->id,
                formatted_ip, CLUSTER_GROUP_ADDRESS_FIRST_PORT(peer->server));
    }
    */

    return 0;
}

static int cluster_deal_ping_master(struct fast_task_info *task)
{
    int result;
    int cluster_change_version;
    bool version_changed;
    int64_t confirmed_data_version;
    FDIRProtoPingMasterReq *req;
    FDIRProtoPingMasterRespHeader *resp_header;
    FDIRProtoPingMasterRespBodyPart *body_part;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *end;

    if ((result=server_expect_body_length(sizeof(*req))) != 0) {
        return result;
    }

    if (CLUSTER_PEER == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "please join first");
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    req = (FDIRProtoPingMasterReq *)REQUEST.body;
    confirmed_data_version = buff2long(req->confirmed_data_version);
    if (FC_ATOMIC_GET(CLUSTER_PEER->confirmed_data_version) !=
            confirmed_data_version)
    {
        FC_ATOMIC_SET(CLUSTER_PEER->confirmed_data_version,
                confirmed_data_version);
        if (REPLICA_QUORUM_NEED_MAJORITY) {
            replication_quorum_deal_version_change(confirmed_data_version);
        }
        version_changed = true;
    } else {
        version_changed = false;
    }

    resp_header = (FDIRProtoPingMasterRespHeader *)SF_PROTO_SEND_BODY(task);
    body_part = (FDIRProtoPingMasterRespBodyPart *)(resp_header + 1);
    long2buff(CURRENT_INODE_SN, resp_header->inode_sn);

    cluster_change_version = FC_ATOMIC_GET(
            CLUSTER_SERVER_ARRAY.change_version);
    if (CLUSTER_PEER->last_change_version != cluster_change_version) {
        CLUSTER_PEER->last_change_version = cluster_change_version;
        int2buff(CLUSTER_SERVER_ARRAY.count, resp_header->server_count);

        end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++, body_part++) {
            int2buff(cs->server->id, body_part->server_id);
            body_part->status = FC_ATOMIC_GET(cs->status);
            long2buff(cs->confirmed_data_version, body_part->data_version);
        }
    } else {
        int2buff(0, resp_header->server_count);
    }

    if (version_changed) {
        FC_ATOMIC_INC(CLUSTER_SERVER_ARRAY.change_version);
    }

    TASK_CTX.common.response_done = true;
    RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_PING_MASTER_RESP;
    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_SEND_BODY(task);
    return 0;
}

static int cluster_deal_next_master(struct fast_task_info *task)
{
    int result;
    int master_id;
    FDIRClusterServerInfo *master;

    if ((result=server_expect_body_length(4)) != 0) {
        return result;
    }

    if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am already master");
        cluster_relationship_trigger_reselect_master();
        return EEXIST;
    }

    master_id = buff2int(REQUEST.body);
    master = fdir_get_server_by_id(master_id);
    if (master == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "master server id: %d not exist", master_id);
        return ENOENT;
    }

    if (REQUEST.header.cmd == FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER) {
        return cluster_relationship_pre_set_master(master);
    } else {
        return cluster_relationship_commit_master(master);
    }
}

static inline int check_replication_slave_task(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE != FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid task type: %d != %d", SERVER_TASK_TYPE,
                FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE);
        return -EINVAL;
    }
    if (TASK_CONSUMER_CTX == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "please join first");
        return -EINVAL;
    }

    if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am master");
        return SF_CLUSTER_ERROR_MASTER_INCONSISTENT;
    }

    return 0;
}

static inline int cluster_deal_push_request(ReplicaConsumerThreadContext *ctx,
        char *binlog_buff, const int binlog_len,
        const SFVersionRange *data_version)
{
    int result;
    result = deal_replica_push_request(ctx, binlog_buff,
            binlog_len, data_version);
    return (result == 0 ? TASK_STATUS_CONTINUE : result);
}

static int cluster_deal_forword_requests_req(struct fast_task_info *task)
{
    int result;
    int binlog_length;
    int count;
    int expect_len;
    SFVersionRange data_version;
    SFRequestMetadata metadata;
    FDIRProtoForwardRequestMetadata *pmeta;
    FDIRProtoForwardRequestMetadata *pend;
    FDIRProtoForwardRequestsBodyHeader *body_header;
    char *binlog;

    if ((result=server_check_min_body_length(sizeof(
                        FDIRProtoForwardRequestsBodyHeader) +
                    FDIR_BINLOG_RECORD_MIN_SIZE)) != 0)
    {
        return result;
    }

    if ((result=check_replication_slave_task(task)) != 0) {
        return result;
    }

    body_header = (FDIRProtoForwardRequestsBodyHeader *)
        SF_PROTO_RECV_BODY(task);
    binlog_length = buff2int(body_header->binlog_length);
    count = buff2int(body_header->count);
    data_version.first = buff2long(body_header->data_version.first);
    data_version.last = buff2long(body_header->data_version.last);

    expect_len = sizeof(FDIRProtoForwardRequestsBodyHeader) +
        binlog_length + sizeof(*pmeta) * count;
    if (expect_len != REQUEST.header.body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expect length: %d",
                REQUEST.header.body_len, expect_len);
        return EINVAL;
    }

    binlog = (char *)(body_header + 1);
    pmeta = (FDIRProtoForwardRequestMetadata *)(binlog + binlog_length);
    pend = pmeta + count;
    for (; pmeta<pend; pmeta++) {
        if ((metadata.req_id=buff2long(pmeta->req_id)) != 0) {
            metadata.data_version = buff2long(pmeta->data_version);
            if ((result=idempotency_request_metadata_add(&REPLICA_REQ_META_CTX,
                            &metadata, 0)) != 0)
            {
                return result;
            }
        }
    }

    return cluster_deal_push_request(TASK_CONSUMER_CTX,
            binlog, binlog_length, &data_version);
}

static inline int check_replication_master_task(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE != FDIR_SERVER_TASK_TYPE_REPLICA_MASTER) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid task type: %d != %d", SERVER_TASK_TYPE,
                FDIR_SERVER_TASK_TYPE_REPLICA_MASTER);
        return EINVAL;
    }

    if (CLUSTER_REPLICA == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "cluster replication ptr is null");
        return EINVAL;
    }
    return 0;
}

static inline int replica_check_replication_client(struct fast_task_info *task)
{
    int result;

    if ((result=check_replication_master_task(task)) != 0) {
        return result;
    }

    if (CLUSTER_REPLICA->connection_info.last_net_comm_time != g_current_time) {
        CLUSTER_REPLICA->connection_info.last_net_comm_time = g_current_time;
    }

    --TASK_PENDING_SEND_COUNT;
    return 0;
}

static int replica_deal_rpc_resp(struct fast_task_info *task)
{
    int result;

    if (REQUEST.header.status != 0) {
        sf_proto_deal_ack(task, &REQUEST, &RESPONSE);
    } else if ((result=server_expect_body_length(sizeof(
                        FDIRProtoForwardRequestsResp))) != 0)
    {
        return result;
    }

    if ((result=replica_check_replication_client(task)) != 0) {
        return result;
    }

    desc_replication_waiting_rpc_count(CLUSTER_REPLICA,
            REQUEST.header.status);
    return REQUEST.header.status;
}

static int cluster_deal_push_binlog_req(struct fast_task_info *task)
{
    int result;
    int binlog_length;
    SFVersionRange data_version;
    FDIRProtoPushBinlogReqBodyHeader *body_header;

    if ((result=server_check_min_body_length(sizeof(
                        FDIRProtoPushBinlogReqBodyHeader) +
                    FDIR_BINLOG_RECORD_MIN_SIZE)) != 0)
    {
        return result;
    }

    if ((result=check_replication_slave_task(task)) != 0) {
        return result;
    }

    body_header = (FDIRProtoPushBinlogReqBodyHeader *)
        SF_PROTO_RECV_BODY(task);
    binlog_length = buff2int(body_header->binlog_length);
    data_version.first = buff2long(body_header->data_version.first);
    data_version.last = buff2long(body_header->data_version.last);
    if (sizeof(FDIRProtoPushBinlogReqBodyHeader) + binlog_length !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expect: %d", REQUEST.header.body_len,
                (int)(sizeof(FDIRProtoPushBinlogReqBodyHeader) +
                    binlog_length));
        return EINVAL;
    }

    return cluster_deal_push_request(TASK_CONSUMER_CTX, (char *)
            (body_header + 1), binlog_length, &data_version);
}

static int replica_deal_push_binlog_resp(struct fast_task_info *task)
{
    int result;
    FDIRProtoPushBinlogResp *resp;

    if (REQUEST.header.status != 0) {
        sf_proto_deal_ack(task, &REQUEST, &RESPONSE);
        return REQUEST.header.status;
    }

    if ((result=server_expect_body_length(sizeof(*resp))) != 0) {
        return result;
    }

    if ((result=replica_check_replication_client(task)) != 0) {
        return result;
    }

    resp = (FDIRProtoPushBinlogResp *)REQUEST.body;
    CLUSTER_REPLICA->context.sync_by_disk_stat.record_count +=
        buff2int(resp->record_count);
    return 0;
}

static int fill_binlog_last_lines(struct fast_task_info *task,
        int *binlog_count, int *binlog_length)
{
    int front_length;
    int buffer_size;

    if (SLAVE_BINLOG_CHECK_LAST_ROWS <= 0) {
        *binlog_length = 0;
        return 0;
    }

    front_length = sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoJoinSlaveResp);
    buffer_size = task->send.ptr->size - front_length;
    *binlog_count = SLAVE_BINLOG_CHECK_LAST_ROWS;
    return sf_binlog_writer_get_last_lines(DATA_PATH_STR,
            FDIR_BINLOG_SUBDIR_NAME, binlog_get_current_write_index(),
            task->send.ptr->data + front_length, buffer_size,
            binlog_count, binlog_length);
}

static int cluster_deal_join_slave_req(struct fast_task_info *task)
{
    int result;
    int cluster_id;
    int server_id;
    int buffer_size;
    int binlog_count;
    int binlog_length;
    ReplicaConsumerThreadContext *consumer_ctx;
    SFBinlogFilePosition bf_position;
    FDIRProtoJoinSlaveReq *req;
    FDIRClusterServerInfo *peer;
    FDIRClusterServerInfo *master;
    FDIRClusterServerInfo *next_master;
    FDIRProtoJoinSlaveResp *resp;

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoJoinSlaveReq))) != 0)
    {
        return result;
    }

    if (!DATA_LOAD_DONE) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not ready");
        TASK_CTX.common.log_level = LOG_NOTHING;
        RESPONSE.header.cmd = FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP;
        return EPROTONOSUPPORT;
    }

    if (REPLICA_QUORUM_NEED_MAJORITY && MYSELF_IS_OLD_MASTER) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "waiting rollback binlog because i am the old master");
        TASK_CTX.common.log_level = LOG_NOTHING;
        RESPONSE.header.cmd = FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP;
        return EBUSY;
    }

    req = (FDIRProtoJoinSlaveReq *)REQUEST.body;
    cluster_id = buff2int(req->cluster_id);
    server_id = buff2int(req->server_id);
    buffer_size = buff2int(req->buffer_size);
    if (cluster_id != CLUSTER_ID) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer cluster id: %d != mine: %d",
                cluster_id, CLUSTER_ID);
        return EINVAL;
    }
    if (buffer_size != task->send.ptr->size) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer task buffer size: %d != mine: %d",
                buffer_size, task->send.ptr->size);
        return EINVAL;
    }

    peer = fdir_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }

    next_master = CLUSTER_NEXT_MASTER;
    if (next_master != NULL) {
        if (next_master != peer) {
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "master selection in progress, the candidate "
                    "master id: %d", next_master->server->id);
            return SF_CLUSTER_ERROR_MASTER_INCONSISTENT;
        }
        return EBUSY;
    }

    master = CLUSTER_MASTER_ATOM_PTR;
    if (peer != master) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "master NOT consistent, peer server id: %d, "
                "local master id: %d", server_id, master != NULL ?
                master->server->id : 0);
        return master != NULL ? SF_CLUSTER_ERROR_MASTER_INCONSISTENT : EFAULT;
    }

    if (memcmp(req->key, REPLICA_KEY_BUFF, FDIR_REPLICA_KEY_SIZE) != 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "check key fail");
        return EPERM;
    }

    if (TASK_CONSUMER_CTX != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "master server id: %d already joined", server_id);
        return EEXIST;
    }

    consumer_ctx = (ReplicaConsumerThreadContext *)
        FC_ATOMIC_GET(REPLICA_CONSUMER_CTX);
    if (consumer_ctx != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "replica consumer thread already exist");
        if (!FC_ATOMIC_GET(consumer_ctx->task->canceled)) {
            sf_nio_add_to_deleted_list(task->thread_data,
                    consumer_ctx->task);
        }
        RESPONSE.header.cmd = FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP;
        return EBUSY;
    }

    if ((result=fill_binlog_last_lines(task, &binlog_count,
                    &binlog_length)) != 0)
    {
        return result;
    }

    consumer_ctx = replica_consumer_thread_init(task, &result);
    if (consumer_ctx == NULL) {
        return result;
    }
    if (!__sync_bool_compare_and_swap(&REPLICA_CONSUMER_CTX,
                NULL, consumer_ctx))
    {
        replica_consumer_thread_terminate(consumer_ctx);
        return EEXIST;
    }

    TASK_CONSUMER_CTX = consumer_ctx;
    SERVER_TASK_TYPE = FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE;

    binlog_get_current_write_position(&bf_position);

    resp = (FDIRProtoJoinSlaveResp *)SF_PROTO_SEND_BODY(task);
    long2buff(DATA_CURRENT_VERSION, resp->last_data_version);
    int2buff(bf_position.index, resp->binlog_pos_hint.index);
    long2buff(bf_position.offset, resp->binlog_pos_hint.offset);
    int2buff(binlog_count, resp->binlog_count);
    int2buff(binlog_length, resp->binlog_length);

    TASK_CTX.common.response_done = true;
    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP;
    RESPONSE.header.body_len = sizeof(FDIRProtoJoinSlaveResp) + binlog_length;
    return 0;
}

static void cluster_notify_slave_quit(struct fast_task_info *task,
        const int result, const int binlog_count,
        const uint64_t first_unmatched_dv)
{
    FDIRProtoNotifySlaveQuit *req;

    req = (FDIRProtoNotifySlaveQuit *)REQUEST.body;
    RESPONSE.header.body_len = sizeof(FDIRProtoNotifySlaveQuit);

    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    int2buff(result, req->error_code);
    int2buff(binlog_count, req->binlog_count);
    long2buff(first_unmatched_dv, req->first_unmatched_dv);
    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_NOTIFY_SLAVE_QUIT;
    TASK_CTX.common.response_done = true;
}

static int cluster_check_binlog_consistency(
        struct fast_task_info *task, const int binlog_count,
        string_t *binlog, SFBinlogFilePosition *hint_pos)
{
    int result;
    int new_binlog_count;
    uint64_t first_unmatched_dv;

    new_binlog_count = binlog_count;
    if ((result=binlog_check_consistency(binlog, hint_pos,
                    &new_binlog_count, &first_unmatched_dv)) != 0)
    {
        char prompt[128];
        if (result == SF_CLUSTER_ERROR_BINLOG_INCONSISTENT) {
            sprintf(prompt, "first unmatched data "
                    "version: %"PRId64, first_unmatched_dv);
            cluster_notify_slave_quit(task, result,
                    new_binlog_count, first_unmatched_dv);
            TASK_CTX.common.need_response = true;
        } else if (result == SF_CLUSTER_ERROR_BINLOG_MISSED) {
            sprintf(prompt, "binlog missed");
            cluster_notify_slave_quit(task, result,
                    new_binlog_count, first_unmatched_dv);
            TASK_CTX.common.need_response = true;
        } else {
            sprintf(prompt, "some mistake happen, "
                    "error code is %d", result);
        }
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "slave server id: %d, binlog consistency check fail, %s",
                CLUSTER_REPLICA->slave->server->id, prompt);
        return result;
    }

    return 0;
}

static int cluster_deal_join_slave_resp(struct fast_task_info *task)
{
    int result;
    int binlog_count;
    string_t binlog;
    SFBinlogFilePosition hint_pos;
    FDIRProtoJoinSlaveResp *req;

    if ((result=check_replication_master_task(task)) != 0) {
        return result;
    }

    if (REQUEST.header.status != 0) {
        int connect_interval;

        CLUSTER_REPLICA->join_fail_count++;
        if (CLUSTER_REPLICA->join_fail_count < 6) {
            connect_interval = (1 << CLUSTER_REPLICA->join_fail_count);
        } else {
            connect_interval = 60;
        }

        CLUSTER_REPLICA->connection_info.next_connect_time =
            g_current_time + connect_interval;

        result = sf_localize_errno(REQUEST.header.status);
        if (REQUEST.header.body_len > 0 && REQUEST.header.body_len <
                sizeof(RESPONSE.error.message))
        {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "%.*s", REQUEST.header.body_len, REQUEST.body);
        } else {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "errno: %d, error info: %s", result, STRERROR(result));
        }
        return result;
    } else {
        CLUSTER_REPLICA->join_fail_count = 0;
    }

    if ((result=server_check_min_body_length(sizeof(
                        FDIRProtoJoinSlaveResp))) != 0)
    {
        return result;
    }

    req = (FDIRProtoJoinSlaveResp *)REQUEST.body;
    binlog_count = buff2int(req->binlog_count);
    binlog.len = buff2int(req->binlog_length);
    if (REQUEST.header.body_len != sizeof(FDIRProtoJoinSlaveResp) +
            binlog.len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "response body length: %d != sizeof(FDIRProtoJoinSlaveResp)"
                ": %d + binlog_length: %d", REQUEST.header.body_len,
                (int)sizeof(FDIRProtoJoinSlaveResp), binlog.len);
        return EINVAL;
    }

    hint_pos.index = buff2int(req->binlog_pos_hint.index);
    hint_pos.offset = buff2long(req->binlog_pos_hint.offset);
    binlog.str = req->binlog;
    if ((result=cluster_check_binlog_consistency(task,
                    binlog_count, &binlog, &hint_pos)) != 0)
    {
        return result;
    }

    CLUSTER_REPLICA->slave->last_data_version = buff2long(
            req->last_data_version);
    CLUSTER_REPLICA->slave->binlog_pos_hint = hint_pos;
    return 0;
}

static int cluster_deal_notify_slave_quit(struct fast_task_info *task)
{
    FDIRProtoNotifySlaveQuit *req;
    int result;
    int server_id;
    int error_code;
    int binlog_count;
    int64_t first_unmatched_dv;
    char prompt[256];

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoNotifySlaveQuit))) != 0)
    {
        return result;
    }

    if ((result=check_replication_slave_task(task)) != 0) {
        return result;
    }

    req = (FDIRProtoNotifySlaveQuit *)REQUEST.body;
    server_id = buff2int(req->server_id);
    error_code = buff2int(req->error_code);
    binlog_count = buff2int(req->binlog_count);
    first_unmatched_dv = buff2long(req->first_unmatched_dv);

    if (error_code == SF_CLUSTER_ERROR_BINLOG_MISSED) {
        sprintf(prompt, "binlog missed on the master server: %d, "
                "you should remove the data then try again", server_id);
    } else {
        sprintf(prompt, "my last %d binlogs NOT consistent with "
                "the master server: %d, the first unmatched data "
                "version: %"PRId64, binlog_count, server_id,
                first_unmatched_dv);
    }
    logCrit("file: "__FILE__", line: %d, "
            "%s, program exit!", __LINE__, prompt);
    sf_terminate_myself();
    return -EBUSY;
}

static int cluster_deal_slave_ack(struct fast_task_info *task)
{
    int result;

    if ((result=check_replication_master_task(task)) != 0) {
        return result;
    }

    if ((result=sf_proto_deal_ack(task, &REQUEST, &RESPONSE)) != 0) {
        if (result == SF_CLUSTER_ERROR_MASTER_INCONSISTENT) {
            logWarning("file: "__FILE__", line: %d, "
                    "more than one masters occur, master brain-split maybe "
                    "happened, will trigger reselecting master", __LINE__);
            cluster_relationship_trigger_reselect_master();
            result = EEXIST;
        }
    }

    return result;
}

static int replica_check_file(struct fast_task_info *task,
        const char *filename)
{
    int result;
    struct stat buf;

    if (stat(filename, &buf) == 0) {
        if (STORAGE_ENABLED) {
            if (g_current_time - buf.st_mtime < 86400) {
                return 0;
            }
            if ((result=fc_delete_file(filename)) == 0) {
                return ENOENT;
            }
        } else {
            return 0;
        }
    } else {
        result = errno != 0 ? errno : EPERM;
        if (result != ENOENT) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "access file %s fail, error info: %s",
                    filename, STRERROR(result));
        }
    }

    return result;
}

static int replica_deal_query_binlog_info(struct fast_task_info *task)
{
    const bool create_thread = true;
    FDIRProtoReplicaQueryBinlogInfoReq *req;
    FDIRProtoReplicaQueryBinlogInfoResp *resp;
    char subdir_name[64];
    char dump_filename[PATH_MAX];
    char mark_filename[PATH_MAX];
    int server_id;
    int dump_start_index;
    int dump_last_index;
    int binlog_start_index;
    int binlog_last_index;
    int result;

    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP;
    if ((result=server_expect_body_length(sizeof(*req))) != 0) {
        return result;
    }

    req = (FDIRProtoReplicaQueryBinlogInfoReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d, i am not master", server_id);
        return EINVAL;
    }
    if (server_id == CLUSTER_MY_SERVER_ID) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d is invalid", server_id);
        return EINVAL;
    }

    if ((result=binlog_get_indexes(&binlog_start_index,
                    &binlog_last_index)) != 0)
    {
        return result;
    }

    resp = (FDIRProtoReplicaQueryBinlogInfoResp *)SF_PROTO_SEND_BODY(task);
    if (binlog_start_index > 0) {
        int result1;
        int result2;

        if (STORAGE_ENABLED) {
            binlog_dump_get_subdir_name(subdir_name, server_id);
        } else {
            strcpy(subdir_name, FDIR_DATA_DUMP_SUBDIR_NAME);
        }
        fdir_get_dump_data_filename_ex(subdir_name,
                dump_filename, sizeof(dump_filename));
        if ((result1=replica_check_file(task, dump_filename)) != 0) {
            if (result1 != ENOENT) {
                return result1;
            }
        }

        fdir_get_dump_mark_filename_ex(subdir_name,
                mark_filename, sizeof(mark_filename));
        if ((result2=replica_check_file(task, mark_filename)) != 0) {
            if (result2 != ENOENT) {
                return result2;
            }
        }
        if (result1 == ENOENT || result2 == ENOENT) {
            result = binlog_dump_all_ex(server_id, create_thread);
            if (!(result == 0 || result == EINPROGRESS)) {
                return result;
            }
        }
        dump_start_index = 0;
        dump_last_index = 0;
    } else {
        dump_start_index = 0;
        dump_last_index = -1;
    }

    int2buff(dump_start_index, resp->dump_data.start_index);
    int2buff(dump_last_index, resp->dump_data.last_index);
    int2buff(binlog_start_index, resp->binlog.start_index);
    int2buff(binlog_last_index, resp->binlog.last_index);
    RESPONSE.header.body_len = sizeof(*resp);
    TASK_CTX.common.response_done = true;
    return 0;
}

static inline int sync_binlog_output(struct fast_task_info *task)
{
    int result;
    int size;
    int read_bytes;

    size = task->send.ptr->size - sizeof(FDIRProtoHeader);
    result = binlog_reader_read_to_buffer(REPLICA_READER,
            SF_PROTO_SEND_BODY(task), size, &read_bytes);
    if (!(result == 0 || result == ENOENT)) {
        return result;
    }

    RESPONSE.header.body_len = read_bytes;
    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_SYNC_BINLOG_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int replica_deal_sync_binlog_first(struct fast_task_info *task)
{
    FDIRProtoReplicaSyncBinlogFirstReq *req;
    char subdir_name[64];
    int server_id;
    int binlog_index;
    int result;

    if ((result=server_expect_body_length(sizeof(*req))) != 0) {
        return result;
    }

    req = (FDIRProtoReplicaSyncBinlogFirstReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    binlog_index = buff2int(req->binlog_index);
    if (server_id == CLUSTER_MY_SERVER_ID) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d is invalid", server_id);
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    if (SERVER_TASK_TYPE != SF_SERVER_TASK_TYPE_NONE) {
        if (SERVER_TASK_TYPE == FDIR_SERVER_TASK_TYPE_SYNC_BINLOG &&
                REPLICA_READER != NULL)
        {
            replica_release_reader(task, true);
        } else {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "already in progress. task type: %d, have reader: %d",
                    SERVER_TASK_TYPE, REPLICA_READER != NULL ? 1 : 0);
            return EALREADY;
        }
    }

    if (req->file_type == FDIR_PROTO_FILE_TYPE_DUMP) {
        char dump_filename[PATH_MAX];

        if (STORAGE_ENABLED) {
            binlog_dump_get_subdir_name(subdir_name, server_id);
        } else {
            strcpy(subdir_name, FDIR_DATA_DUMP_SUBDIR_NAME);
        }
        fdir_get_dump_data_filename_ex(subdir_name,
                dump_filename, sizeof(dump_filename));
        if (access(dump_filename, F_OK) != 0) {
            if (errno == ENOENT) {
                return EAGAIN;
            } else {
                result = (errno != 0 ? errno : EPERM);
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "access file %s fail, error info: %s",
                        dump_filename, STRERROR(result));
                return result;
            }
        }
    } else if (req->file_type == FDIR_PROTO_FILE_TYPE_BINLOG) {
        strcpy(subdir_name, FDIR_BINLOG_SUBDIR_NAME);
    } else {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unkown file type: %d", req->file_type);
        return EINVAL;
    }

    result = replica_alloc_reader(task,
            FDIR_SERVER_TASK_TYPE_SYNC_BINLOG);
    if (result != 0) {
        return result;
    }
    if ((result=binlog_reader_single_init(REPLICA_READER,
                    subdir_name, binlog_index)) != 0)
    {
        replica_release_reader(task, false);
        return result;
    }

    return sync_binlog_output(task);
}

static int replica_deal_sync_binlog_next(struct fast_task_info *task)
{
    int result;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    if (!(SERVER_TASK_TYPE == FDIR_SERVER_TASK_TYPE_SYNC_BINLOG &&
                REPLICA_READER != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "please send cmd %d (%s) first",
                FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ,
                fdir_get_cmd_caption(FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ));
        return EINVAL;
    }

    return sync_binlog_output(task);
}

static int replica_deal_sync_dump_mark(struct fast_task_info *task)
{
    int result;
    int server_id;
    FDIRProtoReplicaSyncDumpMarkReq *req;
    int64_t read_bytes;
    char subdir_name[64];
    char mark_filename[PATH_MAX];

    if ((result=server_expect_body_length(sizeof(*req))) != 0) {
        return result;
    }

    req = (FDIRProtoReplicaSyncDumpMarkReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if (server_id == CLUSTER_MY_SERVER_ID) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d == mine: %d",
                server_id, CLUSTER_MY_SERVER_ID);
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    if (STORAGE_ENABLED) {
        binlog_dump_get_subdir_name(subdir_name, server_id);
    } else {
        strcpy(subdir_name, FDIR_DATA_DUMP_SUBDIR_NAME);
    }
    fdir_get_dump_mark_filename_ex(subdir_name,
            mark_filename, sizeof(mark_filename));
    read_bytes = (task->send.ptr->size - sizeof(FDIRProtoHeader)) - 1;
    result = getFileContentEx(mark_filename, SF_PROTO_SEND_BODY(task),
            0, &read_bytes);
    if (result != 0) {
        return result;
    }

    RESPONSE.header.body_len = read_bytes;
    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int replica_deal_sync_binlog_report(struct fast_task_info *task)
{
    int result;
    int server_id;
    FDIRProtoReplicaSyncBinlogReportReq *req;
    FDIRClusterServerInfo *peer;

    if ((result=server_expect_body_length(sizeof(*req))) != 0) {
        return result;
    }

    req = (FDIRProtoReplicaSyncBinlogReportReq *)REQUEST.body;
    if ((result=cluster_check_server_identity(task, &req->si,
                    &server_id, &peer)) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    if (req->stage == FDIR_PROTO_SYNC_BINLOG_STAGE_START) {
        peer->recovering = true;
    } else if (req->stage == FDIR_PROTO_SYNC_BINLOG_STAGE_END) {
        peer->recovering = false;
        if (STORAGE_ENABLED) {
            result = binlog_dump_clear_files_ex(server_id);
        }
    } else {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unkown stage: 0x%02X", req->stage);
        return EINVAL;
    }

    return result;
}

int cluster_deal_task(struct fast_task_info *task, const int stage)
{
    int result;

    if (stage == SF_NIO_STAGE_HANDSHAKE) {
        result = binlog_replication_join_slave(task);
    } else if (stage == SF_NIO_STAGE_CONTINUE) {
        if (task->continue_callback != NULL) {
            result = task->continue_callback(task);
        } else {
            result = RESPONSE_STATUS;
            if (result == TASK_STATUS_CONTINUE) {
                logError("file: "__FILE__", line: %d, "
                        "unexpect status: %d", __LINE__, result);
                result = EBUSY;
            }
        }
    } else {
        sf_proto_init_task_context(task, &TASK_CTX.common);

        switch (REQUEST.header.cmd) {
            case SF_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
                result = sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
                break;
            case SF_PROTO_ACTIVE_TEST_RESP:
                if ((result=replica_check_replication_client(task)) != 0) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                TASK_CTX.common.need_response = false;
                break;
            case FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
                result = cluster_deal_get_server_status(task);
                break;
            case FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER:
            case FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER:
                result = cluster_deal_next_master(task);
                break;
            case FDIR_CLUSTER_PROTO_JOIN_MASTER_REQ:
                RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_JOIN_MASTER_RESP;
                result = cluster_deal_join_master(task);
                break;
            case FDIR_CLUSTER_PROTO_PING_MASTER_REQ:
                result = cluster_deal_ping_master(task);
                break;
            case FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ:
                if ((result=cluster_deal_join_slave_req(task)) > 0) {
                    if (!(result == EPROTONOSUPPORT || result == EBUSY)) {
                        result *= -1;  //force close connection
                    }
                }
                break;
            case FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP:
                TASK_CTX.common.need_response = false;
                if ((result=cluster_deal_join_slave_resp(task)) > 0) {
                    if (!TASK_CTX.common.need_response) {
                        result *= -1;  //force close connection
                    }
                }
                break;
            case FDIR_REPLICA_PROTO_RPC_CALL_REQ:
                result = cluster_deal_forword_requests_req(task);
                RESPONSE.header.cmd = FDIR_REPLICA_PROTO_RPC_CALL_RESP;
                if (!(result == 0 || result == TASK_STATUS_CONTINUE)) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                break;
            case FDIR_REPLICA_PROTO_RPC_CALL_RESP:
                if ((result=replica_deal_rpc_resp(task)) != 0) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                TASK_CTX.common.need_response = false;
                break;
            case FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ:
                result = cluster_deal_push_binlog_req(task);
                RESPONSE.header.cmd = FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP;
                if (!(result == 0 || result == TASK_STATUS_CONTINUE)) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                break;
            case FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP:
                if ((result=replica_deal_push_binlog_resp(task)) != 0) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                TASK_CTX.common.need_response = false;
                break;
            case FDIR_REPLICA_PROTO_NOTIFY_SLAVE_QUIT:
                result = cluster_deal_notify_slave_quit(task);
                TASK_CTX.common.need_response = false;
                break;

            case FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ:
                result = replica_deal_query_binlog_info(task);
                break;
            case FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ:
                result = replica_deal_sync_binlog_first(task);
                break;
            case FDIR_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ:
                result = replica_deal_sync_binlog_next(task);
                break;
            case FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_REQ:
                result = replica_deal_sync_dump_mark(task);
                break;
            case FDIR_REPLICA_PROTO_SYNC_BINLOG_REPORT:
                result = replica_deal_sync_binlog_report(task);
                break;
            case SF_PROTO_ACK:
                result = cluster_deal_slave_ack(task);
                TASK_CTX.common.need_response = false;
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cluster cmd: %d", REQUEST.header.cmd);
                result = -EINVAL;
                break;
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return sf_proto_deal_task_done(task, "cluster", &TASK_CTX.common);
    }
}

static int alloc_replication_ptr_array(FDIRSlaveReplicationPtrArray *array)
{
    int bytes;

    bytes = sizeof(FDIRSlaveReplication *) * CLUSTER_SERVER_ARRAY.count;
    array->replications = (FDIRSlaveReplication **)fc_malloc(bytes);
    if (array->replications == NULL) {
        return ENOMEM;
    }
    memset(array->replications, 0, bytes);
    return 0;
}

void *cluster_alloc_thread_extra_data(const int thread_index)
{
    FDIRServerContext *server_ctx;

    server_ctx = fc_malloc(sizeof(FDIRServerContext));
    if (server_ctx == NULL) {
        return NULL;
    }
    memset(server_ctx, 0, sizeof(FDIRServerContext));

    if (alloc_replication_ptr_array(&server_ctx->cluster.connectings) != 0) {
        return NULL;
    }
    if (alloc_replication_ptr_array(&server_ctx->cluster.connected) != 0) {
        return NULL;
    }

    if (init_pthread_lock(&server_ctx->cluster.queue.lock) != 0) {
        return NULL;
    }
    server_ctx->thread_index = thread_index;
    return server_ctx;
}

int cluster_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FDIRServerContext *server_ctx;
    //static int count = 0;

    server_ctx = (FDIRServerContext *)thread_data->arg;

    /*
    if (count++ % 100 == 0) {
        logInfo("%d. is_master: %d, server_ctx: %p, consumer_ctx: %p, connected.count: %d",
                count, MYSELF_IS_OLD_MASTER, server_ctx, REPLICA_CONSUMER_CTX,
                server_ctx->cluster.connected.count);
    }
    */

    if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
        return binlog_replication_process(server_ctx);
    } else {
        if (FC_ATOMIC_GET(server_ctx->cluster.clean_replications)) {
            logWarning("file: "__FILE__", line: %d, "
                    "cluster thread #%d, will clean %d connecting "
                    "replications and %d connected replications "
                    "because i am no longer master", __LINE__,
                    server_ctx->thread_index,
                    server_ctx->cluster.connectings.count,
                    server_ctx->cluster.connected.count);

            __sync_bool_compare_and_swap(&server_ctx->cluster.
                    clean_replications, 1, 0);
            clean_master_replications(server_ctx);
        }

        return 0;
    }
}

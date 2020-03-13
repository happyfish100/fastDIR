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
#include "fastcommon/json_parser.h"
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
#include "cluster_topology.h"
#include "cluster_handler.h"

#define TASK_STATUS_CONTINUE   12345

#define TASK_ARG ((FDIRServerTaskArg *)task->arg)
#define REQUEST  TASK_ARG->context.request
#define RESPONSE TASK_ARG->context.response
#define RESPONSE_STATUS RESPONSE.header.status

int cluster_handler_init()
{
    return 0;
}

int cluster_handler_destroy()
{   
    return 0;
}

void cluster_task_finish_cleanup(struct fast_task_info *task)
{
    FDIRServerTaskArg *task_arg;

    task_arg = (FDIRServerTaskArg *)task->arg;

    if (CLUSTER_TASK_TYPE == FDIR_CLUSTER_TASK_TYPE_RELATIONSHIP) {
        if (CLUSTER_PEER != NULL) {
            ct_slave_server_offline(CLUSTER_PEER);
            CLUSTER_PEER = NULL;
        }
        CLUSTER_TASK_TYPE = FDIR_CLUSTER_TASK_TYPE_NONE;
    } else if (CLUSTER_TASK_TYPE == FDIR_CLUSTER_TASK_TYPE_REPLICATION) {
        if (CLUSTER_REPLICA != NULL) {
            binlog_replication_rebind_thread(CLUSTER_REPLICA);
            CLUSTER_REPLICA = NULL;
        }
        CLUSTER_TASK_TYPE = FDIR_CLUSTER_TASK_TYPE_NONE;
    }

    __sync_add_and_fetch(&((FDIRServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int cluster_deal_actvie_test(struct fast_task_info *task)
{
    return server_expect_body_length(task, 0);
}

static int cluster_check_config_sign(struct fast_task_info *task,
        const int server_id, const char *config_sign)
{
    if (memcmp(config_sign, CLUSTER_CONFIG_SIGN_BUF,
                CLUSTER_CONFIG_SIGN_LEN) != 0)
    {
        char peer_hex[2 * CLUSTER_CONFIG_SIGN_LEN + 1];
        char my_hex[2 * CLUSTER_CONFIG_SIGN_LEN + 1];

        bin2hex(config_sign, CLUSTER_CONFIG_SIGN_LEN, peer_hex);
        bin2hex((const char *)CLUSTER_CONFIG_SIGN_BUF,
                CLUSTER_CONFIG_SIGN_LEN, my_hex);

        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "server #%d 's cluster config md5: %s != my: %s",
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

    if ((result=server_expect_body_length(task,
                    sizeof(FDIRProtoGetServerStatusReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoGetServerStatusReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if ((result=cluster_check_config_sign(task, server_id,
                    req->config_sign)) != 0)
    {
        return result;
    }

    resp = (FDIRProtoGetServerStatusResp *)REQUEST.body;

    resp->is_master = MYSELF_IS_MASTER;
    int2buff(CLUSTER_MY_SERVER_ID, resp->server_id);
    long2buff(DATA_CURRENT_VERSION, resp->data_version);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerStatusResp);
    RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int cluster_deal_join_master(struct fast_task_info *task)
{
    int result;
    int cluster_id;
    int server_id;
    FDIRProtoJoinMasterReq *req;
    FDIRClusterServerInfo *peer;

    if ((result=server_expect_body_length(task,
                    sizeof(FDIRProtoJoinMasterReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoJoinMasterReq *)REQUEST.body;
    cluster_id = buff2int(req->cluster_id);
    server_id = buff2int(req->server_id);
    if (cluster_id != CLUSTER_ID) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer cluster id: %d != mine: %d",
                cluster_id, CLUSTER_ID);
        return EINVAL;
    }

    peer = fdir_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }

    if ((result=cluster_check_config_sign(task, server_id,
                    req->config_sign)) != 0)
    {
        return result;
    }

    if (!MYSELF_IS_MASTER) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    if (CLUSTER_PEER != NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d already joined", server_id);
        return EEXIST;
    }

    memcpy(peer->key, req->key, FDIR_REPLICA_KEY_SIZE);
    CLUSTER_TASK_TYPE = FDIR_CLUSTER_TASK_TYPE_RELATIONSHIP;
    CLUSTER_PEER = peer;
    ct_slave_server_online(peer);
    return 0;
}

static int cluster_deal_ping_master(struct fast_task_info *task)
{
    int result;
    FDIRProtoPingMasterResp *resp;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    if (CLUSTER_PEER == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "please join first");
        return EINVAL;
    }

    if (!MYSELF_IS_MASTER) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    resp = (FDIRProtoPingMasterResp *)REQUEST.body;
    long2buff(CURRENT_INODE_SN, resp->inode_sn);
    resp->your_status = CLUSTER_PEER->status;
    TASK_ARG->context.response_done = true;
    RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_PING_MASTER_RESP;
    RESPONSE.header.body_len = sizeof(FDIRProtoPingMasterResp);
    return 0;
}

static int cluster_deal_next_master(struct fast_task_info *task)
{
    int result;
    int master_id;
    FDIRClusterServerInfo *master;

    if ((result=server_expect_body_length(task, 4)) != 0) {
        return result;
    }

    if (MYSELF_IS_MASTER) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am already master");
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
        return cluster_relationship_commit_master(master, false);
    }
}

/*
static void record_deal_done_notify(const int result, void *args)
{
    struct fast_task_info *task;
    task = (struct fast_task_info *)args;
    RESPONSE_STATUS = result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
}
*/

static int cluster_deal_push_binlog(struct fast_task_info *task)
{
    int result;

    if ((result=server_check_min_body_length(task, 1)) != 0) {
        return result;
    }

    logInfo("push_binlog body length: %d", REQUEST.header.body_len);
    return 0;
}

static inline int cluster_check_replication_task(struct fast_task_info *task)
{
    if (CLUSTER_TASK_TYPE != FDIR_CLUSTER_TASK_TYPE_REPLICATION) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid task type: %d != %d", CLUSTER_TASK_TYPE,
                FDIR_CLUSTER_TASK_TYPE_REPLICATION);
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

static int cluster_deal_join_slave_req(struct fast_task_info *task)
{
    int result;
    int cluster_id;
    int server_id;
    FDIRBinlogFilePosition bf_position;
    FDIRProtoJoinSlaveReq *req;
    FDIRClusterServerInfo *peer;
    FDIRClusterServerInfo *master;
    FDIRClusterServerInfo *next_master;
    FDIRProtoJoinSlaveResp *resp;

    if ((result=server_expect_body_length(task,
                    sizeof(FDIRProtoJoinSlaveReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoJoinSlaveReq *)REQUEST.body;
    cluster_id = buff2int(req->cluster_id);
    server_id = buff2int(req->server_id);
    if (cluster_id != CLUSTER_ID) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer cluster id: %d != mine: %d",
                cluster_id, CLUSTER_ID);
        return EINVAL;
    }

    peer = fdir_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }

    next_master = g_next_master;
    if (next_master != NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "master selection in progress, the candidate "
                "master id: %d", next_master->server->id);
        return EBUSY;
    }

    master = CLUSTER_MASTER_PTR;
    if (peer != master) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "master NOT consistent, peer server id: %d, "
                "local master id: %d", server_id, master != NULL ?
                master->server->id : 0);
        return master != NULL ? FDIR_STATUS_MASTER_INCONSISTENT : EFAULT;
    }

    if (memcmp(req->key, REPLICA_KEY_BUFF, FDIR_REPLICA_KEY_SIZE) != 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "check key fail");
        return EPERM;
    }

    binlog_get_current_write_position(&bf_position);

    resp = (FDIRProtoJoinSlaveResp *)REQUEST.body;
    long2buff(DATA_CURRENT_VERSION, resp->last_data_version);
    int2buff(bf_position.index, resp->binlog_pos_hint.index);
    long2buff(bf_position.offset, resp->binlog_pos_hint.offset);

    TASK_ARG->context.response_done = true;
    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP;
    RESPONSE.header.body_len = sizeof(FDIRProtoJoinSlaveResp);
    return 0;
}

static int cluster_deal_join_slave_resp(struct fast_task_info *task)
{
    int result;
    FDIRProtoJoinSlaveResp *req;

    if ((result=cluster_check_replication_task(task)) != 0) {
        return result;
    }

    if ((result=server_expect_body_length(task,
                    sizeof(FDIRProtoJoinSlaveResp))) != 0)
    {
        return result;
    }

    req = (FDIRProtoJoinSlaveResp *)REQUEST.body;
    CLUSTER_REPLICA->slave->last_data_version = buff2long(
            req->last_data_version);
    CLUSTER_REPLICA->slave->binlog_pos_hint.index = buff2int(
            req->binlog_pos_hint.index);
    CLUSTER_REPLICA->slave->binlog_pos_hint.offset = buff2long(
            req->binlog_pos_hint.offset);
    return 0;
}

static int cluster_deal_slave_ack(struct fast_task_info *task)
{
    int result;

    if ((result=cluster_check_replication_task(task)) != 0) {
        return result;
    }

    if (REQUEST_STATUS != 0) {
        if (REQUEST.header.body_len > 0) {
            if (REQUEST.header.body_len >= sizeof(RESPONSE.error.message)) {
                RESPONSE.error.length = sizeof(RESPONSE.error.message) - 1;
            } else {
                RESPONSE.error.length = REQUEST.header.body_len;
            }
            memcpy(RESPONSE.error.message, REQUEST.body, RESPONSE.error.length);
            *(RESPONSE.error.message + RESPONSE.error.length) = '\0';
        }

        if (REQUEST_STATUS == FDIR_STATUS_MASTER_INCONSISTENT) {
            //TODO  reselect master
        }

        return REQUEST_STATUS;
    }

    if (REQUEST.header.body_len > 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "ACK body length: %d != 0",
                REQUEST.header.body_len);
        return -EINVAL;
    }

    return 0;
}

static inline void init_task_context(struct fast_task_info *task)
{
    TASK_ARG->req_start_time = get_current_time_us();
    RESPONSE.header.cmd = FDIR_PROTO_ACK;
    RESPONSE.header.body_len = 0;
    RESPONSE.header.status = 0;
    RESPONSE.error.length = 0;
    RESPONSE.error.message[0] = '\0';
    TASK_ARG->context.log_error = true;
    TASK_ARG->context.response_done = false;
    TASK_ARG->context.need_response = true;

    REQUEST.header.cmd = ((FDIRProtoHeader *)task->data)->cmd;
    REQUEST.header.body_len = task->length - sizeof(FDIRProtoHeader);
    REQUEST.header.status = buff2short(((FDIRProtoHeader *)task->data)->status);
    REQUEST.body = task->data + sizeof(FDIRProtoHeader);
}

static int deal_task_done(struct fast_task_info *task)
{
    FDIRProtoHeader *proto_header;
    int r;
    int time_used;

    if (TASK_ARG->context.log_error && RESPONSE.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, cmd: %d, req body length: %d, %s",
                __LINE__, task->client_ip, REQUEST.header.cmd,
                REQUEST.header.body_len,
                RESPONSE.error.message);
    }

    if (!TASK_ARG->context.need_response) {
        if (RESPONSE_STATUS == 0) {
            task->offset = task->length = 0;
            return sf_set_read_event(task);
        }
        return RESPONSE_STATUS > 0 ? -1 * RESPONSE_STATUS : RESPONSE_STATUS;
    }

    proto_header = (FDIRProtoHeader *)task->data;
    if (!TASK_ARG->context.response_done) {
        RESPONSE.header.body_len = RESPONSE.error.length;
        if (RESPONSE.error.length > 0) {
            memcpy(task->data + sizeof(FDIRProtoHeader),
                    RESPONSE.error.message, RESPONSE.error.length);
        }
    }

    short2buff(RESPONSE_STATUS >= 0 ? RESPONSE_STATUS : -1 * RESPONSE_STATUS,
            proto_header->status);
    proto_header->cmd = RESPONSE.header.cmd;
    int2buff(RESPONSE.header.body_len, proto_header->body_len);
    task->length = sizeof(FDIRProtoHeader) + RESPONSE.header.body_len;

    r = sf_send_add_event(task);
    time_used = (int)(get_current_time_us() - TASK_ARG->req_start_time);
    if (time_used > 50 * 1000) {
        lwarning("process a request timed used: %d us, "
                "cmd: %d, req body len: %d, resp body len: %d",
                time_used, REQUEST.header.cmd,
                REQUEST.header.body_len,
                RESPONSE.header.body_len);
    }

    if (REQUEST.header.cmd != FDIR_CLUSTER_PROTO_PING_MASTER_REQ) {
    logInfo("file: "__FILE__", line: %d, "
            "client ip: %s, req cmd: %d, req body_len: %d, "
            "resp cmd: %d, status: %d, resp body_len: %d, "
            "time used: %d us", __LINE__,
            task->client_ip, REQUEST.header.cmd,
            REQUEST.header.body_len, RESPONSE.header.cmd,
            RESPONSE_STATUS, RESPONSE.header.body_len, time_used);
    }

    return r == 0 ? RESPONSE_STATUS : r;
}

int cluster_deal_task(struct fast_task_info *task)
{
    int result;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "nio_stage: %d, SF_NIO_STAGE_CONTINUE: %d", __LINE__,
            task->nio_stage, SF_NIO_STAGE_CONTINUE);
            */

    if (task->nio_stage == SF_NIO_STAGE_CONTINUE) {
        task->nio_stage = SF_NIO_STAGE_SEND;
        if (TASK_ARG->context.deal_func != NULL) {
            result = TASK_ARG->context.deal_func(task);
        } else {
            result = RESPONSE_STATUS;
            if (result == TASK_STATUS_CONTINUE) {
                logError("file: "__FILE__", line: %d, "
                        "unexpect status: %d", __LINE__, result);
                result = EBUSY;
            }
        }
    } else {
        init_task_context(task);

        switch (REQUEST.header.cmd) {
            case FDIR_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = FDIR_PROTO_ACTIVE_TEST_RESP;
                result = cluster_deal_actvie_test(task);
                break;
            case FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
                result = cluster_deal_get_server_status(task);
                break;
            case FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER:
            case FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER:
                result = cluster_deal_next_master(task);
                break;
            case FDIR_CLUSTER_PROTO_JOIN_MASTER:
                result = cluster_deal_join_master(task);
                break;
            case FDIR_CLUSTER_PROTO_PING_MASTER_REQ:
                result = cluster_deal_ping_master(task);
                break;
            case FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ:
                result = cluster_deal_join_slave_req(task);
                break;
            case FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP:
                result = cluster_deal_join_slave_resp(task);
                TASK_ARG->context.need_response = false;
                break;
            case FDIR_CLUSTER_PROTO_MASTER_PUSH_BINLOG_REQ:
                result = cluster_deal_push_binlog(task);
                break;
            case FDIR_PROTO_ACK:
                result = cluster_deal_slave_ack(task);
                TASK_ARG->context.need_response = false;
                break;
            default:
                RESPONSE.error.length = sprintf(
                        RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = -EINVAL;
                break;
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return deal_task_done(task);
    }
}


void *cluster_alloc_thread_extra_data(const int thread_index)
{
    FDIRServerContext *server_context;

    server_context = (FDIRServerContext *)malloc(sizeof(FDIRServerContext));
    if (server_context == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail, errno: %d, error info: %s",
                __LINE__, (int)sizeof(FDIRServerContext),
                errno, strerror(errno));
        return NULL;
    }

    memset(server_context, 0, sizeof(FDIRServerContext));
    return server_context;
}

int cluster_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FDIRServerContext *server_ctx;

    if (!MYSELF_IS_MASTER) {
        return 0;
    }
 
    server_ctx = (FDIRServerContext *)thread_data->arg;
    return 0;
    //return binlog_replication_process(server_ctx);
}

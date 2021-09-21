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

//service_handler.c

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
#include "fastcommon/system_info.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_service.h"
#include "sf/sf_global.h"
#include "sf/idempotency/server/server_channel.h"
#include "sf/idempotency/server/server_handler.h"
#include "fastcfs/auth/fcfs_auth_for_server.h"
#include "common/fdir_proto.h"
#include "binlog/binlog_pack.h"
#include "binlog/binlog_producer.h"
#include "binlog/binlog_write.h"
#include "server_global.h"
#include "server_func.h"
#include "dentry.h"
#include "inode_index.h"
#include "cluster_relationship.h"
#include "common_handler.h"
#include "ns_manager.h"
#include "service_handler.h"

static volatile int64_t next_token = 0;   //next token for dentry list
static int64_t dstat_mflags_mask = 0;

typedef int (*deal_task_func)(struct fast_task_info *task);

int service_handler_init()
{
    FDIRStatModifyFlags mask;

    mask.flags = 0;
    mask.mode = 1;
    mask.atime = 1;
    mask.ctime = 1;
    mask.mtime = 1;
    mask.uid  = 1;
    mask.gid  = 1;
    mask.size = 1;
    dstat_mflags_mask = mask.flags;

    next_token = ((int64_t)g_current_time) << 32;

    return idempotency_channel_init(SF_IDEMPOTENCY_MAX_CHANNEL_ID,
            SF_IDEMPOTENCY_DEFAULT_REQUEST_HINT_CAPACITY,
            SF_IDEMPOTENCY_DEFAULT_CHANNEL_RESERVE_INTERVAL,
            SF_IDEMPOTENCY_DEFAULT_CHANNEL_SHARED_LOCK_COUNT);
}

int service_handler_destroy()
{   
    return 0;
}

static inline void release_flock_task(struct fast_task_info *task,
        FLockTask *flck)
{
    fc_list_del_init(&flck->clink);
    inode_index_flock_release(flck);
}

void service_task_finish_cleanup(struct fast_task_info *task)
{
    switch (SERVER_TASK_TYPE) {
        case SF_SERVER_TASK_TYPE_CHANNEL_HOLDER:
        case SF_SERVER_TASK_TYPE_CHANNEL_USER:
            if (IDEMPOTENCY_CHANNEL != NULL) {
                idempotency_channel_release(IDEMPOTENCY_CHANNEL,
                        SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_HOLDER);
                IDEMPOTENCY_CHANNEL = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "IDEMPOTENCY_CHANNEL is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        case FDIR_SERVER_TASK_TYPE_NSS_SUBSCRIBE:
            if (NS_SUBSCRIBER != NULL) {
                ns_subscribe_unregister(NS_SUBSCRIBER);
                NS_SUBSCRIBER = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "NS_SUBSCRIBER is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }

    if (IDEMPOTENCY_CHANNEL != NULL) {
        logError("file: "__FILE__", line: %d, "
                "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                "IDEMPOTENCY_CHANNEL: %p != NULL", __LINE__, task,
                SERVER_TASK_TYPE, IDEMPOTENCY_CHANNEL);
        IDEMPOTENCY_CHANNEL = NULL;
    }

    if (!fc_list_empty(FTASK_HEAD_PTR)) {
        FLockTask *flck;
        FLockTask *next;
        fc_list_for_each_entry_safe(flck, next, FTASK_HEAD_PTR, clink) {
            release_flock_task(task, flck);
        }
    }

    if (SYS_LOCK_TASK != NULL) {
        inode_index_sys_lock_release(SYS_LOCK_TASK);
        SYS_LOCK_TASK = NULL;
    }

    dentry_array_free(&DENTRY_LIST_CACHE.array);
    sf_task_finish_clean_up(task);
}

static inline int service_check_master(struct fast_task_info *task)
{
    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return SF_RETRIABLE_ERROR_NOT_MASTER;
    }

    return 0;
}

static int service_deal_client_join(struct fast_task_info *task)
{
    int result;
    uint32_t channel_id;
    int key;
    int flags;
    int my_auth_enabled;
    int req_auth_enabled;
    FDIRProtoClientJoinReq *req;
    FDIRProtoClientJoinResp *join_resp;

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoClientJoinReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoClientJoinReq *)REQUEST.body;
    flags = buff2int(req->flags);
    channel_id = buff2int(req->idempotency.channel_id);
    key = buff2int(req->idempotency.key);

    my_auth_enabled = (AUTH_ENABLED ? 1 : 0);
    req_auth_enabled = (req->auth_enabled ? 1 : 0);
    if (req_auth_enabled != my_auth_enabled) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client auth enabled: %d != mine: %d",
                req_auth_enabled, my_auth_enabled);
        return EINVAL;
    }

    if (memcmp(req->config_sign, CLUSTER_CONFIG_SIGN_BUF,
                SF_CLUSTER_CONFIG_SIGN_LEN) != 0)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client's cluster.conf is not consistent with mine");
        return EINVAL;
    }

    if ((flags & FDIR_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST) != 0) {
        if (IDEMPOTENCY_CHANNEL != NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "channel already exist, the channel id: %d",
                    IDEMPOTENCY_CHANNEL->id);
            return EEXIST;
        }

        IDEMPOTENCY_CHANNEL = idempotency_channel_find_and_hold(
                channel_id, key, &result);
        if (IDEMPOTENCY_CHANNEL == NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "find channel fail, channel id: %d, result: %d",
                    channel_id, result);
            return SF_RETRIABLE_ERROR_NO_CHANNEL;
        }

        SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_CHANNEL_USER;
    }

    join_resp = (FDIRProtoClientJoinResp *)SF_PROTO_RESP_BODY(task);
    int2buff(g_sf_global_vars.min_buff_size - 128,
            join_resp->buffer_size);
    RESPONSE.header.body_len = sizeof(FDIRProtoClientJoinResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_CLIENT_JOIN_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_service_stat(struct fast_task_info *task)
{
    int result;
    FDIRDentryCounters counters;
    FDIRProtoServiceStatResp *stat_resp;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    data_thread_sum_counters(&counters);
    stat_resp = (FDIRProtoServiceStatResp *)SF_PROTO_RESP_BODY(task);

    stat_resp->is_master = (CLUSTER_MYSELF_PTR ==
        CLUSTER_MASTER_ATOM_PTR ? 1 : 0);
    stat_resp->status = __sync_fetch_and_add(&CLUSTER_MYSELF_PTR->status, 0);
    int2buff(CLUSTER_MYSELF_PTR->server->id, stat_resp->server_id);

    int2buff(SF_G_CONN_CURRENT_COUNT, stat_resp->connection.current_count);
    int2buff(SF_G_CONN_MAX_COUNT, stat_resp->connection.max_count);

    long2buff(FC_ATOMIC_GET(DATA_CURRENT_VERSION),
            stat_resp->binlog.current_version);
    long2buff(g_binlog_writer_ctx.writer.fw.total_count,
            stat_resp->binlog.writer.total_count);
    long2buff(g_binlog_writer_ctx.writer.version_ctx.next,
            stat_resp->binlog.writer.next_version);
    int2buff(g_binlog_writer_ctx.writer.version_ctx.ring.waiting_count,
            stat_resp->binlog.writer.waiting_count);
    int2buff(g_binlog_writer_ctx.writer.version_ctx.ring.max_waitings,
            stat_resp->binlog.writer.max_waitings);

    long2buff(CURRENT_INODE_SN, stat_resp->dentry.current_inode_sn);
    long2buff(counters.ns, stat_resp->dentry.counters.ns);
    long2buff(counters.dir, stat_resp->dentry.counters.dir);
    long2buff(counters.file, stat_resp->dentry.counters.file);

    RESPONSE.header.body_len = sizeof(FDIRProtoServiceStatResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SERVICE_STAT_RESP;
    TASK_CTX.common.response_done = true;

    return 0;
}

static int service_deal_cluster_stat(struct fast_task_info *task)
{
    int result;
    FDIRProtoClusterStatRespBodyHeader *body_header;
    FDIRProtoClusterStatRespBodyPart *body_part;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    body_header = (FDIRProtoClusterStatRespBodyHeader *)
        SF_PROTO_RESP_BODY(task);
    body_part = (FDIRProtoClusterStatRespBodyPart *)(body_header + 1);
    int2buff(CLUSTER_SERVER_ARRAY.count, body_header->count);

    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++, body_part++) {
        int2buff(cs->server->id, body_part->server_id);
        body_part->is_master = (cs == CLUSTER_MASTER_ATOM_PTR ? 1 : 0);
        body_part->status = __sync_fetch_and_add(&cs->status, 0);

        snprintf(body_part->ip_addr, sizeof(body_part->ip_addr), "%s",
                SERVICE_GROUP_ADDRESS_FIRST_IP(cs->server));
        short2buff(SERVICE_GROUP_ADDRESS_FIRST_PORT(cs->server),
                body_part->port);
    }

    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_namespace_stat(struct fast_task_info *task)
{
    int result;
    int expect_blen;
    static int64_t mem_size = 0;
    FDIRNamespaceStat stat;
    int64_t inode_total;
    string_t ns;
    FDIRProtoNamespaceStatReq *req;
    FDIRProtoNamespaceStatResp *resp;

    if ((result=server_check_min_body_length(sizeof(
                        FDIRProtoNamespaceStatReq) + 1)) != 0)
    {
        return result;
    }

    req = (FDIRProtoNamespaceStatReq *)REQUEST.body;
    ns.len = req->ns_len;
    ns.str = req->ns_str;
    expect_blen = sizeof(FDIRProtoNamespaceStatReq) + ns.len;
    if (expect_blen != REQUEST.header.body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "request body length: %d != expect: %d",
                REQUEST.header.body_len, expect_blen);
        return EINVAL;
    }

    if (mem_size == 0) {
        get_sys_total_mem_size(&mem_size);
    }

    /*
    logInfo("mem_size: %d MB, sizeof(FDIRServerDentry): %d",
            (int)(mem_size / (1024 * 1024)), (int)sizeof(FDIRServerDentry));
            */

    inode_total = mem_size / 300;
    if ((result=fdir_namespace_stat(&ns, &stat)) != 0) {
        return result;
    }

    resp = (FDIRProtoNamespaceStatResp *)SF_PROTO_RESP_BODY(task);
    long2buff(inode_total, resp->inode_counters.total);
    long2buff(stat.used_inodes, resp->inode_counters.used);
    long2buff(inode_total - stat.used_inodes, resp->inode_counters.avail);
    long2buff(stat.used_bytes, resp->used_bytes);

    RESPONSE.header.body_len = sizeof(FDIRProtoNamespaceStatResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_NAMESPACE_STAT_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_nss_subscribe(struct fast_task_info *task)
{
    int result;

    if ((result=service_check_master(task)) != 0) {
        return result;
    }

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    if (SERVER_TASK_TYPE != SF_SERVER_TASK_TYPE_NONE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unexpect server type: %d != expect: %d",
                SERVER_TASK_TYPE, SF_SERVER_TASK_TYPE_NONE);
        return EINVAL;
    }

    if ((NS_SUBSCRIBER=ns_subscribe_register()) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "namespace subscribe fail, exceed max subscribers: %d",
                FDIR_MAX_NS_SUBSCRIBERS);
        return EOVERFLOW;
    }
    fdir_namespace_push_all_to_holding_queue(NS_SUBSCRIBER);

    SERVER_TASK_TYPE = FDIR_SERVER_TASK_TYPE_NSS_SUBSCRIBE;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_RESP;
    return 0;
}

static int service_deal_nss_fetch(struct fast_task_info *task)
{
    int result;
    struct fc_queue_info qinfo;
    FDIRProtoNSSFetchRespBodyHeader *body_header;
    FDIRProtoNSSFetchRespBodyPart *body_part;
    FDIRNSSubscribeEntry *entry;
    FDIRNSSubscribeEntry *current;
    char *p;
    char *end;
    int count;

    if ((result=service_check_master(task)) != 0) {
        return result;
    }

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    if (SERVER_TASK_TYPE != FDIR_SERVER_TASK_TYPE_NSS_SUBSCRIBE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unexpect server type: %d != expect: %d",
                SERVER_TASK_TYPE, FDIR_SERVER_TASK_TYPE_NSS_SUBSCRIBE);
        return EPERM;
    }
    if (NS_SUBSCRIBER == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "internal error: subscriber ptr is NULL");
        return EBUSY;
    }

    if (fc_queue_empty(NS_SUBSCRIBER->queues +
                FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING))
    {
        ns_subscribe_holding_to_sending_queue(NS_SUBSCRIBER);
    }

    body_header = (FDIRProtoNSSFetchRespBodyHeader *)SF_PROTO_RESP_BODY(task);
    p = (char *)(body_header + 1);
    end = task->data + task->size;
    count = 0;

    fc_queue_try_pop_to_queue(NS_SUBSCRIBER->queues +
            FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING, &qinfo);
    entry = (FDIRNSSubscribeEntry *)qinfo.head;
    while (entry != NULL) {
        current = entry;

        body_part = (FDIRProtoNSSFetchRespBodyPart *)p;
        p += sizeof(FDIRProtoNSSFetchRespBodyPart) + current->ns->name.len;
        if (p > end) {
            p -= sizeof(FDIRProtoNSSFetchRespBodyPart) +
                current->ns->name.len;
            break;
        }

        long2buff(__sync_add_and_fetch(&current->ns->used_bytes, 0),
                body_part->used_bytes);
        body_part->ns_name.len = current->ns->name.len;
        memcpy(body_part->ns_name.str, current->ns->name.str,
                current->ns->name.len);

        entry = entry->entries[FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING].next;
        __sync_bool_compare_and_swap(&current->entries[
                FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING].
                in_queue, 1, 0);
        ++count;
    }

    if (entry == NULL) {
        body_header->is_last = 1;
    } else {
        body_header->is_last = 0;
        qinfo.head = entry;
        fc_queue_push_queue_to_head_silence(NS_SUBSCRIBER->queues +
                FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING, &qinfo);
    }

    int2buff(count, body_header->count);
    RESPONSE.header.body_len = p - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_NSS_FETCH_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_get_master(struct fast_task_info *task)
{
    int result;
    FDIRProtoGetServerResp *resp;
    FDIRClusterServerInfo *master;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    master = CLUSTER_MASTER_ATOM_PTR;
    if (master == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "the master NOT exist");
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FDIRProtoGetServerResp *)SF_PROTO_RESP_BODY(task);
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                master->server), task->client_ip);

    int2buff(master->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerResp);
    TASK_CTX.common.response_done = true;

    return 0;
}

int service_deal_get_group_servers(struct fast_task_info *task)
{
    int result;
    int group_id;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;
    SFProtoGetGroupServersReq *req;
    SFProtoGetGroupServersRespBodyHeader *body_header;
    SFProtoGetGroupServersRespBodyPart *body_part;

    if ((result=server_expect_body_length(sizeof(
                        SFProtoGetGroupServersReq))) != 0)
    {
        return result;
    }

    req = (SFProtoGetGroupServersReq *)REQUEST.body;
    group_id = buff2int(req->group_id);
    if (group_id != 1) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid group_id: %d != 1", group_id);
        return EINVAL;
    }

    body_header = (SFProtoGetGroupServersRespBodyHeader *)SF_PROTO_RESP_BODY(task);
    body_part = (SFProtoGetGroupServersRespBodyPart *)(body_header + 1);
    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++, body_part++) {
        int2buff(cs->server->id, body_part->server_id);
        body_part->is_master = (cs == CLUSTER_MASTER_ATOM_PTR);
        body_part->is_active = (FC_ATOMIC_GET(cs->status) ==
                FDIR_SERVER_STATUS_ACTIVE) ? 1 : 0;
    }
    int2buff(CLUSTER_SERVER_ARRAY.count, body_header->count);

    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = SF_SERVICE_PROTO_GET_GROUP_SERVERS_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_get_slaves(struct fast_task_info *task)
{
    int result;
    FDIRProtoGetSlavesRespBodyHeader *body_header;
    FDIRProtoGetSlavesRespBodyPart *part_start;
    FDIRProtoGetSlavesRespBodyPart *body_part;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    body_header = (FDIRProtoGetSlavesRespBodyHeader *)SF_PROTO_RESP_BODY(task);
    part_start = (FDIRProtoGetSlavesRespBodyPart *)(body_header + 1);
    body_part = part_start;

    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++) {
        if (cs == CLUSTER_MASTER_ATOM_PTR) {
            continue;
        }

        int2buff(cs->server->id, body_part->server_id);
        body_part->status = __sync_fetch_and_add(&cs->status, 0);

        addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                cs->server), task->client_ip);
        snprintf(body_part->ip_addr, sizeof(body_part->ip_addr),
                "%s", addr->conn.ip_addr);
        short2buff(addr->conn.port, body_part->port);

        body_part++;
    }
    int2buff(body_part - part_start, body_header->count);

    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GET_SLAVES_RESP;
    TASK_CTX.common.response_done = true;

    return 0;
}

static FDIRClusterServerInfo *get_readable_server()
{
    int index;
    int old_index;
    int acc_index;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;

    index = rand() % CLUSTER_SERVER_ARRAY.count;
    if (__sync_fetch_and_add(&CLUSTER_SERVER_ARRAY.servers[index].status, 0) ==
            FDIR_SERVER_STATUS_ACTIVE)
    {
        return CLUSTER_SERVER_ARRAY.servers + index;
    }

    acc_index = 0;
    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    do {
        old_index = acc_index;
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++) {
            if (__sync_fetch_and_add(&cs->status, 0) ==
                    FDIR_SERVER_STATUS_ACTIVE)
            {
                if (acc_index++ == index) {
                    return cs;
                }
            }
        }
    } while (acc_index - old_index > 0);

    return NULL;
}

static int service_deal_get_readable_server(struct fast_task_info *task)
{
    FDIRClusterServerInfo *cs;
    FDIRProtoGetServerResp *resp;
    const FCAddressInfo *addr;

    if ((cs=get_readable_server()) == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "no active server");
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FDIRProtoGetServerResp *)SF_PROTO_RESP_BODY(task);
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                cs->server), task->client_ip);

    int2buff(cs->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP;
    TASK_CTX.common.response_done = true;

    return 0;
}

static int server_parse_dentry_info(struct fast_task_info *task,
        char *start, FDIRDEntryFullName *fullname)
{
    FDIRProtoDEntryInfo *proto_dentry;

    proto_dentry = (FDIRProtoDEntryInfo *)start;
    fullname->ns.len = proto_dentry->ns_len;
    fullname->ns.str = proto_dentry->ns_str;
    fullname->path.len = buff2short(proto_dentry->path_len);
    fullname->path.str = proto_dentry->ns_str + fullname->ns.len;

    if (fullname->ns.len <= 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid namespace length: %d <= 0",
                fullname->ns.len);
        return EINVAL;
    }
    if (fullname->ns.len > NAME_MAX) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid namespace length: %d > %d",
                fullname->ns.len, NAME_MAX);
        return EINVAL;
    }

    if (fullname->path.len <= 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid path length: %d <= 0",
                fullname->path.len);
        return EINVAL;
    }
    if (fullname->path.len > PATH_MAX) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid path length: %d > %d",
                fullname->path.len, PATH_MAX);
        return EINVAL;
    }

    if (fullname->path.str[0] != '/') {
        RESPONSE.error.length = snprintf(
                RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "invalid path: %.*s", fullname->path.len,
                fullname->path.str);
        return EINVAL;
    }

    return 0;
}

static int server_check_and_parse_dentry(struct fast_task_info *task,
        const int front_part_size, FDIRDEntryFullName *fullname)
{
    int result;
    int fixed_part_size;
    int req_body_len;

    fixed_part_size = front_part_size + sizeof(FDIRProtoDEntryInfo);
    if ((result=server_check_body_length(fixed_part_size + 2,
                    fixed_part_size + NAME_MAX + PATH_MAX)) != 0)
    {
        return result;
    }

    if ((result=server_parse_dentry_info(task, REQUEST.body +
                    front_part_size, fullname)) != 0)
    {
        return result;
    }

    req_body_len = fixed_part_size + fullname->ns.len +
        fullname->path.len;
    if (req_body_len != REQUEST.header.body_len) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "body length: %d != expect: %d",
                REQUEST.header.body_len, req_body_len);
        return EINVAL;
    }

    return 0;
}

static inline int check_name_length(struct fast_task_info *task,
        const int length, const char *caption)
{
    if (length <= 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid %s length: %d <= 0",
                caption, length);
        return EINVAL;
    }
    if (length > NAME_MAX) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid %s length: %d > %d",
                caption, length, NAME_MAX);
        return EINVAL;
    }
    return 0;
}

static int server_parse_pname(struct fast_task_info *task,
        const int front_part_size, string_t *ns, string_t *name,
        FDIRServerDentry **parent_dentry)
{
    FDIRProtoDEntryByPName *req;
    int64_t parent_inode;
    int result;

    req = (FDIRProtoDEntryByPName *)(REQUEST.body + front_part_size);
    if ((result=check_name_length(task, req->ns_len, "namespace")) != 0) {
        return result;
    }
    if ((result=check_name_length(task, req->name_len, "path name")) != 0) {
        return result;
    }

    ns->len = req->ns_len;
    ns->str = req->ns_str;
    name->len = req->name_len;
    name->str = ns->str + ns->len;
    parent_inode = buff2long(req->parent_inode);
    if ((*parent_dentry=inode_index_get_dentry(parent_inode)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "parent inode: %"PRId64" not exist", parent_inode);
        return ENOENT;
    }

    return 0;
}

static int server_check_and_parse_pname(struct fast_task_info *task,
        const int front_part_size, string_t *ns, string_t *name,
        FDIRServerDentry **parent_dentry)
{
    int fixed_part_size;
    int result;

    fixed_part_size = front_part_size + sizeof(FDIRProtoDEntryByPName);
    if ((result=server_check_body_length(fixed_part_size + 2,
                    fixed_part_size + 2 * NAME_MAX)) != 0)
    {
        return result;
    }

    if ((result=server_parse_pname(task, front_part_size, ns, name,
                    parent_dentry)) != 0)
    {
        return result;
    }

    if (fixed_part_size + ns->len + name->len != REQUEST.header.body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d", REQUEST.header.body_len,
                fixed_part_size + ns->len + name->len);
        return EINVAL;
    }

    return 0;
}

static int check_and_parse_inode_info(struct fast_task_info *task,
        const int front_part_size, string_t *ns, FDIRServerDentry **dentry)
{
    FDIRProtoInodeInfo *req;
    int64_t inode;
    int result;

    req = (FDIRProtoInodeInfo *)(REQUEST.body + front_part_size);
    if ((result=check_name_length(task, req->ns_len, "namespace")) != 0) {
        return result;
    }

    ns->len = req->ns_len;
    ns->str = req->ns_str;
    inode = buff2long(req->inode);
    if ((*dentry=inode_index_get_dentry(inode)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "inode: %"PRId64" not exist", inode);
        return ENOENT;
    }

    return 0;
}

static inline int alloc_record_object(struct fast_task_info *task)
{
    RECORD = (FDIRBinlogRecord *)fast_mblock_alloc_object(
            &((FDIRServerContext *)task->thread_data->arg)->
            service.record_allocator);
    if (RECORD == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "system busy, please try later");
        return EBUSY;
    }

    RECORD->me.pname.parent_inode = 0;
    return 0;
}


static inline void free_record_object(struct fast_task_info *task)
{
    fast_mblock_free_object(&((FDIRServerContext *)task->
                thread_data->arg)->service.record_allocator, RECORD);
    RECORD = NULL;
}

static void service_idempotency_request_finish(struct fast_task_info *task,
        const int result)
{
    if (IDEMPOTENCY_REQUEST != NULL) {
        IDEMPOTENCY_REQUEST->finished = true;
        IDEMPOTENCY_REQUEST->output.result = result;
        idempotency_request_release(IDEMPOTENCY_REQUEST);

        /* server task type for channel ONLY, do NOT set task type to NONE!!! */
        IDEMPOTENCY_REQUEST = NULL;
    }
}

static int handle_replica_done(struct fast_task_info *task)
{
    int result;

    task->continue_callback = NULL;
    service_idempotency_request_finish(task, 0);

    if (RBUFFER != NULL) {
        result = push_to_binlog_write_queue(RBUFFER);
        server_binlog_release_rbuffer(RBUFFER);
        RBUFFER = NULL;
    } else {
        logError("file: "__FILE__", line: %d, "
                "rbuffer is NULL, some mistake happen?",
                __LINE__);
        result = 0;
    }

    sf_release_task(task);
    return result;
}

static inline int do_binlog_produce(struct fast_task_info *task,
        ServerBinlogRecordBuffer *rbuffer)
{
    rbuffer->args = task;
    RBUFFER = rbuffer;
    if (SLAVE_SERVER_COUNT > 0) {
        task->continue_callback = handle_replica_done;
        binlog_push_to_producer_queue(rbuffer);
        return TASK_STATUS_CONTINUE;
    } else {
        return handle_replica_done(task);
    }
}

static int server_binlog_produce(struct fast_task_info *task)
{
    ServerBinlogRecordBuffer *rbuffer;
    int result;

    if ((rbuffer=server_binlog_alloc_hold_rbuffer()) == NULL) {
        free_record_object(task);
        sf_release_task(task);
        return ENOMEM;
    }

    rbuffer->data_version.first = RECORD->data_version;
    rbuffer->data_version.last = RECORD->data_version;
    RECORD->timestamp = g_current_time;

    result = binlog_pack_record(RECORD, &rbuffer->buffer);
    free_record_object(task);

    if (result == 0) {
        return do_binlog_produce(task, rbuffer);
    } else {
        server_binlog_free_rbuffer(rbuffer);
        sf_release_task(task);
        return result;
    }
}

static inline void dstat_output(struct fast_task_info *task,
            const int64_t inode, const FDIRDEntryStat *stat)
{
    FDIRProtoStatDEntryResp *resp;

    resp = (FDIRProtoStatDEntryResp *)(task->data + sizeof(FDIRProtoHeader));
    long2buff(inode, resp->inode);
    fdir_proto_pack_dentry_stat_ex(stat, &resp->stat, true);
    RESPONSE.header.body_len = sizeof(FDIRProtoStatDEntryResp);
    TASK_CTX.common.response_done = true;
}

static inline void dentry_stat_output(struct fast_task_info *task,
        FDIRServerDentry **dentry)
{
    if (FDIR_IS_DENTRY_HARD_LINK((*dentry)->stat.mode)) {
        *dentry = (*dentry)->src_dentry;
    }
    dstat_output(task, (*dentry)->inode, &(*dentry)->stat);
}

static inline void set_update_result_and_output(
        struct fast_task_info *task, FDIRServerDentry *dentry)
{
    dentry_stat_output(task, &dentry);
    if (IDEMPOTENCY_REQUEST != NULL) {
        FDIRDEntryInfo *dinfo;

        dinfo = (FDIRDEntryInfo *)IDEMPOTENCY_REQUEST->output.response;
        IDEMPOTENCY_REQUEST->output.flags = TASK_UPDATE_FLAG_OUTPUT_DENTRY;
        dinfo->inode = dentry->inode;
        dinfo->stat = dentry->stat;
    }
}

static void record_deal_done_notify(FDIRBinlogRecord *record,
        const int result, const bool is_error)
{
    struct fast_task_info *task;
    char xattr_name_buff[256];

    task = (struct fast_task_info *)record->notify.args;
    if (result != 0) {
        int log_level;

        if (REQUEST.header.cmd == FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_REQ ||
                REQUEST.header.cmd == FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_REQ)
        {
            if (result == ENODATA) {
                log_level = LOG_DEBUG;
            } else {
                log_level = is_error ? LOG_WARNING : LOG_DEBUG;
            }
            snprintf(xattr_name_buff, sizeof(xattr_name_buff),
                    ", xattr name: %.*s", record->xattr.key.len,
                    record->xattr.key.str);
        } else {
            log_level = is_error ? LOG_ERR : LOG_WARNING;
            *xattr_name_buff = '\0';
        }

        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, "
                "client ip: %s, %s dentry fail, "
                "errno: %d, error info: %s, "
                "parent inode: %"PRId64", current inode: %"PRId64", "
                "namespace: %.*s, dir name: %.*s%s",
                __LINE__, task->client_ip,
                get_operation_caption(record->operation),
                result, STRERROR(result), record->me.pname.parent_inode,
                record->inode, record->ns.len, record->ns.str,
                record->me.pname.name.len, record->me.pname.name.str,
                xattr_name_buff);
    } else {
        if (record->operation == BINLOG_OP_CREATE_DENTRY_INT ||
                record->operation == BINLOG_OP_REMOVE_DENTRY_INT)
        {
            set_update_result_and_output(task, record->me.dentry);
        } else if (record->operation == BINLOG_OP_RENAME_DENTRY_INT) {
            if (RECORD->rename.overwritten != NULL) {
                set_update_result_and_output(task,
                        RECORD->rename.overwritten);
            }
        }
    }

    RESPONSE_STATUS = result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
}

static int handle_record_deal_done(struct fast_task_info *task)
{
    int result;
    bool need_release;

    task->continue_callback = NULL;
    if (RESPONSE_STATUS == 0) {
        result = server_binlog_produce(task);
        need_release = false;
    } else {
        result = RESPONSE_STATUS;
        need_release = true;
    }

    if (result != TASK_STATUS_CONTINUE) {
        service_idempotency_request_finish(task, result);
    }

    if (need_release) {
        free_record_object(task);
        sf_release_task(task);
    }
    return result;
}

static inline int push_record_to_data_thread_queue(struct fast_task_info *task)
{
    sf_hold_task(task);

    task->continue_callback = handle_record_deal_done;
    RECORD->notify.func = record_deal_done_notify; //call by data thread
    RECORD->notify.args = task;
    RECORD->type = fdir_record_type_update;
    push_to_data_thread_queue(RECORD);
    return TASK_STATUS_CONTINUE;
}

static int service_set_record_pname_info(struct fast_task_info *task,
        const int reserved_size)
{
    char *p;

    RECORD->inode = RECORD->data_version = 0;
    RECORD->options.flags = 0;
    RECORD->options.path_info.flags = BINLOG_OPTIONS_PATH_ENABLED;
    RECORD->hash_code = simple_hash(RECORD->ns.str, RECORD->ns.len);

    if (REQUEST.header.body_len > reserved_size) {
        if ((REQUEST.header.body_len + RECORD->ns.len +
                    RECORD->me.pname.name.len) < task->size)
        {
            p = REQUEST.body + REQUEST.header.body_len;
        } else {
            p = REQUEST.body + reserved_size;
        }
    } else {
        p = REQUEST.body + reserved_size;
    }

    if (p + RECORD->ns.len + RECORD->me.pname.name.len >
            task->data + task->size)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "task pkg size: %d is too small", task->size);
        return EOVERFLOW;
    }

    memcpy(p, RECORD->ns.str, RECORD->ns.len);
    memcpy(p + RECORD->ns.len, RECORD->me.pname.name.str,
            RECORD->me.pname.name.len);

    RECORD->ns.str = p;
    RECORD->me.pname.name.str = p + RECORD->ns.len;
    return 0;
}

#define init_record_for_create(task, mode) \
    init_record_for_create_ex(task, mode, false)

static void init_record_for_create_ex(struct fast_task_info *task,
        const int mode, const bool is_hdlink)
{
    int new_mode;
    if (is_hdlink) {
        new_mode = FDIR_SET_DENTRY_HARD_LINK((mode & (~S_IFMT)) |
                (RECORD->hdlink.src_dentry->stat.mode & S_IFMT));
    } else {
        new_mode = FDIR_UNSET_DENTRY_HARD_LINK(mode);
    }
    RECORD->stat.mode = new_mode;
    RECORD->operation = BINLOG_OP_CREATE_DENTRY_INT;
    RECORD->stat.uid = buff2int(((FDIRProtoCreateDEntryFront *)
                REQUEST.body)->uid);
    RECORD->stat.gid = buff2int(((FDIRProtoCreateDEntryFront *)
                REQUEST.body)->gid);
    RECORD->stat.size = 0;
    RECORD->stat.atime = RECORD->stat.btime = RECORD->stat.ctime =
        RECORD->stat.mtime = g_current_time;
    RECORD->options.atime = RECORD->options.btime = RECORD->options.ctime =
        RECORD->options.mtime = 1;
    RECORD->options.mode = 1;
    RECORD->options.uid = 1;
    RECORD->options.gid = 1;
}

static int server_parse_dentry_for_update(struct fast_task_info *task,
        const int front_part_size, const bool is_create)
{
    FDIRDEntryFullName fullname;
    FDIRServerDentry *parent_dentry;
    string_t name;
    int result;

    if ((result=server_check_and_parse_dentry(task,
                    front_part_size, &fullname)) != 0)
    {
        return result;
    }

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "ns: %.*s, path: %.*s", __LINE__, __FUNCTION__,
            fullname.ns.len, fullname.ns.str,
            fullname.path.len, fullname.path.str);
            */

    if ((result=dentry_find_parent(&fullname, &parent_dentry, &name)) != 0) {
        if (!(result == ENOENT && is_create)) {
            return result;
        }
        if (!FDIR_IS_ROOT_PATH(fullname.path)) {
            return result;
        }
    } else if (is_create && FDIR_IS_ROOT_PATH(fullname.path)) {
        return EEXIST;
    }

    if ((result=alloc_record_object(task)) != 0) {
        return result;
    }

    RECORD->ns = fullname.ns;
    RECORD->me.pname.name = name;
    if (parent_dentry != NULL) {
        RECORD->me.pname.parent_inode = parent_dentry->inode;
    }
    RECORD->me.parent = parent_dentry;
    RECORD->me.dentry = NULL;
    return service_set_record_pname_info(task,
            sizeof(FDIRProtoStatDEntryResp));
}

static int server_parse_pname_for_update(struct fast_task_info *task,
        const int front_part_size)
{
    int result;
    FDIRServerDentry *parent_dentry;
    string_t ns;
    string_t name;

    if ((result=server_check_and_parse_pname(task, front_part_size,
                    &ns, &name, &parent_dentry)) != 0)
    {
        return result;
    }

    if ((result=alloc_record_object(task)) != 0) {
        return result;
    }

    RECORD->ns = ns;
    RECORD->me.pname.name = name;
    if (parent_dentry != NULL) {
        RECORD->me.pname.parent_inode = parent_dentry->inode;
    }
    RECORD->me.parent = parent_dentry;
    RECORD->me.dentry = NULL;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "parent inode: %"PRId64", ns: %.*s, name: %.*s",
            __LINE__, RECORD->me.pname.parent_inode, RECORD->ns.len,
            RECORD->ns.str, RECORD->me.pname.name.len,
            RECORD->me.pname.name.str);
            */

    return service_set_record_pname_info(task,
            sizeof(FDIRProtoStatDEntryResp));
}

static int server_parse_inode_for_update(struct fast_task_info *task,
        const int front_part_size)
{
    int result;
    int body_len;
    FDIRServerDentry *dentry;
    string_t ns;

    if ((result=check_and_parse_inode_info(task,
                    front_part_size, &ns, &dentry)) != 0)
    {
        return result;
    }

    body_len = front_part_size + sizeof(FDIRProtoInodeInfo) + ns.len;
    if (REQUEST.header.body_len != body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, body_len);
        return EINVAL;
    }

    if ((result=alloc_record_object(task)) != 0) {
        return result;
    }

    RECORD->data_version = 0;
    RECORD->inode = dentry->inode;
    RECORD->ns = ns;
    RECORD->hash_code = simple_hash(ns.str, ns.len);
    RECORD->me.dentry = dentry;
    RECORD->me.pname.name = dentry->name;
    if (dentry->parent != NULL) {
        RECORD->me.pname.parent_inode = dentry->parent->inode;
    }
    return 0;
}

static int service_update_prepare_and_check(struct fast_task_info *task,
        const int resp_cmd, bool *deal_done)
{
    if (SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_USER &&
            IDEMPOTENCY_CHANNEL != NULL)
    {
        IdempotencyRequest *request;
        int result;

        request = sf_server_update_prepare_and_check(
                &REQUEST, &SERVER_CTX->service.request_allocator,
                IDEMPOTENCY_CHANNEL, &RESPONSE, &result);
        if (request == NULL) {
            *deal_done = true;
            if (result == SF_RETRIABLE_ERROR_CHANNEL_INVALID) {
                TASK_CTX.common.log_level = LOG_DEBUG;
            }
            return result;
        }

        if (result != 0) {
            if (result == EEXIST) { //found
                result = request->output.result;
                if (result == 0) {
                    if ((request->output.flags &
                                TASK_UPDATE_FLAG_OUTPUT_DENTRY))
                    {
                        FDIRDEntryInfo *dentry;
                        dentry = (FDIRDEntryInfo *)request->output.response;
                        dstat_output(task, dentry->inode, &dentry->stat);
                        RESPONSE.header.cmd = resp_cmd;
                    }
                }
            } else {
                TASK_CTX.common.log_level = LOG_WARNING;
            }

            fast_mblock_free_object(request->allocator, request);
            *deal_done = true;
            return result;
        }

        REQUEST.body += sizeof(SFProtoIdempotencyAdditionalHeader);
        REQUEST.header.body_len -= sizeof(SFProtoIdempotencyAdditionalHeader);
        request->output.flags = 0;
        IDEMPOTENCY_REQUEST = request;
    }

    *deal_done = false;
    return 0;
}

static int service_deal_create_dentry(struct fast_task_info *task)
{
    int result;

    if ((result=server_parse_dentry_for_update(task,
                    sizeof(FDIRProtoCreateDEntryFront), true)) != 0)
    {
        return result;
    }

    init_record_for_create(task, buff2int(((FDIRProtoCreateDEntryFront *)
                REQUEST.body)->mode));
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_create_by_pname(struct fast_task_info *task)
{
    int result;

    if ((result=server_parse_pname_for_update(task,
                    sizeof(FDIRProtoCreateDEntryFront))) != 0)
    {
        return result;
    }

    init_record_for_create(task, buff2int(((FDIRProtoCreateDEntryFront *)
                REQUEST.body)->mode));
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_CREATE_BY_PNAME_RESP;
    return push_record_to_data_thread_queue(task);
}

static int parse_symlink_dentry_front(struct fast_task_info *task,
        string_t *link, int *mode)
{
    FDIRProtoSymlinkDEntryFront *front;

    if (REQUEST.header.body_len <= sizeof(FDIRProtoSymlinkDEntryReq)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "request body length: %d is too small",
                REQUEST.header.body_len);
        return EINVAL;
    }

    front = (FDIRProtoSymlinkDEntryFront *)REQUEST.body;
    link->len = buff2short(front->link_len);
    link->str = front->link_str;

    if (link->len <= 0 || link->len >= PATH_MAX) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "link length: %d is invalid", link->len);
        return EINVAL;
    }

    *mode = buff2int(front->common.mode);
    *mode = (*mode & (~S_IFMT)) | S_IFLNK;
    return 0;
}

static int init_record_for_symlink(struct fast_task_info *task,
        const string_t *link, const int mode)
{
    init_record_for_create(task, mode);

    RECORD->link.str = RECORD->me.pname.name.str +
        RECORD->me.pname.name.len;
    if (RECORD->link.str + link->len > task->data + task->size) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "task pkg size: %d is too small", task->size);
        return EOVERFLOW;
    }

    memcpy(RECORD->link.str, link->str, link->len);
    RECORD->link.len = link->len;
    RECORD->options.link = 1;
    return 0;
}

static int service_deal_symlink_dentry(struct fast_task_info *task)
{
    int result;
    int mode;
    string_t link;

    if ((result=parse_symlink_dentry_front(task, &link, &mode)) != 0) {
        return result;
    }

    if ((result=server_parse_dentry_for_update(task,
                    sizeof(FDIRProtoSymlinkDEntryFront) +
                    link.len, false)) != 0)
    {
        return result;
    }

    if ((result=init_record_for_symlink(task, &link, mode)) != 0) {
        free_record_object(task);
        return result;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SYMLINK_DENTRY_RESP;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_symlink_by_pname(struct fast_task_info *task)
{
    int result;
    int mode;
    string_t link;

    if ((result=parse_symlink_dentry_front(task, &link, &mode)) != 0) {
        return result;
    }

    if ((result=server_parse_pname_for_update(task,
                    sizeof(FDIRProtoSymlinkDEntryFront) + link.len)) != 0)
    {
        return result;
    }

    if ((result=init_record_for_symlink(task, &link, mode)) != 0) {
        free_record_object(task);
        return result;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_RESP;
    return push_record_to_data_thread_queue(task);
}

static int do_hdlink_dentry(struct fast_task_info *task,
        FDIRServerDentry *src_dentry, const int mode, const int resp_cmd)
{
    /*
    logInfo("file: "__FILE__", line: %d, "
            "resp_cmd: %d, src inode: %"PRId64", "
            "dest parent: %"PRId64", name: %.*s", __LINE__,
            resp_cmd, src_dentry->inode,
            RECORD->hdlink.dest.pname.parent_inode,
            RECORD->hdlink.dest.pname.name.len,
            RECORD->hdlink.dest.pname.name.str);
            */

    RECORD->hdlink.src_dentry = src_dentry;
    RECORD->hdlink.src_inode = src_dentry->inode;
    init_record_for_create_ex(task, mode, true);
    RECORD->options.src_inode = 1;
    RESPONSE.header.cmd = resp_cmd;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_hdlink_dentry(struct fast_task_info *task)
{
    FDIRDEntryFullName src_fullname;
    FDIRServerDentry *src_dentry;
    int mode;
    int result;

    if ((result=server_check_body_length(
                    sizeof(FDIRProtoHDLinkDEntry) + 4,
                    sizeof(FDIRProtoHDLinkDEntry) + 2 *
                    (NAME_MAX + PATH_MAX))) != 0)
    {
        return result;
    }

    if ((result=server_parse_dentry_info(task, REQUEST.body +
                    sizeof(FDIRProtoCreateDEntryFront),
                    &src_fullname)) != 0)
    {
        return result;
    }

    if ((result=dentry_find(&src_fullname, &src_dentry)) != 0) {
        return result;
    }

    if ((result=server_parse_dentry_for_update(task,
                    sizeof(FDIRProtoCreateDEntryFront) +
                    sizeof(FDIRProtoDEntryInfo) + src_fullname.ns.len +
                    src_fullname.path.len, false)) != 0)
    {
        return result;
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "src ns: %.*s, path: %.*s",
            __LINE__, src_fullname.ns.len, src_fullname.ns.str,
            src_fullname.path.len, src_fullname.path.str);
            */

    if (!fc_string_equal(&RECORD->ns, &src_fullname.ns)) {
        free_record_object(task);
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "src and dest namespace not equal");
        return EINVAL;
    }

    mode = buff2int(((FDIRProtoCreateDEntryFront *)REQUEST.body)->mode);
    return do_hdlink_dentry(task, src_dentry, mode,
            FDIR_SERVICE_PROTO_HDLINK_DENTRY_RESP);
}

static int parse_hdlink_dentry_front(struct fast_task_info *task,
        int64_t *src_inode, int *mode)
{
    FDIRProtoHDlinkByPNameFront *front;

    if (REQUEST.header.body_len <= sizeof(FDIRProtoHDLinkDEntryByPName)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "request body length: %d is too small",
                REQUEST.header.body_len);
        return EINVAL;
    }

    front = (FDIRProtoHDlinkByPNameFront *)REQUEST.body;
    *src_inode = buff2long(front->src_inode);
    *mode = buff2int(front->common.mode);
    return 0;
}

static int service_deal_hdlink_by_pname(struct fast_task_info *task)
{
    FDIRServerDentry *src_dentry;
    int result;
    int mode;
    int64_t src_inode;

    if ((result=parse_hdlink_dentry_front(task, &src_inode, &mode)) != 0) {
        return result;
    }

    if ((src_dentry=inode_index_get_dentry(src_inode)) == NULL) {
        return ENOENT;
    }

    if ((result=server_parse_pname_for_update(task,
                    sizeof(FDIRProtoHDlinkByPNameFront))) != 0)
    {
        return result;
    }

    return do_hdlink_dentry(task, src_dentry, mode,
            FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_RESP);
}

static int service_deal_remove_dentry(struct fast_task_info *task)
{
    int result;

    if ((result=server_parse_dentry_for_update(task, 0, false)) != 0) {
        return result;
    }

    RECORD->operation = BINLOG_OP_REMOVE_DENTRY_INT;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_remove_by_pname(struct fast_task_info *task)
{
    int result;

    if ((result=server_parse_pname_for_update(task, 0)) != 0) {
        return result;
    }

    RECORD->operation = BINLOG_OP_REMOVE_DENTRY_INT;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_RESP;
    return push_record_to_data_thread_queue(task);
}

static int set_rename_src_by_dentry(struct fast_task_info *task,
        FDIRServerDentry *dentry)
{
    FDIRProtoRenameDEntryFront *front;

    if (dentry->parent == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "can't rename root path: /");
        return EINVAL;
    }

    front = (FDIRProtoRenameDEntryFront *)REQUEST.body;
    RECORD->flags = buff2int(front->flags);

    RECORD->rename.src.pname.name.str = RECORD->rename.dest.pname.name.str +
        RECORD->rename.dest.pname.name.len;
    memcpy(RECORD->rename.src.pname.name.str,
            dentry->name.str, dentry->name.len);
    RECORD->rename.src.pname.name.len = dentry->name.len;
    RECORD->rename.src.pname.parent_inode = dentry->parent->inode;
    return 0;
}

static inline int set_rename_src_dentry(struct fast_task_info *task,
        FDIRDEntryFullName *src_fullname)
{
    int result;
    FDIRServerDentry *dentry;

    if ((result=dentry_find(src_fullname, &dentry)) != 0) {
        return result;
    }

    return set_rename_src_by_dentry(task, dentry);
}

static inline int set_rename_src_pname(struct fast_task_info *task,
        FDIRServerDentry *parent, string_t *src_name)
{
    int result;
    FDIRServerDentry *dentry;

    if ((result=dentry_find_by_pname(parent, src_name, &dentry)) != 0) {
        return result;
    }

    return set_rename_src_by_dentry(task, dentry);
}

static int service_deal_rename_dentry(struct fast_task_info *task)
{
    FDIRDEntryFullName src_fullname;
    int result;

    if ((result=server_check_body_length(
                    sizeof(FDIRProtoRenameDEntry) + 4,
                    sizeof(FDIRProtoRenameDEntry) + 2 *
                    (NAME_MAX + PATH_MAX))) != 0)
    {
        return result;
    }

    if ((result=server_parse_dentry_info(task, REQUEST.body +
                    sizeof(FDIRProtoRenameDEntryFront),
                    &src_fullname)) != 0)
    {
        return result;
    }

    if ((result=server_parse_dentry_for_update(task,
                    sizeof(FDIRProtoRenameDEntryFront) +
                    sizeof(FDIRProtoDEntryInfo) + src_fullname.ns.len +
                    src_fullname.path.len, false)) != 0)
    {
        return result;
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "src ns: %.*s, path: %.*s",
            __LINE__, src_fullname.ns.len, src_fullname.ns.str,
            src_fullname.path.len, src_fullname.path.str);
            */

    if ((result=set_rename_src_dentry(task, &src_fullname)) != 0) {
        free_record_object(task);
        return result;
    }

    if (!fc_string_equal(&RECORD->ns, &src_fullname.ns)) {
        free_record_object(task);
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "src and dest namespace not equal");
        return EINVAL;
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "src parent: %"PRId64", name: %.*s, "
            "dest parent: %"PRId64", name: %.*s", __LINE__,
            RECORD->rename.src.pname.parent_inode,
            RECORD->rename.src.pname.name.len,
            RECORD->rename.src.pname.name.str,
            RECORD->rename.dest.pname.parent_inode,
            RECORD->rename.dest.pname.name.len,
            RECORD->rename.dest.pname.name.str);
            */

    RECORD->rename.overwritten = NULL;
    RECORD->operation = BINLOG_OP_RENAME_DENTRY_INT;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_RENAME_DENTRY_RESP;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_rename_by_pname(struct fast_task_info *task)
{
    int result;
    string_t src_ns;
    string_t src_name;
    FDIRServerDentry *src_parent;

    if ((result=server_check_body_length(
                    sizeof(FDIRProtoRenameDEntryByPName) + 4,
                    sizeof(FDIRProtoRenameDEntryByPName) + 2 *
                    (NAME_MAX + PATH_MAX))) != 0)
    {
        return result;
    }

    if ((result=server_parse_pname(task, sizeof(FDIRProtoRenameDEntryFront),
                    &src_ns, &src_name, &src_parent)) != 0)
    {
        return result;
    }

    if ((result=server_parse_pname_for_update(task,
                    sizeof(FDIRProtoRenameDEntryFront) +
                    sizeof(FDIRProtoDEntryByPName) +
                    src_ns.len + src_name.len)) != 0)
    {
        return result;
    }

    if ((result=set_rename_src_pname(task, src_parent, &src_name)) != 0) {
        free_record_object(task);
        return result;
    }

    if (!fc_string_equal(&RECORD->ns, &src_ns)) {
        free_record_object(task);
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "src and dest namespace not equal");
        return EINVAL;
    }

    RECORD->rename.overwritten = NULL;
    RECORD->operation = BINLOG_OP_RENAME_DENTRY_INT;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_RENAME_BY_PNAME_RESP;
    return push_record_to_data_thread_queue(task);
}

static int parse_xattr_fields(struct fast_task_info *task,
        FDIRProtoSetXAttrFields *fields, key_value_pair_t *xattr)
{
    xattr->key.len = fields->name_len;
    xattr->key.str = fields->name_str;
    xattr->value.len = buff2short(fields->value_len);
    xattr->value.str = fields->name_str + xattr->key.len;
    if (xattr->key.len <= 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid xattr name, length: %d <= 0",
                xattr->key.len);
        return EINVAL;
    }
    if (xattr->key.len > NAME_MAX) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "xattr name length: %d is too long, exceeds %d",
                xattr->key.len, NAME_MAX);
        return ENAMETOOLONG;
    }
    if (memchr(xattr->key.str, '\0', xattr->key.len) != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid xattr name, including special char \\0 (0x0)");
        return EINVAL;
    }

    if (xattr->value.len < 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "value length: %d is invalid", xattr->value.len);
        return EINVAL;
    }
    if (xattr->value.len > FDIR_XATTR_MAX_VALUE_SIZE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "value length: %d is too large, exceeds %d",
                xattr->value.len, FDIR_XATTR_MAX_VALUE_SIZE);
        return ENAMETOOLONG;
    }

    return 0;
}

static inline int service_do_setxattr(struct fast_task_info *task,
        const key_value_pair_t *xattr, const int flags,
        const int resp_cmd)
{
    RECORD->options.flags = 0;
    RECORD->flags = flags;
    RECORD->xattr = *xattr;
    RECORD->operation = BINLOG_OP_SET_XATTR_INT;
    RESPONSE.header.cmd = resp_cmd;
    return push_record_to_data_thread_queue(task);
}

static int parse_dentry_for_xattr_update(struct fast_task_info *task,
        const int front_part_size)
{
    FDIRServerDentry *dentry;
    FDIRDEntryFullName fullname;
    int result;

    if ((result=server_check_and_parse_dentry(task,
                    front_part_size, &fullname)) != 0)
    {
        return result;
    }

    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    if ((result=alloc_record_object(task)) != 0) {
        return result;
    }

    RECORD->me.dentry = dentry;
    RECORD->ns = fullname.ns;
    RECORD->inode = dentry->inode;
    RECORD->hash_code = simple_hash(RECORD->ns.str, RECORD->ns.len);
    RECORD->data_version = 0;
    return 0;
}

static int service_deal_set_xattr_by_path(struct fast_task_info *task)
{
    int result;
    int min_body_len;
    int fields_part_len;;
    FDIRProtoSetXAttrFields *fields;
    key_value_pair_t xattr;

    if ((result=server_check_min_body_length(sizeof(
                        FDIRProtoSetXAttrByPathReq) + 3)) != 0)
    {
        return result;
    }

    fields = (FDIRProtoSetXAttrFields *)REQUEST.body;
    if ((result=parse_xattr_fields(task, fields, &xattr)) != 0) {
        return result;
    }

    min_body_len = sizeof(FDIRProtoSetXAttrByPathReq) +
        xattr.key.len + xattr.value.len + 2;
    if (REQUEST.header.body_len < min_body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d is too small which < %d",
                REQUEST.header.body_len, min_body_len);
        return EINVAL;
    }

    fields_part_len = sizeof(FDIRProtoSetXAttrFields) +
        xattr.key.len + xattr.value.len;
    if ((result=parse_dentry_for_xattr_update(task,
                    fields_part_len)) != 0)
    {
        return result;
    }

    return service_do_setxattr(task, &xattr, buff2short(fields->flags),
            FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_RESP);
}

static int service_deal_set_xattr_by_inode(struct fast_task_info *task)
{
    FDIRProtoSetXAttrFields *fields;
    key_value_pair_t xattr;
    int min_body_len;
    int front_part_size;
    int result;

    if ((result=server_check_body_length(
                    sizeof(FDIRProtoSetXAttrByInodeReq) + 2,
                    sizeof(FDIRProtoSetXAttrByInodeReq) + 1 +
                    NAME_MAX + FDIR_XATTR_MAX_VALUE_SIZE)) != 0)
    {
        return result;
    }

    fields = (FDIRProtoSetXAttrFields *)REQUEST.body;
    if ((result=parse_xattr_fields(task, fields, &xattr)) != 0) {
        return result;
    }

    min_body_len = sizeof(FDIRProtoSetXAttrByInodeReq) +
        xattr.key.len + xattr.value.len + 1;
    if (REQUEST.header.body_len < min_body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d is too small which < %d",
                REQUEST.header.body_len, min_body_len);
        return EINVAL;
    }

    front_part_size = sizeof(FDIRProtoSetXAttrFields) +
        xattr.key.len + xattr.value.len;
    if ((result=server_parse_inode_for_update(
                    task, front_part_size)) != 0)
    {
        return result;
    }

    return service_do_setxattr(task, &xattr, buff2short(fields->flags),
            FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_RESP);
}

static int parse_xattr_name_info(struct fast_task_info *task,
        const int fixed_size, const bool check_whole_body_len,
        string_t *name)
{
    int result;
    int expect_blen;
    FDIRProtoNameInfo *proto_name;

    if ((result=server_check_min_body_length( fixed_size + 1)) != 0) {
        return result;
    }

    proto_name = (FDIRProtoNameInfo *)REQUEST.body;
    name->len = proto_name->len;
    name->str = proto_name->str;
    if (check_whole_body_len) {
        expect_blen = fixed_size + name->len;
        if (REQUEST.header.body_len != expect_blen) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "request body length: %d != expect: %d",
                    REQUEST.header.body_len, expect_blen);
            return EINVAL;
        }
    }

    return 0;
}

static inline int service_do_removexattr(struct fast_task_info *task,
        const string_t *name, const int resp_cmd)
{
    RECORD->options.flags = 0;
    RECORD->xattr.key = *name;
    RECORD->operation = BINLOG_OP_REMOVE_XATTR_INT;
    RESPONSE.header.cmd = resp_cmd;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_remove_xattr_by_path(struct fast_task_info *task)
{
    int result;
    int min_body_len;
    int fields_part_len;;
    int fixed_size;
    string_t name;

    if ((result=server_check_min_body_length(sizeof(
                        FDIRProtoRemoveXAttrByPathReq) + 3)) != 0)
    {
        return result;
    }

    fixed_size = sizeof(FDIRProtoRemoveXAttrByPathReq) + 1;
    if ((result=parse_xattr_name_info(task, fixed_size,
                    false, &name)) != 0)
    {
        return result;
    }

    min_body_len = sizeof(FDIRProtoRemoveXAttrByPathReq) + name.len + 2;
    if (REQUEST.header.body_len < min_body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d is too small which < %d",
                REQUEST.header.body_len, min_body_len);
        return EINVAL;
    }

    fields_part_len = sizeof(FDIRProtoNameInfo) + name.len;
    if ((result=parse_dentry_for_xattr_update(task,
                    fields_part_len)) != 0)
    {
        return result;
    }

    return service_do_removexattr(task, &name,
            FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_RESP);
}

static int service_deal_remove_xattr_by_inode(struct fast_task_info *task)
{
    string_t name;
    int min_body_len;
    int front_part_size;
    int fixed_size;
    int result;

    if ((result=server_check_body_length(
                    sizeof(FDIRProtoRemoveXAttrByInodeReq) + 2,
                    sizeof(FDIRProtoRemoveXAttrByInodeReq) + 1 +
                    NAME_MAX)) != 0)
    {
        return result;
    }

    fixed_size = sizeof(FDIRProtoRemoveXAttrByInodeReq) + 1;
    if ((result=parse_xattr_name_info(task, fixed_size,
                    false, &name)) != 0)
    {
        return result;
    }

    min_body_len = sizeof(FDIRProtoRemoveXAttrByInodeReq) + name.len + 1;
    if (REQUEST.header.body_len < min_body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d is too small which < %d",
                REQUEST.header.body_len, min_body_len);
        return EINVAL;
    }

    front_part_size = sizeof(FDIRProtoNameInfo) + name.len;
    if ((result=server_parse_inode_for_update(
                    task, front_part_size)) != 0)
    {
        return result;
    }

    return service_do_removexattr(task, &name,
            FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_RESP);
}

static int service_deal_stat_dentry_by_path(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;
    FDIRServerDentry *dentry;

    if ((result=server_check_and_parse_dentry(task, 0, &fullname)) != 0) {
        return result;
    }

    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP;
    dentry_stat_output(task, &dentry);
    return 0;
}

static int readlink_output(struct fast_task_info *task,
        FDIRServerDentry *dentry, const int resp_cmd)
{
    if (!S_ISLNK(dentry->stat.mode)) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "not symbol link");
        return ENOLINK;
    }

    RESPONSE.header.cmd = resp_cmd;
    RESPONSE.header.body_len = dentry->link.len;
    memcpy(SF_PROTO_RESP_BODY(task), dentry->link.str, dentry->link.len);
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_readlink_by_path(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;
    FDIRServerDentry *dentry;

    if ((result=server_check_and_parse_dentry(task, 0, &fullname)) != 0) {
        return result;
    }

    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    return readlink_output(task, dentry,
            FDIR_SERVICE_PROTO_READLINK_BY_PATH_RESP);
}

static int service_deal_readlink_by_pname(struct fast_task_info *task)
{
    FDIRProtoReadlinkByPNameReq *req;
    FDIRServerDentry *dentry;
    int64_t parent_inode;
    string_t name;
    int result;

    if ((result=server_check_body_length(sizeof(
                        FDIRProtoReadlinkByPNameReq) + 1,
                    sizeof(FDIRProtoReadlinkByPNameReq) + NAME_MAX)) != 0)
    {
        return result;
    }

    req = (FDIRProtoReadlinkByPNameReq *)REQUEST.body;
    if (sizeof(FDIRProtoReadlinkByPNameReq) + req->name_len !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, (int)sizeof(
                    FDIRProtoReadlinkByPNameReq) + req->name_len);
        return EINVAL;
    }

    parent_inode = buff2long(req->parent_inode);
    name.str = req->name_str;
    name.len = req->name_len;
    if ((dentry=inode_index_get_dentry_by_pname(
                    parent_inode, &name)) == NULL)
    {
        return ENOENT;
    }

    return readlink_output(task, dentry,
            FDIR_SERVICE_PROTO_READLINK_BY_PNAME_RESP);
}

static inline int server_check_and_parse_inode(
        struct fast_task_info *task, int64_t *inode)
{
    int result;

    if ((result=server_expect_body_length(8)) != 0) {
        return result;
    }

    *inode = buff2long(REQUEST.body);
    return 0;
}

static int service_deal_readlink_by_inode(struct fast_task_info *task)
{
    FDIRServerDentry *dentry;
    int64_t inode;
    int result;

    if ((result=server_check_and_parse_inode(task, &inode)) != 0) {
        return result;
    }

    if ((dentry=inode_index_get_dentry(inode)) == NULL) {
        return ENOENT;
    }

    return readlink_output(task, dentry,
            FDIR_SERVICE_PROTO_READLINK_BY_INODE_RESP);
}

static int service_deal_lookup_inode_by_path(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;
    FDIRServerDentry *dentry;
    FDIRProtoLookupInodeResp *resp;


    if ((result=server_check_and_parse_dentry(task, 0, &fullname)) != 0) {
        return result;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_RESP;
    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    resp = (FDIRProtoLookupInodeResp *)SF_PROTO_RESP_BODY(task);
    long2buff(dentry->inode, resp->inode);
    RESPONSE.header.body_len = sizeof(FDIRProtoLookupInodeResp);
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_stat_dentry_by_inode(struct fast_task_info *task)
{
    FDIRServerDentry *dentry;
    int64_t inode;
    int result;

    if ((result=server_check_and_parse_inode(task, &inode)) != 0) {
        return result;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_STAT_BY_INODE_RESP;
    if ((dentry=inode_index_get_dentry(inode)) == NULL) {
        return ENOENT;
    }

    dentry_stat_output(task, &dentry);
    return 0;
}

static int get_dentry_by_pname(struct fast_task_info *task,
        FDIRServerDentry **dentry)
{
    FDIRProtoStatDEntryByPNameReq *req;
    int64_t parent_inode;
    string_t name;
    int result;

    if ((result=server_check_body_length(sizeof(
                        FDIRProtoStatDEntryByPNameReq) + 1,
                    sizeof(FDIRProtoStatDEntryByPNameReq) + NAME_MAX)) != 0)
    {
        return result;
    }

    req = (FDIRProtoStatDEntryByPNameReq *)REQUEST.body;
    if (sizeof(FDIRProtoStatDEntryByPNameReq) + req->name_len !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, (int)sizeof(
                    FDIRProtoStatDEntryByPNameReq) + req->name_len);
        return EINVAL;
    }

    parent_inode = buff2long(req->parent_inode);
    name.str = req->name_str;
    name.len = req->name_len;
    if ((*dentry=inode_index_get_dentry_by_pname(
                    parent_inode, &name)) == NULL)
    {
        return ENOENT;
    }

    return 0;
}

static int service_deal_stat_dentry_by_pname(struct fast_task_info *task)
{
    int result;
    FDIRServerDentry *dentry;

    if ((result=get_dentry_by_pname(task, &dentry)) == 0) {
        RESPONSE.header.cmd = FDIR_SERVICE_PROTO_STAT_BY_PNAME_RESP;
        dentry_stat_output(task, &dentry);
    }
    return result;
}

static int service_deal_lookup_inode_by_pname(struct fast_task_info *task)
{
    int result;
    FDIRServerDentry *dentry;
    FDIRProtoLookupInodeResp *resp;

    if ((result=get_dentry_by_pname(task, &dentry)) == 0) {
        RESPONSE.header.cmd = FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_RESP;
        resp = (FDIRProtoLookupInodeResp *)SF_PROTO_RESP_BODY(task);
        long2buff(dentry->inode, resp->inode);
        RESPONSE.header.body_len = sizeof(FDIRProtoLookupInodeResp);
        TASK_CTX.common.response_done = true;
    }
    return result;
}

static inline int binlog_produce_directly(struct fast_task_info *task)
{
    RECORD->data_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 1);
    sf_hold_task(task);
    return server_binlog_produce(task);
}

static FDIRServerDentry *do_set_dentry_size(FDIRBinlogRecord *record,
        const char *ns_str, const int ns_len,
        const FDIRSetDEntrySizeInfo *dsize, const bool need_lock,
        int *result, int *modified_flags)
{
    FDIRServerDentry *dentry;

    if ((dentry=inode_index_check_set_dentry_size(dsize,
                    need_lock, modified_flags)) == NULL)
    {
        *result = ENOENT;
        return NULL;
    }

    if (*modified_flags == 0) {  //no fields changed
        *result = 0;
        return dentry;
    }

    record->inode = dsize->inode;
    record->me.dentry = dentry;
    record->hash_code = simple_hash(ns_str, ns_len);
    record->options.flags = 0;
    if ((*modified_flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE)) {
        record->options.size = 1;
        record->stat.size = record->me.dentry->stat.size;
    }
    if ((*modified_flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_SPACE_END)) {
        record->options.space_end = 1;
        record->stat.space_end = record->me.dentry->stat.space_end;
    }
    if ((*modified_flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC)) {
        record->options.inc_alloc = 1;
        record->stat.alloc = dsize->inc_alloc;
    }
    if ((*modified_flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME)) {
        record->options.mtime = 1;
        record->stat.mtime = record->me.dentry->stat.mtime;
    }
    record->operation = BINLOG_OP_UPDATE_DENTRY_INT;
    *result = 0;
    return dentry;
}

static FDIRServerDentry *set_dentry_size(
        struct fast_task_info *task,
        const char *ns_str, const int ns_len,
        const FDIRSetDEntrySizeInfo *dsize,
        const bool need_lock, int *result)
{
    int modified_flags;
    FDIRServerDentry *dentry;

    if ((*result=alloc_record_object(task)) != 0) {
        return NULL;
    }

    dentry = do_set_dentry_size(RECORD, ns_str, ns_len,
            dsize, need_lock, result, &modified_flags);
    if (dentry == NULL || modified_flags == 0) {
        free_record_object(task);
        return dentry;
    }

    *result = binlog_produce_directly(task);
    return dentry;
}

#define SERVICE_UNPACK_DENTRY_SIZE_INFO(dsize, req) \
    dsize.inode = buff2long(req->inode); \
    dsize.file_size = buff2long(req->file_size); \
    dsize.inc_alloc = buff2long(req->inc_alloc); \
    dsize.flags = buff2int(req->flags);  \
    dsize.force = req->force

static int service_deal_set_dentry_size(struct fast_task_info *task)
{
    FDIRProtoSetDentrySizeReq *req;
    FDIRServerDentry *dentry;
    FDIRSetDEntrySizeInfo dsize;
    int result;

    if ((result=server_check_body_length(
                    sizeof(FDIRProtoSetDentrySizeReq) + 1,
                    sizeof(FDIRProtoSetDentrySizeReq) + NAME_MAX)) != 0)
    {
        return result;
    }

    req = (FDIRProtoSetDentrySizeReq *)REQUEST.body;
    if (req->ns_len <= 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "namespace length: %d is invalid which <= 0",
                req->ns_len);
        return EINVAL;
    }
    if (sizeof(FDIRProtoSetDentrySizeReq) + req->ns_len !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, (int)sizeof(
                    FDIRProtoSetDentrySizeReq) + req->ns_len);
        return EINVAL;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_RESP;
    SERVICE_UNPACK_DENTRY_SIZE_INFO(dsize, req);

    dentry = set_dentry_size(task, req->ns_str, req->ns_len,
            &dsize, true, &result);
    if (result == 0 || result == TASK_STATUS_CONTINUE) {
        if (dentry != NULL) {
            set_update_result_and_output(task, dentry);
        }
    }

    return result;
}

static int service_deal_batch_set_dentry_size(struct fast_task_info *task)
{
    FDIRProtoBatchSetDentrySizeReqHeader *rheader;
    FDIRProtoBatchSetDentrySizeReqBody *rbody;
    FDIRProtoBatchSetDentrySizeReqBody *rbend;
    ServerBinlogRecordBuffer *rbuffer;
    FDIRServerDentry *dentry;
    FDIRSetDEntrySizeInfo dsize;
    FDIRBinlogRecord *records[FDIR_BATCH_SET_MAX_DENTRY_COUNT];
    FDIRBinlogRecord **record;
    FDIRBinlogRecord **recend;
    uint64_t current_version;
    int record_count;
    int result;
    int count;
    int expect_blen;
    int modified_flags;

    if ((result=server_check_min_body_length(
                    sizeof(FDIRProtoBatchSetDentrySizeReqHeader) + 1 +
                    sizeof(FDIRProtoBatchSetDentrySizeReqBody))) != 0)
    {
        return result;
    }

    rheader = (FDIRProtoBatchSetDentrySizeReqHeader *)REQUEST.body;
    if (rheader->ns_len <= 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "namespace length: %d is invalid which <= 0",
                rheader->ns_len);
        return EINVAL;
    }
    count = buff2int(rheader->count);
    if (count <= 0 || count > FDIR_BATCH_SET_MAX_DENTRY_COUNT) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "count: %d is invalid which <= 0 or > %d",
                count, FDIR_BATCH_SET_MAX_DENTRY_COUNT);
        return EINVAL;
    }

    expect_blen = sizeof(FDIRProtoBatchSetDentrySizeReqHeader) +
        rheader->ns_len + sizeof(FDIRProtoBatchSetDentrySizeReqBody) * count;
    if (REQUEST.header.body_len != expect_blen) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, expect_blen);
        return EINVAL;
    }

    if ((rbuffer=server_binlog_alloc_hold_rbuffer()) == NULL) {
        return ENOMEM;
    }
    memset(records, 0, sizeof(FDIRBinlogRecord *) * count);
    record = records;

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_RESP;
    rbody = (FDIRProtoBatchSetDentrySizeReqBody *)
        (rheader->ns_str + rheader->ns_len);
    rbend = rbody + count;
    for (; rbody < rbend; rbody++) {
        SERVICE_UNPACK_DENTRY_SIZE_INFO(dsize, rbody);

        if (*record == NULL) {
            *record = (FDIRBinlogRecord *)fast_mblock_alloc_object(
                    &((FDIRServerContext *)task->thread_data->arg)->
                    service.record_allocator);
            if (*record == NULL) {
                RESPONSE.error.length = sprintf(
                        RESPONSE.error.message,
                        "system busy, please try later");
                return EBUSY;
            }
        }

        dentry = do_set_dentry_size(*record, rheader->ns_str,
                rheader->ns_len, &dsize, true, &result, &modified_flags);
        if (dentry != NULL && modified_flags != 0) {
            record++;
        }
    }

    recend = record;
    record_count = recend - records;
    rbuffer->data_version.last = __sync_add_and_fetch(
                &DATA_CURRENT_VERSION, record_count);
    rbuffer->data_version.first = rbuffer->
        data_version.last - record_count + 1;
    current_version = rbuffer->data_version.first;
    for (record=records; record<recend; record++) {
        (*record)->data_version = current_version++;
        (*record)->timestamp = g_current_time;
        if ((result=binlog_pack_record(*record, &rbuffer->buffer)) != 0) {
            break;
        }
    }

    for (record=records; record<recend; record++) {
        fast_mblock_free_object(&((FDIRServerContext *)task->
                    thread_data->arg)->service.record_allocator, *record);
    }
    if (record_count < count && *recend != NULL) {
        fast_mblock_free_object(&((FDIRServerContext *)task->
                    thread_data->arg)->service.record_allocator, *recend);
    }

    /*
    logInfo("result: %d, count: %d, record_count: %d, "
            "first data_version: %"PRId64", last data_version: %"PRId64
            ", buffer length: %d", result, count, record_count,
            rbuffer->data_version.first, rbuffer->data_version.last,
            rbuffer->buffer.length);
            */

    if (result == 0 && record_count > 0) {
        sf_hold_task(task);
        return do_binlog_produce(task, rbuffer);
    } else {
        server_binlog_free_rbuffer(rbuffer);
        return result;
    }
}

static FDIRServerDentry *modify_dentry_stat(struct fast_task_info *task,
        const char *ns_str, const int ns_len, const int64_t inode,
        const int64_t flags, const FDIRDEntryStat *stat, int *result)
{
    FDIRServerDentry *dentry;

    if ((*result=alloc_record_object(task)) != 0) {
        return NULL;
    }

    RECORD->inode = inode;
    RECORD->options.flags = flags;
    RECORD->stat = *stat;
    RECORD->hash_code = simple_hash(ns_str, ns_len);
    RECORD->operation = BINLOG_OP_UPDATE_DENTRY_INT;

    if ((dentry=inode_index_update_dentry(RECORD)) == NULL) {
        free_record_object(task);
        *result = ENOENT;
        return NULL;
    }

    RECORD->me.dentry = dentry;
    *result = binlog_produce_directly(task);
    return dentry;
}

static int service_deal_modify_dentry_stat(struct fast_task_info *task)
{
    FDIRProtoModifyDentryStatReq *req;
    FDIRServerDentry *dentry;
    FDIRDEntryStat stat;
    int64_t inode;
    int64_t flags;
    int64_t masked_flags;
    int result;

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_RESP;
    if ((result=server_check_body_length(
                    sizeof(FDIRProtoModifyDentryStatReq) + 1,
                    sizeof(FDIRProtoModifyDentryStatReq) + NAME_MAX)) != 0)
    {
        return result;
    }

    req = (FDIRProtoModifyDentryStatReq *)REQUEST.body;
    if (req->ns_len <= 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "namespace length: %d is invalid which <= 0",
                req->ns_len);
        return EINVAL;
    }
    if (sizeof(FDIRProtoModifyDentryStatReq) + req->ns_len !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, (int)sizeof(
                    FDIRProtoModifyDentryStatReq) + req->ns_len);
        return EINVAL;
    }

    inode = buff2long(req->inode);
    flags = buff2long(req->mflags);
    masked_flags = (flags & dstat_mflags_mask);

    if (masked_flags == 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid flags: %"PRId64, flags);
        return EINVAL;
    }

    fdir_proto_unpack_dentry_stat(&req->stat, &stat);
    dentry = modify_dentry_stat(task, req->ns_str, req->ns_len,
            inode, masked_flags, &stat, &result);

    /*
    logInfo("file: "__FILE__", line: %d, "
            "flags: %"PRId64" (0x%llX), masked_flags: %"PRId64", result: %d",
            __LINE__, flags, flags, masked_flags, result);
            */

    if (result == 0 || result == TASK_STATUS_CONTINUE) {
        if (dentry != NULL) {
            set_update_result_and_output(task, dentry);
        }
    }

    return result;
}

static inline int service_check_readable(struct fast_task_info *task)
{
    if (__sync_fetch_and_add(&CLUSTER_MYSELF_PTR->status, 0) !=
                FDIR_SERVER_STATUS_ACTIVE)
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not active");
        return SF_RETRIABLE_ERROR_NOT_ACTIVE;
    }

    return 0;
}

static int service_process_update(struct fast_task_info *task,
        deal_task_func real_update_func, const int resp_cmd)
{
    int result;
    bool deal_done;

    if ((result=service_check_master(task)) != 0) {
        return result;
    }

    result = service_update_prepare_and_check(task, resp_cmd, &deal_done);
    if (result != 0 || deal_done) {
        return result;
    }

    if ((result=real_update_func(task)) != TASK_STATUS_CONTINUE) {
        service_idempotency_request_finish(task, result);
    }

    return result;
}

static int compare_flock_task(FLockTask *flck, const FlockOwner *owner,
        const int64_t inode, const int64_t offset, const int64_t length)
{
    int sub;
    if ((sub=fc_compare_int64(flck->owner.id, owner->id)) != 0) {
        return sub;
    }

    if ((sub=fc_compare_int64(flck->dentry->inode, inode)) != 0) {
        return sub;
    }

    if ((sub=fc_compare_int64(flck->region->offset, offset)) != 0) {
        return sub;
    }

    if ((sub=fc_compare_int64(flck->region->length, length)) != 0) {
        return sub;
    }

    return 0;
}

static int flock_unlock_dentry(struct fast_task_info *task,
        const FlockOwner *owner, const int64_t inode, const int64_t offset,
        const int64_t length)
{
    FLockTask *flck;
    fc_list_for_each_entry(flck, FTASK_HEAD_PTR, clink) {

        /*
        logInfo("==type: %d, which_queue: %d, inode: %"PRId64", "
                "offset: %"PRId64", length: %"PRId64", "
                "owner.id: %"PRId64", owner.pid: %d",
                flck->type, flck->which_queue, flck->dentry->inode,
                flck->region->offset, flck->region->length,
                flck->owner.id, flck->owner.pid);
                */

        if (flck->which_queue != FDIR_FLOCK_TASK_IN_LOCKED_QUEUE) {
            continue;
        }

        if (compare_flock_task(flck, owner, inode, offset, length) == 0) {
            release_flock_task(task, flck);
            return 0;
        }
    }

    return ENOENT;
}

static int service_deal_flock_dentry(struct fast_task_info *task)
{
    FDIRProtoFlockDEntryReq *req;
    FLockTask *ftask;
    int result;
    short type;
    FlockOwner owner;
    int64_t inode;
    int64_t offset;
    int64_t length;
    short operation;

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_FLOCK_DENTRY_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoFlockDEntryReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoFlockDEntryReq *)REQUEST.body;
    inode = buff2long(req->inode);
    offset = buff2long(req->offset);
    length = buff2long(req->length);
    owner.id = buff2long(req->owner.id);
    owner.pid = buff2int(req->owner.pid);
    operation = buff2int(req->operation);

    /*
    logInfo("file: "__FILE__", line: %d, "
            "sock: %d, operation: %d, inode: %"PRId64", "
            "offset: %"PRId64", length: %"PRId64", "
            "owner.id: %"PRId64", owner.pid: %d", __LINE__,
            task->event.fd, operation, inode,
            offset, length, owner.id, owner.pid);
            */

    if (operation & LOCK_UN) {
        return flock_unlock_dentry(task, &owner, inode, offset, length);
    }

    if (operation & LOCK_EX) {
        type = LOCK_EX;
    } else if (operation & LOCK_SH) {
        type = LOCK_SH;
    } else {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid operation: %d", operation);
        return EINVAL;
    }

    if ((ftask=inode_index_flock_apply(inode, type, offset, length,
                    (operation & LOCK_NB) == 0, &owner, task,
                    &result)) == NULL)
    {
        if (result == EDEADLK) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "deadlock occur, inode: %"PRId64", operation: %d",
                    inode, operation);
        }
        return result;
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "===operation: %d, inode: %"PRId64", offset: %"PRId64", "
            "length: %"PRId64", owner.id: %"PRId64", owner.pid: %d, "
            "result: %d, task: %p, deal_func: %p", __LINE__, operation,
            inode, offset, length, owner.id, owner.pid,
            result, task, task->continue_callback);
            */

    fc_list_add_tail(&ftask->clink, FTASK_HEAD_PTR);
    return result == 0 ? 0 : TASK_STATUS_CONTINUE;
}

static int service_deal_getlk_dentry(struct fast_task_info *task)
{
    FDIRProtoGetlkDEntryReq *req;
    FDIRProtoGetlkDEntryResp *resp;
    FLockTask ftask;
    int64_t inode;
    int64_t offset;
    int64_t length;
    int pid;
    short operation;
    int result;
    FLockRegion region;

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GETLK_DENTRY_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoGetlkDEntryReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoGetlkDEntryReq *)REQUEST.body;
    inode = buff2long(req->inode);
    offset = buff2long(req->offset);
    length = buff2long(req->length);
    operation = buff2int(req->operation);
    pid = buff2int(req->pid);

    /*
    logInfo("file: "__FILE__", line: %d, "
            "operation: %d, inode: %"PRId64", "
            "offset: %"PRId64", length: %"PRId64, 
            __LINE__, operation, inode, offset, length);
            */

    if (operation & LOCK_EX) {
        ftask.type = LOCK_EX;
    } else if (operation & LOCK_SH) {
        ftask.type = LOCK_SH;
    } else {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid operation: %d", operation);
        return EINVAL;
    }

    memset(&region, 0, sizeof(region));
    region.offset = offset;
    region.length = length;
    ftask.region = &region;  //for region compare
    result = inode_index_flock_getlk(inode, &ftask);
    if (result == 0 || result == ENOENT) {
        resp = (FDIRProtoGetlkDEntryResp *)SF_PROTO_RESP_BODY(task);
        if (result == 0) {
            int2buff(ftask.type, resp->type);
            long2buff(ftask.region->offset, resp->offset);
            long2buff(ftask.region->length, resp->length);
            long2buff(ftask.owner.id, resp->owner.id);
            int2buff(ftask.owner.pid, resp->owner.pid);
        } else {
            int2buff(LOCK_UN, resp->type);
            long2buff(offset, resp->offset);
            long2buff(length, resp->length);
            int2buff(pid, resp->owner.pid);
            long2buff(0, resp->owner.id);

            result = 0;
        }

        RESPONSE.header.body_len = sizeof(FDIRProtoGetlkDEntryResp);
        TASK_CTX.common.response_done = true;
    }

    return result;
}

static void sys_lock_dentry_output(struct fast_task_info *task,
        const FDIRServerDentry *dentry)
{
    FDIRProtoSysLockDEntryResp *resp;
    resp = (FDIRProtoSysLockDEntryResp *)SF_PROTO_RESP_BODY(task);

    long2buff(dentry->stat.size, resp->size);
    long2buff(dentry->stat.space_end, resp->space_end);
    RESPONSE.header.body_len = sizeof(FDIRProtoSysLockDEntryResp);
    TASK_CTX.common.response_done = true;
}

static int handle_sys_lock_done(struct fast_task_info *task)
{
    struct sys_lock_task *sys_lock_task;

    if (__sync_add_and_fetch(&task->canceled, 0)) {
        logWarning("file: "__FILE__", line: %d, "
                "task: %p, already canceled!",
                __LINE__, task);
        return ECANCELED;
    }

    sys_lock_task = SYS_LOCK_TASK;
    if (sys_lock_task == NULL) {
        logWarning("file: "__FILE__", line: %d, "
                "task: %p, SYS_LOCK_TASK is NULL!",
                __LINE__, task);
        return ENOENT;
    } else {
        /*
           logInfo("file: "__FILE__", line: %d, func: %s, "
           "inode: %"PRId64", file size: %"PRId64,
           __LINE__, __FUNCTION__,
           sys_lock_task->dentry->inode,
           sys_lock_task->dentry->stat.size);
         */

        sys_lock_dentry_output(task, sys_lock_task->dentry);
        return 0;
    }
}

static int service_deal_sys_lock_dentry(struct fast_task_info *task)
{
    FDIRProtoSysLockDEntryReq *req;
    int result;
    int flags;
    int64_t inode;

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoSysLockDEntryReq))) != 0)
    {
        return result;
    }

    if (SYS_LOCK_TASK != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "sys lock already exist, locked inode: %"PRId64,
                SYS_LOCK_TASK->dentry->inode);
        return EEXIST;
    }

    req = (FDIRProtoSysLockDEntryReq *)REQUEST.body;
    inode = buff2long(req->inode);
    flags = buff2int(req->flags);

    if ((SYS_LOCK_TASK=inode_index_sys_lock_apply(inode, (flags & LOCK_NB) == 0,
                    task, &result)) == NULL)
    {
        if (result == EDEADLK) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "deadlock occur, inode: %"PRId64, inode);
        }
        return result;
    }

    if (result == 0) {
        /*
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "locked for inode: %"PRId64", task: %p, sock: %d",
                __LINE__, __FUNCTION__, SYS_LOCK_TASK->dentry->inode,
                task, task->event.fd);
                */

        sys_lock_dentry_output(task, SYS_LOCK_TASK->dentry);
        return 0;
    } else {
        /*
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "waiting lock for inode: %"PRId64", task: %p, "
                "sock: %d", __LINE__, __FUNCTION__,
                SYS_LOCK_TASK->dentry->inode, task, task->event.fd);
                */

        task->continue_callback = handle_sys_lock_done;
        return TASK_STATUS_CONTINUE;
    }
}

static void on_sys_lock_release(FDIRServerDentry *dentry, void *args)
{
    struct fast_task_info *task;
    FDIRProtoSysUnlockDEntryReq *req;
    FDIRSetDEntrySizeInfo dsize;
    int result;

    task = (struct fast_task_info *)args;
    req = (FDIRProtoSysUnlockDEntryReq *)REQUEST.body;
    dsize.inode = SYS_LOCK_TASK->dentry->inode;
    dsize.file_size = buff2long(req->new_size);
    dsize.inc_alloc = buff2long(req->inc_alloc);
    dsize.flags = buff2int(req->flags);
    dsize.force = req->force;
    set_dentry_size(task, req->ns_str, req->ns_len,
            &dsize, false, &result);

    RESPONSE_STATUS = result;
}

static int service_deal_sys_unlock_dentry(struct fast_task_info *task)
{
    FDIRProtoSysUnlockDEntryReq *req;
    int result;
    int flags;
    int64_t inode;
    int64_t old_size;
    int64_t new_size;
    sys_lock_release_callback callback;

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_RESP;
    if ((result=server_check_body_length(
                    sizeof(FDIRProtoSysUnlockDEntryReq),
                    sizeof(FDIRProtoSysUnlockDEntryReq) + NAME_MAX)) != 0)
    {
        return result;
    }

    req = (FDIRProtoSysUnlockDEntryReq *)REQUEST.body;
    if (sizeof(FDIRProtoSysUnlockDEntryReq) + req->ns_len !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d",
                REQUEST.header.body_len, (int)sizeof(
                    FDIRProtoSysUnlockDEntryReq) + req->ns_len);
        return EINVAL;
    }

    if (SYS_LOCK_TASK == NULL) {
        logError("file: "__FILE__", line: %d, func: %s, "
                "task: %p, sock: %d", __LINE__, __FUNCTION__,
                task, task->event.fd);

        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "sys lock not exist");
        return ENOENT;
    }

    inode = buff2long(req->inode);
    if (inode != SYS_LOCK_TASK->dentry->inode) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "sys lock check fail, req inode: %"PRId64", "
                "expect: %"PRId64, inode, SYS_LOCK_TASK->dentry->inode);
        return EINVAL;
    }
    flags = buff2int(req->flags);

    if ((flags & (FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE |
                    FDIR_DENTRY_FIELD_MODIFIED_FLAG_SPACE_END |
                    FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC)))
    {
        if (req->ns_len <= 0) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "namespace length: %d is invalid which <= 0",
                    req->ns_len);
            return ENOENT;
        }

        old_size = buff2long(req->old_size);
        new_size = buff2long(req->new_size);
        if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE) &&
                old_size != SYS_LOCK_TASK->dentry->stat.size)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "client ip: %s, inode: %"PRId64", old size: %"PRId64
                    ", != current size: %"PRId64", maybe changed by others",
                    __LINE__, task->client_ip, inode, old_size,
                    SYS_LOCK_TASK->dentry->stat.size);
        }
        if (new_size < 0) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "invalid new file size: %"PRId64" which < 0", new_size);
            return EINVAL;
        }
        callback = on_sys_lock_release;
    } else {
        callback = NULL;
    }

    if ((result=inode_index_sys_lock_release_ex(
                    SYS_LOCK_TASK, callback, task)) != 0)
    {
        return result;
    }

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "task: %p, callback: %p, status: %d, nio stage: %d, fd: %d",
            __LINE__, __FUNCTION__, task, callback, RESPONSE_STATUS,
            SF_NIO_TASK_STAGE_FETCH(task), task->event.fd);
            */

    SYS_LOCK_TASK = NULL;
    if (RESPONSE_STATUS == TASK_STATUS_CONTINUE) { //status set by the callback
        RESPONSE_STATUS = 0;
        return TASK_STATUS_CONTINUE;
    } else {
        return RESPONSE_STATUS;
    }
}

static int server_list_dentry_output(struct fast_task_info *task)
{
    FDIRProtoListDEntryRespBodyHeader *body_header;
    FDIRServerDentry *src_dentry;
    FDIRServerDentry **dentry;
    FDIRServerDentry **start;
    FDIRServerDentry **end;
    FDIRProtoListDEntryRespBodyPart *body_part;
    char *p;
    char *buf_end;
    int remain_count;
    int count;

    remain_count = DENTRY_LIST_CACHE.array.count -
        DENTRY_LIST_CACHE.offset;

    buf_end = task->data + task->size;
    p = SF_PROTO_RESP_BODY(task) + sizeof(FDIRProtoListDEntryRespBodyHeader);
    start = DENTRY_LIST_CACHE.array.entries +
        DENTRY_LIST_CACHE.offset;
    end = start + remain_count;
    for (dentry=start; dentry<end; dentry++) {
        src_dentry = FDIR_GET_REAL_DENTRY(*dentry);
        if (buf_end - p < sizeof(FDIRProtoListDEntryRespBodyPart) +
                (*dentry)->name.len)
        {
            break;
        }
        body_part = (FDIRProtoListDEntryRespBodyPart *)p;
        long2buff(src_dentry->inode, body_part->inode);

        fdir_proto_pack_dentry_stat_ex(&src_dentry->stat,
                &body_part->stat, true);
        body_part->name_len = (*dentry)->name.len;
        memcpy(body_part->name_str, (*dentry)->name.str, (*dentry)->name.len);
        p += sizeof(FDIRProtoListDEntryRespBodyPart) + (*dentry)->name.len;
    }
    count = dentry - start;
    RESPONSE.header.body_len = p - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_LIST_DENTRY_RESP;

    body_header = (FDIRProtoListDEntryRespBodyHeader *)SF_PROTO_RESP_BODY(task);
    int2buff(count, body_header->count);
    if (count < remain_count) {
        DENTRY_LIST_CACHE.offset += count;
        DENTRY_LIST_CACHE.expires = g_current_time + 60;
        DENTRY_LIST_CACHE.token = __sync_add_and_fetch(&next_token, 1);

        body_header->is_last = 0;
        long2buff(DENTRY_LIST_CACHE.token, body_header->token);
    } else {
        body_header->is_last = 1;
        long2buff(0, body_header->token);
    }

    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_list_dentry_by_path(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;

    if ((result=server_check_and_parse_dentry(task, 0, &fullname)) != 0) {
        return result;
    }

    if ((result=dentry_list_by_path(&fullname,
                    &DENTRY_LIST_CACHE.array)) != 0)
    {
        return result;
    }

    DENTRY_LIST_CACHE.offset = 0;
    return server_list_dentry_output(task);
}

static int service_deal_list_dentry_by_inode(struct fast_task_info *task)
{
    FDIRServerDentry *dentry;
    int64_t inode;
    int result;

    if ((result=server_check_and_parse_inode(task, &inode)) != 0) {
        return result;
    }

    if ((dentry=inode_index_get_dentry(inode)) == NULL) {
        return ENOENT;
    }

    if ((result=dentry_list(dentry, &DENTRY_LIST_CACHE.array)) != 0) {
        return result;
    }

    DENTRY_LIST_CACHE.offset = 0;
    return server_list_dentry_output(task);
}

static int service_deal_list_dentry_next(struct fast_task_info *task)
{
    FDIRProtoListDEntryNextBody *next_body;
    int result;
    int offset;
    int64_t token;

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoListDEntryNextBody))) != 0)
    {
        return result;
    }

    if (DENTRY_LIST_CACHE.expires < g_current_time) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "dentry list cache expires, please try again");
        return ETIMEDOUT;
    }

    next_body = (FDIRProtoListDEntryNextBody *)REQUEST.body;
    token = buff2long(next_body->token);
    offset = buff2int(next_body->offset);
    if (token != DENTRY_LIST_CACHE.token) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid token for next list");
        return EINVAL;
    }
    if (offset != DENTRY_LIST_CACHE.offset) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "next list offset: %d != expected: %d",
                offset, DENTRY_LIST_CACHE.offset);
        return EINVAL;
    }
    return server_list_dentry_output(task);
}

static int service_do_getxattr(struct fast_task_info *task,
        FDIRServerDentry *dentry, const string_t *name,
        const int resp_cmd)
{
    int result;
    string_t value;

    if ((result=inode_index_get_xattr(dentry, name, &value)) != 0) {
        /*
           RESPONSE.error.length = sprintf(RESPONSE.error.message,
           "inode: %"PRId64", get xattr %.*s fail, %s",
           dentry->inode, name->len, name->str, STRERROR(result));
         */
        return result;
    }

    RESPONSE.header.cmd = resp_cmd;
    RESPONSE.header.body_len = value.len;
    memcpy(SF_PROTO_RESP_BODY(task), value.str, value.len);
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_get_xattr_by_path(struct fast_task_info *task)
{
    int result;
    int fixed_size;
    string_t name;
    FDIRDEntryFullName fullname;
    FDIRServerDentry *dentry;

    fixed_size = sizeof(FDIRProtoGetXAttrByPathReq) + 1;
    if ((result=parse_xattr_name_info(task, fixed_size,
                    false, &name)) != 0)
    {
        return result;
    }

    if ((result=server_check_and_parse_dentry(task,
                    sizeof(FDIRProtoNameInfo) + name.len,
                    &fullname)) != 0)
    {
        return result;
    }

    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    return service_do_getxattr(task, dentry, &name,
            FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_RESP);
}

static int service_get_xattr_by_inode(struct fast_task_info *task)
{
    int result;
    int fixed_size;
    int64_t inode;
    string_t name;
    FDIRServerDentry *dentry;

    fixed_size = sizeof(FDIRProtoGetXAttrByInodeReq);
    if ((result=parse_xattr_name_info(task, fixed_size,
                    true, &name)) != 0)
    {
        return result;
    }

    inode = buff2long(REQUEST.body + sizeof(FDIRProtoNameInfo) + name.len);
    if ((dentry=inode_index_get_dentry(inode)) == NULL) {
        return ENOENT;
    }

    return service_do_getxattr(task, dentry, &name,
            FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_RESP);
}

static int service_do_listxattr(struct fast_task_info *task,
        FDIRServerDentry *dentry, const int resp_cmd)
{
    FDIRXAttrIterator it;
    const key_value_pair_t *kv;
    char *p;

    p = SF_PROTO_RESP_BODY(task);
    inode_index_list_xattr(dentry, &it);
    while ((kv=xattr_iterator_next(&it)) != NULL) {
        memcpy(p, kv->key.str, kv->key.len);
        p += kv->key.len;
        *p++ = '\0';
    }

    RESPONSE.header.cmd = resp_cmd;
    RESPONSE.header.body_len = p - SF_PROTO_RESP_BODY(task);
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_list_xattr_by_path(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;
    FDIRServerDentry *dentry;

    if ((result=server_check_and_parse_dentry(task,
                    0, &fullname)) != 0)
    {
        return result;
    }

    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    return service_do_listxattr(task, dentry,
            FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_RESP);
}

static int service_list_xattr_by_inode(struct fast_task_info *task)
{
    int result;
    int64_t inode;
    FDIRProtoListXAttrByInodeReq *req;
    FDIRServerDentry *dentry;

    if ((result=server_expect_body_length(sizeof(
                        FDIRProtoListXAttrByInodeReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoListXAttrByInodeReq *)REQUEST.body;
    inode = buff2long(req->inode);
    if ((dentry=inode_index_get_dentry(inode)) == NULL) {
        return ENOENT;
    }

    return service_do_listxattr(task, dentry,
            FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_RESP);
}

static int service_check_priv(struct fast_task_info *task)
{
    FCFSAuthValidatePriviledgeType priv_type;
    int64_t the_priv;

    switch (REQUEST.header.cmd) {
        case SF_PROTO_ACTIVE_TEST_REQ:
        case FDIR_SERVICE_PROTO_CLIENT_JOIN_REQ:
        case FDIR_SERVICE_PROTO_GET_MASTER_REQ:
        case SF_SERVICE_PROTO_GET_LEADER_REQ:
        case SF_SERVICE_PROTO_GET_GROUP_SERVERS_REQ:
        case FDIR_SERVICE_PROTO_GET_SLAVES_REQ:
        case FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ:
        case SF_SERVICE_PROTO_SETUP_CHANNEL_REQ:
        case SF_SERVICE_PROTO_CLOSE_CHANNEL_REQ:
        case SF_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ:
        case SF_SERVICE_PROTO_REBIND_CHANNEL_REQ:
        case FDIR_SERVICE_PROTO_NSS_FETCH_REQ:
            return 0;

        case FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_CREATE_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_SYMLINK_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_HDLINK_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_RENAME_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_RENAME_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_REQ:
        case FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_REQ:
        case FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_REQ:
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_GETLK_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_REQ:
        case FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_REQ:
            priv_type = fcfs_auth_validate_priv_type_pool_fdir;
            the_priv = FCFS_AUTH_POOL_ACCESS_WRITE;
            break;

        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_STAT_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_READLINK_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_READLINK_BY_PNAME_REQ:
        case FDIR_SERVICE_PROTO_READLINK_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_LIST_DENTRY_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_LIST_DENTRY_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ:
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_REQ:
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_REQ:
        case FDIR_SERVICE_PROTO_NAMESPACE_STAT_REQ:
            priv_type = fcfs_auth_validate_priv_type_pool_fdir;
            the_priv = FCFS_AUTH_POOL_ACCESS_READ;
            break;

        case FDIR_SERVICE_PROTO_SERVICE_STAT_REQ:
        case FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ:
            priv_type = fcfs_auth_validate_priv_type_user;
            the_priv = FCFS_AUTH_USER_PRIV_MONITOR_CLUSTER;
            break;

        case FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_REQ:
            priv_type = fcfs_auth_validate_priv_type_user;
            the_priv = FCFS_AUTH_USER_PRIV_SUBSCRIBE_SESSION;
            break;
        default:
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "unkown cmd: %d", REQUEST.header.cmd);
            return -EINVAL;
    }

    return fcfs_auth_for_server_check_priv(AUTH_CLIENT_CTX,
            &REQUEST, &RESPONSE, priv_type, the_priv);
}

static int service_process(struct fast_task_info *task)
{
    int result;
    switch (REQUEST.header.cmd) {
        case SF_PROTO_ACTIVE_TEST_REQ:
            RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
            return sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
        case FDIR_SERVICE_PROTO_CLIENT_JOIN_REQ:
            return service_deal_client_join(task);
        case FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ:
            return service_process_update(task,
                    service_deal_create_dentry,
                    FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP);
        case FDIR_SERVICE_PROTO_CREATE_BY_PNAME_REQ:
            return service_process_update(task,
                    service_deal_create_by_pname,
                    FDIR_SERVICE_PROTO_CREATE_BY_PNAME_RESP);
        case FDIR_SERVICE_PROTO_SYMLINK_DENTRY_REQ:
            return service_process_update(task,
                    service_deal_symlink_dentry,
                    FDIR_SERVICE_PROTO_SYMLINK_DENTRY_RESP);
        case FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_REQ:
            return service_process_update(task,
                    service_deal_symlink_by_pname,
                    FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_RESP);
        case FDIR_SERVICE_PROTO_HDLINK_DENTRY_REQ:
            return service_process_update(task,
                    service_deal_hdlink_dentry,
                    FDIR_SERVICE_PROTO_HDLINK_DENTRY_RESP);
        case FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_REQ:
            return service_process_update(task,
                    service_deal_hdlink_by_pname,
                    FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_RESP);
        case FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ:
            return service_process_update(task,
                    service_deal_remove_dentry,
                    FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP);
        case FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_REQ:
            return service_process_update(task,
                    service_deal_remove_by_pname,
                    FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_RESP);
        case FDIR_SERVICE_PROTO_RENAME_DENTRY_REQ:
            return service_process_update(task,
                    service_deal_rename_dentry,
                    FDIR_SERVICE_PROTO_RENAME_DENTRY_RESP);
        case FDIR_SERVICE_PROTO_RENAME_BY_PNAME_REQ:
            return service_process_update(task,
                    service_deal_rename_by_pname,
                    FDIR_SERVICE_PROTO_RENAME_BY_PNAME_RESP);
        case FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_REQ:
            return service_process_update(task,
                    service_deal_set_dentry_size,
                    FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_RESP);
        case FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_REQ:
            return service_process_update(task,
                    service_deal_batch_set_dentry_size,
                    FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_RESP);
        case FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_REQ:
            return service_process_update(task,
                    service_deal_modify_dentry_stat,
                    FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_RESP);
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_REQ:
            return service_process_update(task,
                    service_deal_set_xattr_by_path,
                    FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_RESP);
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_REQ:
            return service_process_update(task,
                    service_deal_set_xattr_by_inode,
                    FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_RESP);
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_REQ:
            return service_process_update(task,
                    service_deal_remove_xattr_by_path,
                    FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_RESP);
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_REQ:
            return service_process_update(task,
                    service_deal_remove_xattr_by_inode,
                    FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_RESP);
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_lookup_inode_by_path(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_lookup_inode_by_pname(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_stat_dentry_by_path(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_stat_dentry_by_inode(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_STAT_BY_PNAME_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_stat_dentry_by_pname(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_READLINK_BY_PATH_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_readlink_by_path(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_READLINK_BY_PNAME_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_readlink_by_pname(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_READLINK_BY_INODE_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_readlink_by_inode(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_LIST_DENTRY_BY_PATH_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_list_dentry_by_path(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_LIST_DENTRY_BY_INODE_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_list_dentry_by_inode(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_deal_list_dentry_next(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ:
            if ((result=service_check_master(task)) == 0) {
                return service_deal_flock_dentry(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_GETLK_DENTRY_REQ:
            if ((result=service_check_master(task)) == 0) {
                return service_deal_getlk_dentry(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_REQ:
            if ((result=service_check_master(task)) == 0) {
                return service_deal_sys_lock_dentry(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_REQ:
            if ((result=service_check_master(task)) == 0) {
                return service_deal_sys_unlock_dentry(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_get_xattr_by_path(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_get_xattr_by_inode(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_list_xattr_by_path(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_REQ:
            if ((result=service_check_readable(task)) == 0) {
                return service_list_xattr_by_inode(task);
            }
            return result;
        case FDIR_SERVICE_PROTO_SERVICE_STAT_REQ:
            return service_deal_service_stat(task);
        case FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ:
            return service_deal_cluster_stat(task);
        case FDIR_SERVICE_PROTO_NAMESPACE_STAT_REQ:
            return service_deal_namespace_stat(task);
        case FDIR_SERVICE_PROTO_GET_MASTER_REQ:
            if ((result=service_deal_get_master(task)) == 0) {
                RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GET_MASTER_RESP;
            }
            return result;
        case SF_SERVICE_PROTO_GET_LEADER_REQ:
            if ((result=service_deal_get_master(task)) == 0) {
                RESPONSE.header.cmd = SF_SERVICE_PROTO_GET_LEADER_RESP;
            }
            return result;
        case SF_SERVICE_PROTO_GET_GROUP_SERVERS_REQ:
            return service_deal_get_group_servers(task);
        case FDIR_SERVICE_PROTO_GET_SLAVES_REQ:
            return service_deal_get_slaves(task);
        case FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ:
            return service_deal_get_readable_server(task);
        case FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_REQ:
            return service_deal_nss_subscribe(task);
        case FDIR_SERVICE_PROTO_NSS_FETCH_REQ:
            return service_deal_nss_fetch(task);
        case SF_SERVICE_PROTO_SETUP_CHANNEL_REQ:
            if ((result=sf_server_deal_setup_channel(task,
                            &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL,
                            &RESPONSE)) == 0)
            {
                TASK_CTX.common.response_done = true;
            }
            return result;
        case SF_SERVICE_PROTO_CLOSE_CHANNEL_REQ:
            return sf_server_deal_close_channel(task,
                    &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL, &RESPONSE);
        case SF_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ:
            return sf_server_deal_report_req_receipt(task,
                    SERVER_TASK_TYPE, IDEMPOTENCY_CHANNEL, &RESPONSE);
        case SF_SERVICE_PROTO_REBIND_CHANNEL_REQ:
            return sf_server_deal_rebind_channel(task,
                    &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL, &RESPONSE);
        default:
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "unkown cmd: %d", REQUEST.header.cmd);
            return -EINVAL;
    }
}

int service_deal_task(struct fast_task_info *task, const int stage)
{
    int result;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "task: %p, sock: %d, nio stage: %d, continue: %d, "
            "cmd: %d (%s)", __LINE__, task, task->event.fd, stage,
            stage == SF_NIO_STAGE_CONTINUE,
            ((FDIRProtoHeader *)task->data)->cmd,
            fdir_get_cmd_caption(((FDIRProtoHeader *)task->data)->cmd));
            */

    if (stage == SF_NIO_STAGE_CONTINUE) {
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
        if (AUTH_ENABLED) {
            if ((result=service_check_priv(task)) == 0) {
                result = service_process(task);
            }
        } else {
            result = service_process(task);
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return sf_proto_deal_task_done(task, &TASK_CTX.common);
    }
}

void *service_alloc_thread_extra_data(const int thread_index)
{
    FDIRServerContext *server_context;
    int element_size;

    server_context = (FDIRServerContext *)fc_malloc(sizeof(FDIRServerContext));
    if (server_context == NULL) {
        return NULL;
    }

    memset(server_context, 0, sizeof(FDIRServerContext));
    if (fast_mblock_init_ex1(&server_context->service.record_allocator,
                "binlog_record1", sizeof(FDIRBinlogRecord), 4 * 1024,
                0, NULL, NULL, false) != 0)
    {
        free(server_context);
        return NULL;
    }

    element_size = sizeof(IdempotencyRequest) + sizeof(FDIRDEntryInfo);
    if (fast_mblock_init_ex1(&server_context->service.request_allocator,
                "idempotency_request", element_size,
                1024, 0, idempotency_request_alloc_init,
                &server_context->service.request_allocator, true) != 0)
    {
        free(server_context);
        return NULL;
    }

    return server_context;
}

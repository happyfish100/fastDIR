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
#include "fastcommon/json_parser.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "common/fdir_proto.h"
#include "binlog/binlog_producer.h"
#include "binlog/binlog_pack.h"
#include "server_global.h"
#include "server_func.h"
#include "dentry.h"
#include "cluster_relationship.h"
#include "service_handler.h"

static volatile int64_t next_token;   //next token for dentry list

int service_handler_init()
{
    next_token = ((int64_t)g_current_time) << 32;
    return 0;
}

int service_handler_destroy()
{   
    return 0;
}

void service_task_finish_cleanup(struct fast_task_info *task)
{
    //FDIRServerTaskArg *task_arg;

    //task_arg = (FDIRServerTaskArg *)task->arg;

    dentry_array_free(&DENTRY_LIST_CACHE.array);

    __sync_add_and_fetch(&((FDIRServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int service_deal_actvie_test(struct fast_task_info *task)
{
    return server_expect_body_length(task, 0);
}

static int service_deal_service_stat(struct fast_task_info *task)
{
    int result;
    FDIRDentryCounters counters;
    FDIRProtoServiceStatResp *stat_resp;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    data_thread_sum_counters(&counters);
    stat_resp = (FDIRProtoServiceStatResp *)REQUEST.body;

    stat_resp->is_master = CLUSTER_MYSELF_PTR == CLUSTER_MASTER_PTR ? 1 : 0;
    stat_resp->status = CLUSTER_MYSELF_PTR->status;
    int2buff(CLUSTER_MYSELF_PTR->server->id, stat_resp->server_id);

    int2buff(SF_G_CONN_CURRENT_COUNT, stat_resp->connection.current_count);
    int2buff(SF_G_CONN_MAX_COUNT, stat_resp->connection.max_count);
    long2buff(DATA_CURRENT_VERSION, stat_resp->dentry.current_data_version);
    long2buff(CURRENT_INODE_SN, stat_resp->dentry.current_inode_sn);
    long2buff(counters.ns, stat_resp->dentry.counters.ns);
    long2buff(counters.dir, stat_resp->dentry.counters.dir);
    long2buff(counters.file, stat_resp->dentry.counters.file);

    RESPONSE.header.body_len = sizeof(FDIRProtoServiceStatResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_SERVICE_STAT_RESP;
    TASK_ARG->context.response_done = true;

    return 0;
}

static int service_deal_cluster_stat(struct fast_task_info *task)
{
    int result;
    FDIRProtoClusterStatRespBodyHeader *body_header;
    FDIRProtoClusterStatRespBodyPart *body_part;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    body_header = (FDIRProtoClusterStatRespBodyHeader *)REQUEST.body;
    body_part = (FDIRProtoClusterStatRespBodyPart *)(REQUEST.body +
            sizeof(FDIRProtoClusterStatRespBodyHeader));

    int2buff(CLUSTER_SERVER_ARRAY.count, body_header->count);

    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++, body_part++) {
        int2buff(cs->server->id, body_part->server_id);
        body_part->is_master = cs->is_master;
        body_part->status = cs->status;

        snprintf(body_part->ip_addr, sizeof(body_part->ip_addr), "%s",
                SERVICE_GROUP_ADDRESS_FIRST_IP(cs->server));
        short2buff(SERVICE_GROUP_ADDRESS_FIRST_PORT(cs->server),
                body_part->port);
    }

    RESPONSE.header.body_len = (char *)body_part - REQUEST.body;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP;
    TASK_ARG->context.response_done = true;

    return 0;
}

static int service_deal_get_master(struct fast_task_info *task)
{
    int result;
    FDIRProtoGetServerResp *resp;
    FDIRClusterServerInfo *master;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    master = CLUSTER_MASTER_PTR;
    if (master == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "the master NOT exist");
        return ENOENT;
    }

    resp = (FDIRProtoGetServerResp *)REQUEST.body;
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                master->server), task->client_ip);

    int2buff(master->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GET_MASTER_RESP;
    TASK_ARG->context.response_done = true;

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

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    body_header = (FDIRProtoGetSlavesRespBodyHeader *)REQUEST.body;
    part_start = body_part = (FDIRProtoGetSlavesRespBodyPart *)(
            REQUEST.body + sizeof(FDIRProtoGetSlavesRespBodyHeader));

    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++) {
        if (cs->is_master) {
            continue;
        }

        int2buff(cs->server->id, body_part->server_id);
        body_part->status = cs->status;

        addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                cs->server), task->client_ip);
        snprintf(body_part->ip_addr, sizeof(body_part->ip_addr),
                "%s", addr->conn.ip_addr);
        short2buff(addr->conn.port, body_part->port);

        body_part++;
    }
    int2buff(body_part - part_start, body_header->count);

    RESPONSE.header.body_len = (char *)body_part - REQUEST.body;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GET_SLAVES_RESP;
    TASK_ARG->context.response_done = true;

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
    if (CLUSTER_SERVER_ARRAY.servers[index].status ==
            FDIR_SERVER_STATUS_ACTIVE)
    {
        return CLUSTER_SERVER_ARRAY.servers + index;
    }

    acc_index = 0;
    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    do {
        old_index = acc_index;
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++) {
            if (cs->status == FDIR_SERVER_STATUS_ACTIVE) {
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
        return ENOENT;
    }

    resp = (FDIRProtoGetServerResp *)REQUEST.body;
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                cs->server), task->client_ip);

    int2buff(cs->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerResp);
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP;
    TASK_ARG->context.response_done = true;

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
        const int front_part_size, const int fixed_part_size,
        FDIRDEntryFullName *fullname)
{
    int result;
    int req_body_len;

    if ((result=server_check_body_length(task,
                    fixed_part_size + 1, fixed_part_size +
                    NAME_MAX + PATH_MAX)) != 0)
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

/*
static void server_get_dentry_hashcode(FDIRPathInfo *path_info,
        const bool include_last)
{
    char logic_path[NAME_MAX + PATH_MAX + 2];
    int len;
    string_t *part;
    string_t *end;
    char *p;

    p = logic_path;
    memcpy(p, path_info->fullname.ns.str, path_info->fullname.ns.len);
    p += path_info->fullname.ns.len;

    if (include_last) {
        end = path_info->paths + path_info->count;
    } else {
        end = path_info->paths + path_info->count - 1;
    }

    for (part=path_info->paths; part<end; part++) {
        *p++ = '/';
        memcpy(p, part->str, part->len);
        p += part->len;
    }

    len = p - logic_path;
    //logInfo("logic_path for hash code: %.*s", len, logic_path);
    path_info->hash_code = simple_hash(logic_path, len);
}

#define server_get_parent_hashcode(path_info)  \
    server_get_dentry_hashcode(path_info, false)

#define server_get_my_hashcode(path_info)  \
    server_get_dentry_hashcode(path_info, true)
*/

static int server_binlog_produce(struct fast_task_info *task)
{
    ServerBinlogRecordBuffer *rbuffer;
    int result;

    if ((rbuffer=server_binlog_alloc_rbuffer()) == NULL) {
        return ENOMEM;
    }

    TASK_ARG->context.deal_func = NULL;
    rbuffer->data_version = RECORD->data_version;
    RECORD->timestamp = g_current_time;

    fast_buffer_reset(&rbuffer->buffer);
    result = binlog_pack_record(RECORD, &rbuffer->buffer);

    fast_mblock_free_object(&((FDIRServerContext *)task->thread_data->arg)->
            service.record_allocator, RECORD);
    RECORD = NULL;

    if (result == 0) {
        rbuffer->args = task;
        rbuffer->task_version = __sync_add_and_fetch(
                &((FDIRServerTaskArg *)task->arg)->task_version, 0);
        binlog_push_to_producer_queue(rbuffer);
        return SLAVE_SERVER_COUNT > 0 ? TASK_STATUS_CONTINUE : result;
    } else {
        server_binlog_free_rbuffer(rbuffer);
        return result;
    }
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

    return 0;
}

static inline void dentry_stat_output(struct fast_task_info *task,
        FDIRServerDentry *dentry)
{
    FDIRProtoStatDEntryResp *stat_resp;

    stat_resp = (FDIRProtoStatDEntryResp *)REQUEST.body;
    long2buff(dentry->inode, stat_resp->inode);
    int2buff(dentry->stat.mode, stat_resp->mode);
    int2buff(dentry->stat.ctime, stat_resp->ctime);
    int2buff(dentry->stat.mtime, stat_resp->mtime);
    long2buff(dentry->stat.size, stat_resp->size);

    RESPONSE.header.body_len = sizeof(FDIRProtoStatDEntryResp);
    TASK_ARG->context.response_done = true;
}

static void record_deal_done_notify(FDIRBinlogRecord *record,
        const int result, const bool is_error)
{
    struct fast_task_info *task;

    task = (struct fast_task_info *)record->notify.args;
    if (result != 0) {
        int log_level;

        if (is_error) {
            log_level = LOG_ERR;
        } else {
            log_level = LOG_WARNING;
        }
        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, "
                "client ip: %s, %s dentry fail, "
                "errno: %d, error info: %s, "
                "namespace: %.*s, path: %.*s",
                __LINE__, task->client_ip,
                get_operation_caption(record->operation),
                result, STRERROR(result),
                record->fullname.ns.len, record->fullname.ns.str,
                record->fullname.path.len, record->fullname.path.str);
    } else {
        if (record->operation == BINLOG_OP_CREATE_DENTRY_INT ||
                record->operation == BINLOG_OP_REMOVE_DENTRY_INT)
        {
            dentry_stat_output(task, record->dentry);
            RESPONSE.header.body_len = sizeof(FDIRProtoStatDEntryResp);
            TASK_ARG->context.response_done = true;
        }
    }

    RESPONSE_STATUS = result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
}

static int handle_record_deal_done(struct fast_task_info *task)
{
    if (RESPONSE_STATUS == 0) {
        return server_binlog_produce(task);
    } else {
        return RESPONSE_STATUS;
    }
}

static inline int push_record_to_data_thread_queue(struct fast_task_info *task)
{
    int result;

    RECORD->notify.func = record_deal_done_notify;
    RECORD->notify.args = task;

    TASK_ARG->context.deal_func = handle_record_deal_done;
    result = push_to_data_thread_queue(RECORD);
    return result == 0 ? TASK_STATUS_CONTINUE : result;
}

#define SERVER_SET_RECORD_PATH_INFO()  \
    do {   \
        RECORD->hash_code = simple_hash(RECORD->fullname.ns.str,  \
                RECORD->fullname.ns.len);  \
        RECORD->inode = RECORD->data_version = 0;  \
        RECORD->options.flags = 0;   \
        RECORD->options.path_info.flags = BINLOG_OPTIONS_PATH_ENABLED;  \
    } while (0)

static int service_deal_create_dentry(struct fast_task_info *task)
{
    int result;
    FDIRProtoCreateDEntryFront *proto_front;

    if ((result=alloc_record_object(task)) != 0) {
        return result;
    }

    if ((result=server_check_and_parse_dentry(task,
                    sizeof(FDIRProtoCreateDEntryFront),
                    sizeof(FDIRProtoCreateDEntryBody),
                    &RECORD->fullname)) != 0)
    {
        return result;
    }

    SERVER_SET_RECORD_PATH_INFO();

    proto_front = (FDIRProtoCreateDEntryFront *)REQUEST.body;
    RECORD->stat.mode = buff2int(proto_front->mode);

    RECORD->operation = BINLOG_OP_CREATE_DENTRY_INT;
    RECORD->stat.ctime = RECORD->stat.mtime = g_current_time;
    RECORD->options.ctime = RECORD->options.mtime = 1;
    RECORD->options.mode = 1;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_remove_dentry(struct fast_task_info *task)
{
    int result;

    if ((result=alloc_record_object(task)) != 0) {
        return result;
    }

    if ((result=server_check_and_parse_dentry(task,
                    0, sizeof(FDIRProtoRemoveDEntry),
                    &RECORD->fullname)) != 0)
    {
        return result;
    }

    SERVER_SET_RECORD_PATH_INFO();
    RECORD->operation = BINLOG_OP_REMOVE_DENTRY_INT;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP;
    return push_record_to_data_thread_queue(task);
}

static int service_deal_stat_dentry_by_path(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;
    FDIRServerDentry *dentry;
    FDIRProtoStatDEntryResp *stat_resp;

    if ((result=server_check_and_parse_dentry(task, 0,
                    sizeof(FDIRProtoDEntryInfo), &fullname)) != 0)
    {
        return result;
    }

    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP;
    if ((result=dentry_find(&fullname, &dentry)) != 0) {
        return result;
    }

    stat_resp = (FDIRProtoStatDEntryResp *)REQUEST.body;
    long2buff(dentry->inode, stat_resp->inode);
    int2buff(dentry->stat.mode, stat_resp->mode);
    int2buff(dentry->stat.ctime, stat_resp->ctime);
    int2buff(dentry->stat.mtime, stat_resp->mtime);
    long2buff(dentry->stat.size, stat_resp->size);

    RESPONSE.header.body_len = sizeof(FDIRProtoStatDEntryResp);
    TASK_ARG->context.response_done = true;
    return 0;
}

static int service_deal_stat_dentry_by_inode(struct fast_task_info *task)
{
    return 0;
}

static int server_list_dentry_output(struct fast_task_info *task)
{
    FDIRProtoListDEntryRespBodyHeader *body_header;
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
    p = REQUEST.body + sizeof(FDIRProtoListDEntryRespBodyHeader);
    start = DENTRY_LIST_CACHE.array.entries +
        DENTRY_LIST_CACHE.offset;
    end = start + remain_count;
    for (dentry=start; dentry<end; dentry++) {
        if (buf_end - p < sizeof(FDIRProtoListDEntryRespBodyPart) +
                (*dentry)->name.len)
        {
            break;
        }
        body_part = (FDIRProtoListDEntryRespBodyPart *)p;
        body_part->name_len = (*dentry)->name.len;
        memcpy(body_part->name_str, (*dentry)->name.str, (*dentry)->name.len);
        p += sizeof(FDIRProtoListDEntryRespBodyPart) + (*dentry)->name.len;
    }
    count = dentry - start;
    RESPONSE.header.body_len = p - REQUEST.body;
    RESPONSE.header.cmd = FDIR_SERVICE_PROTO_LIST_DENTRY_RESP;

    body_header = (FDIRProtoListDEntryRespBodyHeader *)REQUEST.body;
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

    TASK_ARG->context.response_done = true;
    return 0;
}

static int service_deal_list_dentry_first(struct fast_task_info *task)
{
    int result;
    FDIRDEntryFullName fullname;

    if ((result=server_check_and_parse_dentry(task,
                    0, sizeof(FDIRProtoListDEntryFirstBody),
                    &fullname)) != 0)
    {
        return result;
    }

    if ((result=dentry_list(&fullname, &DENTRY_LIST_CACHE.array)) != 0) {
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

    if ((result=server_expect_body_length(task,
                    sizeof(FDIRProtoListDEntryNextBody))) != 0)
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

    REQUEST.header.cmd = ((FDIRProtoHeader *)task->data)->cmd;
    REQUEST.header.body_len = task->length - sizeof(FDIRProtoHeader);
    REQUEST.body = task->data + sizeof(FDIRProtoHeader);
}

static inline int service_check_master(struct fast_task_info *task)
{
    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }

    return 0;
}

static inline int service_check_readable(struct fast_task_info *task)
{
    if (!(CLUSTER_MYSELF_PTR == CLUSTER_MASTER_PTR ||
                CLUSTER_MYSELF_PTR->status == FDIR_SERVER_STATUS_ACTIVE))
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not active");
        return EINVAL;
    }

    return 0;
}

static int deal_task_done(struct fast_task_info *task)
{
    FDIRProtoHeader *proto_header;
    int r;
    int time_used;
    char time_buff[32];

    if (TASK_ARG->context.log_error && RESPONSE.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, cmd: %d (%s), req body length: %d, %s",
                __LINE__, task->client_ip, REQUEST.header.cmd,
                fdir_get_cmd_caption(REQUEST.header.cmd),
                REQUEST.header.body_len,
                RESPONSE.error.message);
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
    if (time_used > 20 * 1000) {
        lwarning("process a request timed used: %s us, "
                "cmd: %d (%s), req body len: %d, resp body len: %d",
                long_to_comma_str(time_used, time_buff),
                REQUEST.header.cmd, fdir_get_cmd_caption(REQUEST.header.cmd),
                REQUEST.header.body_len, RESPONSE.header.body_len);
    }

    if (REQUEST.header.cmd != FDIR_CLUSTER_PROTO_PING_MASTER_REQ) {
    logDebug("file: "__FILE__", line: %d, "
            "client ip: %s, req cmd: %d (%s), req body_len: %d, "
            "resp cmd: %d (%s), status: %d, resp body_len: %d, "
            "time used: %s us", __LINE__,
            task->client_ip, REQUEST.header.cmd,
            fdir_get_cmd_caption(REQUEST.header.cmd),
            REQUEST.header.body_len, RESPONSE.header.cmd,
            fdir_get_cmd_caption(RESPONSE.header.cmd),
            RESPONSE_STATUS, RESPONSE.header.body_len,
            long_to_comma_str(time_used, time_buff));
    }

    return r == 0 ? RESPONSE_STATUS : r;
}

int service_deal_task(struct fast_task_info *task)
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
                result = service_deal_actvie_test(task);
                break;
            case FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ:
                if ((result=service_check_master(task)) == 0) {
                    result = service_deal_create_dentry(task);
                }
                break;
            case FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ:
                if ((result=service_check_master(task)) == 0) {
                    result = service_deal_remove_dentry(task);
                }
                break;
            case FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ:
                if ((result=service_check_readable(task)) == 0) {
                    result = service_deal_stat_dentry_by_path(task);
                }
                break;
            case FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ:
                if ((result=service_check_readable(task)) == 0) {
                    result = service_deal_stat_dentry_by_inode(task);
                }
                break;
            case FDIR_SERVICE_PROTO_LIST_DENTRY_FIRST_REQ:
                if ((result=service_check_readable(task)) == 0) {
                    result = service_deal_list_dentry_first(task);
                }
                break;
            case FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ:
                if ((result=service_check_readable(task)) == 0) {
                    result = service_deal_list_dentry_next(task);
                }
                break;
            case FDIR_SERVICE_PROTO_SERVICE_STAT_REQ:
                result = service_deal_service_stat(task);
                break;
            case FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ:
                result = service_deal_cluster_stat(task);
                break;
            case FDIR_SERVICE_PROTO_GET_MASTER_REQ:
                result = service_deal_get_master(task);
                break;
            case FDIR_SERVICE_PROTO_GET_SLAVES_REQ:
                result = service_deal_get_slaves(task);
                break;
            case FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ:
                result = service_deal_get_readable_server(task);
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

void *service_alloc_thread_extra_data(const int thread_index)
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
    if (fast_mblock_init_ex2(&server_context->service.record_allocator,
                "binlog_record1", sizeof(FDIRBinlogRecord), 4 * 1024,
                NULL, NULL, false, NULL, NULL, NULL) != 0)
    {
        free(server_context);
        return NULL;
    }

    return server_context;
}

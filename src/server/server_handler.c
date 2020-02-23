//server_handler.c

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
#include "server_global.h"
#include "server_func.h"
#include "dentry.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"
#include "server_handler.h"

#define SERVER_CONTEXT task_context->server_context
#define TASK     task_context->task
#define TASK_ARG task_context->task_arg
#define REQUEST  task_context->request
#define RESPONSE task_context->response
#define RESPONSE_STATUS RESPONSE.header.status
#define RESP_STATUS     task_context.response.header.status

static volatile int64_t next_token;   //next token for dentry list

int server_handler_init()
{
    next_token = ((int64_t)g_current_time) << 32;
    return 0;
}

int server_handler_destroy()
{   
    return 0;
}

void server_task_finish_cleanup(struct fast_task_info *task)
{
    FDIRServerTaskArg *task_arg;

    task_arg = (FDIRServerTaskArg *)task->arg;

    if (task_arg->cluster_peer != NULL) {
        ct_slave_server_offline(task_arg->cluster_peer);
        task_arg->cluster_peer = NULL;
    }

    dentry_array_free(&task_arg->dentry_list_cache.array);

    __sync_add_and_fetch(&((FDIRServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int server_deal_actvie_test(ServerTaskContext *task_context)
{
    return server_expect_body_length(task_context, 0);
}

static int server_check_config_sign(ServerTaskContext *task_context,
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

static int server_deal_get_server_status(ServerTaskContext *task_context)
{
    int result;
    int server_id;
    FDIRProtoGetServerStatusReq *req;
    FDIRProtoGetServerStatusResp *resp;

    if ((result=server_expect_body_length(task_context,
                    sizeof(FDIRProtoGetServerStatusReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoGetServerStatusReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if ((result=server_check_config_sign(task_context, server_id,
                    req->config_sign)) != 0)
    {
        return result;
    }

    resp = (FDIRProtoGetServerStatusResp *)REQUEST.body;

    resp->is_master = MYSELF_IS_MASTER;
    int2buff(CLUSTER_MYSELF_PTR->id, resp->server_id);
    long2buff(DATA_VERSION, resp->data_version);

    RESPONSE.header.body_len = sizeof(FDIRProtoGetServerStatusResp);
    RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP;
    task_context->response_done = true;
    return 0;
}

static int server_deal_join_master(ServerTaskContext *task_context)
{
    int result;
    int server_id;
    int64_t data_version;
    FDIRProtoJoinMasterReq *req;
    FCServerInfo *peer;

    if ((result=server_expect_body_length(task_context,
                    sizeof(FDIRProtoJoinMasterReq))) != 0)
    {
        return result;
    }

    req = (FDIRProtoJoinMasterReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    data_version = buff2long(req->data_version);
    peer = fc_server_get_by_id(&CLUSTER_CONFIG_CTX, server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }

    if ((result=server_check_config_sign(task_context, server_id,
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

    if (TASK_ARG->cluster_peer != NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d already joined", server_id);
        return EEXIST;
    }

    TASK_ARG->cluster_peer = peer;
    ct_slave_server_online(peer);
    return 0;
}

static int server_deal_ping_master(ServerTaskContext *task_context)
{
    int result;

    if ((result=server_expect_body_length(task_context, 0)) != 0) {
        return result;
    }

    if (TASK_ARG->cluster_peer == NULL) {
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

    RESPONSE.header.cmd = FDIR_CLUSTER_PROTO_PING_MASTER_RESP;
    return 0;
}

static int server_deal_next_master(ServerTaskContext *task_context)
{
    int result;
    int master_id;
    FCServerInfo *master;

    if ((result=server_expect_body_length(task_context, 4)) != 0) {
        return result;
    }

    if (MYSELF_IS_MASTER) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am already master");
        return EEXIST;
    }

    master_id = buff2int(REQUEST.body);
    master = fc_server_get_by_id(&CLUSTER_CONFIG_CTX, master_id);
    if (master == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "master id: %d not exist", master_id);
        return ENOENT;
    }

    if (REQUEST.header.cmd == FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER) {
        return cluster_relationship_pre_set_master(master);
    } else {
        return cluster_relationship_commit_master(master, false);
    }
}

static int server_compare_dentry_info(FDIRPathInfo *pinfo1,
        FDIRPathInfo *pinfo2)
{
    int result;
    string_t *s1;
    string_t *s2;
    string_t *end;

    if ((result=fc_string_compare(&pinfo1->ns, &pinfo2->ns)) != 0) {
        return result;
    }

    if ((result=pinfo1->count - pinfo2->count) != 0) {
        return result;
    }

    end = pinfo1->paths + pinfo1->count;
    for (s1=pinfo1->paths,s2=pinfo2->paths; s1<end; s1++,s2++) {
        if ((result=fc_string_compare(s1, s2)) != 0) {
            return result;
        }
    }

    return 0;
}

static int server_parse_dentry_info(ServerTaskContext *task_context,
        char *start, FDIRPathInfo *path_info)
{
    FDIRProtoDEntryInfo *proto_dentry;

    proto_dentry = (FDIRProtoDEntryInfo *)start;
    path_info->ns.len = proto_dentry->ns_len;
    path_info->ns.str = proto_dentry->ns_str;
    path_info->path.len = buff2short(proto_dentry->path_len);
    path_info->path.str = proto_dentry->ns_str + path_info->ns.len;

    if (path_info->ns.len <= 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid namespace length: %d <= 0",
                path_info->ns.len);
        return EINVAL;
    }

    if (path_info->path.len <= 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid path length: %d <= 0",
                path_info->path.len);
        return EINVAL;
    }
    if (path_info->path.len > PATH_MAX) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid path length: %d > %d",
                path_info->path.len, PATH_MAX);
        return EINVAL;
    }

    if (path_info->path.str[0] != '/') {
        RESPONSE.error.length = snprintf(
                RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "invalid path: %.*s", path_info->path.len,
                path_info->path.str);
        return EINVAL;
    }

    path_info->count = split_string_ex(&path_info->path, '/',
        path_info->paths, FDIR_MAX_PATH_COUNT, true);
    return 0;
}

static inline int server_check_and_parse_dentry(ServerTaskContext *task_context,
        const int front_part_size, const int fixed_part_size)
{
    int result;
    int req_body_len;

    if ((result=server_check_body_length(task_context,
                    fixed_part_size + 1, fixed_part_size +
                    NAME_MAX + PATH_MAX)) != 0)
    {
        return result;
    }

    if ((result=server_parse_dentry_info(task_context,
                    REQUEST.body + front_part_size,
                    &TASK_ARG->path_info)) != 0)
    {
        return result;
    }

    req_body_len = fixed_part_size + TASK_ARG->path_info.ns.len +
        TASK_ARG->path_info.path.len;
    if (req_body_len != REQUEST.header.body_len) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "body length: %d != expect: %d",
                REQUEST.header.body_len, req_body_len);
        return EINVAL;
    }

    return 0;
}

static unsigned int server_get_dentry_hashcode(FDIRPathInfo *path_info,
        const bool include_last)
{
    char logic_path[NAME_MAX + PATH_MAX + 2];
    int len;
    string_t *part;
    string_t *end;
    char *p;

    p = logic_path;
    memcpy(p, path_info->ns.str, path_info->ns.len);
    p += path_info->ns.len;

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
    return simple_hash(logic_path, len);
}

#define server_get_parent_hashcode(path_info)  \
    server_get_dentry_hashcode(path_info, false)

#define server_get_my_hashcode(path_info)  \
    server_get_dentry_hashcode(path_info, true)

static int server_deal_create_dentry(ServerTaskContext *task_context)
{
    int result;
    FDIRProtoCreateDEntryFront *proto_front;
    unsigned int hash_code;
    unsigned int thread_index;
    int flags;
    int mode;

    if (!REQUEST.forwarded) {
        if ((result=server_check_and_parse_dentry(task_context,
                        sizeof(FDIRProtoCreateDEntryFront),
                        sizeof(FDIRProtoCreateDEntryBody))) != 0)
        {
            return result;
        }
         
        hash_code = server_get_parent_hashcode(&TASK_ARG->path_info);
        thread_index = hash_code % g_sf_global_vars.work_threads;

        logInfo("hash_code: %u, target thread_index: %d, current thread_index: %d",
                hash_code, thread_index, SERVER_CONTEXT->thread_index);

        if (thread_index != SERVER_CONTEXT->thread_index) {
            REQUEST.done = false;
            return sf_nio_notify(TASK, SF_NIO_STAGE_FORWARDED);
        }
    }

    proto_front = (FDIRProtoCreateDEntryFront *)REQUEST.body;
    flags = buff2int(proto_front->flags);
    mode = buff2int(proto_front->mode);
    return dentry_create(SERVER_CONTEXT, &TASK_ARG->path_info, flags, mode);
}

static int server_deal_remove_dentry(ServerTaskContext *task_context)
{
    int result;
    unsigned int hash_code;
    unsigned int thread_index;

    if (!REQUEST.forwarded) {
        if ((result=server_check_and_parse_dentry(task_context,
                        0, sizeof(FDIRProtoRemoveDEntry))) != 0)
        {
            return result;
        }
         
        hash_code = server_get_parent_hashcode(&TASK_ARG->path_info);
        thread_index = hash_code % g_sf_global_vars.work_threads;

        logInfo("hash_code: %u, target thread_index: %d, current thread_index: %d",
                hash_code, thread_index, SERVER_CONTEXT->thread_index);

        if (thread_index != SERVER_CONTEXT->thread_index) {
            REQUEST.done = false;
            return sf_nio_notify(TASK, SF_NIO_STAGE_FORWARDED);
        }
    }

    return dentry_remove(SERVER_CONTEXT, &TASK_ARG->path_info);
}

static int server_list_dentry_output(ServerTaskContext *task_context)
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

    remain_count = TASK_ARG->dentry_list_cache.array.count -
        TASK_ARG->dentry_list_cache.offset;

    buf_end = TASK->data + TASK->size;
    p = REQUEST.body + sizeof(FDIRProtoListDEntryRespBodyHeader);
    start = TASK_ARG->dentry_list_cache.array.entries +
        TASK_ARG->dentry_list_cache.offset;
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
    RESPONSE.header.cmd = FDIR_PROTO_LIST_DENTRY_RESP;

    body_header = (FDIRProtoListDEntryRespBodyHeader *)REQUEST.body;
    int2buff(count, body_header->count);
    if (count < remain_count) {
        TASK_ARG->dentry_list_cache.offset += count;
        TASK_ARG->dentry_list_cache.expires = g_current_time + 60;
        TASK_ARG->dentry_list_cache.token = __sync_add_and_fetch(&next_token, 1);

        body_header->is_last = 0;
        long2buff(TASK_ARG->dentry_list_cache.token, body_header->token);
    } else {
        body_header->is_last = 1;
        long2buff(0, body_header->token);
    }

    task_context->response_done = true;
    return 0;
}

static int server_deal_list_dentry_first(ServerTaskContext *task_context)
{
    int result;

    if ((result=server_check_and_parse_dentry(task_context,
                    0, sizeof(FDIRProtoListDEntryFirstBody))) != 0)
    {
        return result;
    }

    if ((result=dentry_list(SERVER_CONTEXT, &TASK_ARG->path_info,
                    &TASK_ARG->dentry_list_cache.array)) != 0)
    {
        return result;
    }

    TASK_ARG->dentry_list_cache.offset = 0;
    return server_list_dentry_output(task_context);
}

static int server_deal_list_dentry_next(ServerTaskContext *task_context)
{
    FDIRProtoListDEntryNextBody *next_body;
    int result;
    int offset;
    int64_t token;

    if ((result=server_expect_body_length(task_context,
                    sizeof(FDIRProtoListDEntryNextBody))) != 0)
    {
        return result;
    }

    if (TASK_ARG->dentry_list_cache.expires < g_current_time) {
        task_context->response.error.length = sprintf(
                task_context->response.error.message,
                "dentry list cache expires, please try again");
        return ETIMEDOUT;
    }

    next_body = (FDIRProtoListDEntryNextBody *)REQUEST.body;
    token = buff2long(next_body->token);
    offset = buff2int(next_body->offset);
    if (token != TASK_ARG->dentry_list_cache.token) {
        task_context->response.error.length = sprintf(
                task_context->response.error.message,
                "invalid token for next list");
        return EINVAL;
    }
    if (offset != TASK_ARG->dentry_list_cache.offset) {
        task_context->response.error.length = sprintf(
                task_context->response.error.message,
                "next list offset: %d != expected: %d",
                offset, TASK_ARG->dentry_list_cache.offset);
        return EINVAL;
    }
    return server_list_dentry_output(task_context);
}

static inline void init_task_context(ServerTaskContext *task_context)
{
    SERVER_CONTEXT = (FDIRServerContext *)TASK->thread_data->arg;
    TASK_ARG = (FDIRServerTaskArg *)TASK->arg;

    if (TASK->nio_stage != SF_NIO_STAGE_FORWARDED) {
        TASK_ARG->req_start_time = get_current_time_us();
    }
    RESPONSE.header.cmd = FDIR_PROTO_ACK;
    RESPONSE.header.body_len = 0;
    RESPONSE.header.status = 0;
    RESPONSE.error.length = 0;
    RESPONSE.error.message[0] = '\0';
    task_context->log_error = true;
    task_context->response_done = false;

    REQUEST.done = true;
    REQUEST.header.cmd = ((FDIRProtoHeader *)TASK->data)->cmd;
    REQUEST.header.body_len = TASK->length - sizeof(FDIRProtoHeader);
    REQUEST.body = TASK->data + sizeof(FDIRProtoHeader);

    if (TASK->nio_stage == SF_NIO_STAGE_FORWARDED) {
        TASK->nio_stage = SF_NIO_STAGE_SEND;
        REQUEST.forwarded = true;
    } else {
        REQUEST.forwarded = false;
    }
}

static inline int deal_task_done(ServerTaskContext *task_context)
{
    FDIRProtoHeader *proto_header;
    int r;
    int time_used;

    if (task_context->log_error && RESPONSE.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, cmd: %d, req body length: %d, %s",
                __LINE__, TASK->client_ip, REQUEST.header.cmd,
                REQUEST.header.body_len,
                RESPONSE.error.message);
    }

    if (RESPONSE_STATUS == 0 && !REQUEST.done) {
        return RESPONSE_STATUS;
    }

    proto_header = (FDIRProtoHeader *)TASK->data;
    if (!task_context->response_done) {
        RESPONSE.header.body_len = RESPONSE.error.length;
        if (RESPONSE.error.length > 0) {
            memcpy(TASK->data + sizeof(FDIRProtoHeader),
                    RESPONSE.error.message, RESPONSE.error.length);
        }
    }

    short2buff(RESPONSE_STATUS >= 0 ? RESPONSE_STATUS : -1 * RESPONSE_STATUS,
            proto_header->status);
    proto_header->cmd = RESPONSE.header.cmd;
    int2buff(RESPONSE.header.body_len, proto_header->body_len);
    TASK->length = sizeof(FDIRProtoHeader) + RESPONSE.header.body_len;

    r = sf_send_add_event(TASK);
    time_used = (int)(get_current_time_us() - TASK_ARG->req_start_time);
    if (time_used > 50 * 1000) {
        lwarning("process a request timed used: %d us, "
                "cmd: %d, req body len: %d, resp body len: %d",
                time_used, REQUEST.header.cmd,
                REQUEST.header.body_len,
                RESPONSE.header.body_len);
    }

    logDebug("file: "__FILE__", line: %d, thread: #%d, forwarded: %d, "
            "client ip: %s, req cmd: %d, req body_len: %d, "
            "resp cmd: %d, status: %d, resp body_len: %d, "
            "time used: %d us", __LINE__, SERVER_CONTEXT->thread_index,
            REQUEST.forwarded, TASK->client_ip, REQUEST.header.cmd,
            REQUEST.header.body_len, RESPONSE.header.cmd,
            RESPONSE_STATUS, RESPONSE.header.body_len, time_used);

    return r == 0 ? RESPONSE_STATUS : r;
}

int server_deal_task(struct fast_task_info *task)
{
    ServerTaskContext task_context;

    task_context.task = task;
    init_task_context(&task_context);

    do {
        switch (task_context.request.header.cmd) {
            case FDIR_PROTO_ACTIVE_TEST_REQ:
                task_context.response.header.cmd = FDIR_PROTO_ACTIVE_TEST_RESP;
                RESP_STATUS = server_deal_actvie_test(&task_context);
                break;
            case FDIR_PROTO_CREATE_DENTRY:
                RESP_STATUS = server_deal_create_dentry(&task_context);
                break;
            case FDIR_PROTO_REMOVE_DENTRY:
                RESP_STATUS = server_deal_remove_dentry(&task_context);
                break;
            case FDIR_PROTO_LIST_DENTRY_FIRST_REQ:
                RESP_STATUS = server_deal_list_dentry_first(&task_context);
                break;
            case FDIR_PROTO_LIST_DENTRY_NEXT_REQ:
                RESP_STATUS = server_deal_list_dentry_next(&task_context);
                break;
            case FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
                RESP_STATUS = server_deal_get_server_status(&task_context);
                break;
            case FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER:
            case FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER:
                RESP_STATUS = server_deal_next_master(&task_context);
                break;
            case FDIR_CLUSTER_PROTO_JOIN_MASTER:
                RESP_STATUS = server_deal_join_master(&task_context);
                break;
            case FDIR_CLUSTER_PROTO_PING_MASTER_REQ:
                RESP_STATUS = server_deal_ping_master(&task_context);
                break;
            default:
                task_context.response.error.length = sprintf(
                        task_context.response.error.message,
                        "unkown cmd: %d", task_context.request.header.cmd);
                RESP_STATUS = -EINVAL;
                break;
        }
    } while(0);

    return deal_task_done(&task_context);
}

void *server_alloc_thread_extra_data(const int thread_index)
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
    if ((dentry_init_context(server_context)) != 0) {
        free(server_context);
        return NULL;
    }

    if (fast_mblock_init(&server_context->delay_free_context.allocator,
                    sizeof(ServerDelayFreeNode), 16 * 1024) != 0)
    {
        free(server_context);
        return NULL;
    }

    server_context->thread_index = thread_index;
    return server_context;
}

static inline void add_to_delay_free_queue(ServerDelayFreeContext *pContext,
        ServerDelayFreeNode *node, void *ptr, const int delay_seconds)
{
    node->expires = g_current_time + delay_seconds;
    node->ptr = ptr;
    node->next = NULL;
    if (pContext->queue.head == NULL)
    {
        pContext->queue.head = node;
    }
    else
    {
        pContext->queue.tail->next = node;
    }
    pContext->queue.tail = node;
}

int server_add_to_delay_free_queue(ServerDelayFreeContext *pContext,
        void *ptr, server_free_func free_func, const int delay_seconds)
{
    ServerDelayFreeNode *node;

    node = (ServerDelayFreeNode *)fast_mblock_alloc_object(
            &pContext->allocator);
    if (node == NULL) {
        return ENOMEM;
    }

    node->free_func = free_func;
    node->free_func_ex = NULL;
    node->ctx = NULL;
    add_to_delay_free_queue(pContext, node, ptr, delay_seconds);
    return 0;
}

int server_add_to_delay_free_queue_ex(ServerDelayFreeContext *pContext,
        void *ptr, void *ctx, server_free_func_ex free_func_ex,
        const int delay_seconds)
{
    ServerDelayFreeNode *node;

    node = (ServerDelayFreeNode *)fast_mblock_alloc_object(
            &pContext->allocator);
    if (node == NULL) {
        return ENOMEM;
    }

    node->free_func = NULL;
    node->free_func_ex = free_func_ex;
    node->ctx = ctx;
    add_to_delay_free_queue(pContext, node, ptr, delay_seconds);
    return 0;
}

int server_thread_loop(struct nio_thread_data *thread_data)
{
    ServerDelayFreeContext *delay_context;
    ServerDelayFreeNode *node;
    ServerDelayFreeNode *deleted;

    delay_context = &((FDIRServerContext *)thread_data->arg)->
        delay_free_context;
    if (delay_context->last_check_time == g_current_time ||
            delay_context->queue.head == NULL)
    {
        return 0;
    }

    delay_context->last_check_time = g_current_time;
    node = delay_context->queue.head;
    while ((node != NULL) && (node->expires < g_current_time)) {
        if (node->free_func != NULL) {
            node->free_func(node->ptr);
            //logInfo("free ptr: %p", node->ptr);
        } else {
            node->free_func_ex(node->ctx, node->ptr);
            //logInfo("free ex func, ctx: %p, ptr: %p", node->ctx, node->ptr);
        }

        deleted = node;
        node = node->next;
        fast_mblock_free_object(&delay_context->allocator, deleted);
    }

    delay_context->queue.head = node;
    if (node == NULL) {
        delay_context->queue.tail = NULL;
    }

    return 0;
}

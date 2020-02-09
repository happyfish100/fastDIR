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
#include "server_handler.h"

int server_handler_init()
{
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

    __sync_add_and_fetch(&((FDIRServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int server_deal_actvie_test(ServerTaskContext *task_context)
{
    return server_expect_body_length(task_context, 0);
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

    logInfo("ns.len: %d, path.len: %d", path_info->ns.len, path_info->path.len);

    if (path_info->ns.len <= 0) {
        task_context->response.error.length = sprintf(
                task_context->response.error.message,
                "invalid namespace length: %d <= 0",
                path_info->ns.len);
        return EINVAL;
    }

    if (path_info->path.len <= 0) {
        task_context->response.error.length = sprintf(
                task_context->response.error.message,
                "invalid path length: %d <= 0",
                path_info->path.len);
        return EINVAL;
    }
    if (path_info->path.len > PATH_MAX) {
        task_context->response.error.length = sprintf(
                task_context->response.error.message,
                "invalid path length: %d > %d",
                path_info->path.len, PATH_MAX);
        return EINVAL;
    }

    if (path_info->path.str[0] != '/') {
        task_context->response.error.length = snprintf(
                task_context->response.error.message,
                sizeof(task_context->response.error.message),
                "invalid path: %.*s", path_info->path.len,
                path_info->path.str);
        return EINVAL;
    }

    path_info->count = split_string_ex(&path_info->path, '/',
        path_info->paths, FDIR_MAX_PATH_COUNT, true);
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
    logInfo("logic_path for hash code: %.*s", len, logic_path);
    return simple_hash(logic_path, len);
}

#define server_get_parent_hashcode(path_info)  \
    server_get_dentry_hashcode(path_info, false)

#define server_get_my_hashcode(path_info)  \
    server_get_dentry_hashcode(path_info, true)

static int server_deal_create_dentry(ServerTaskContext *task_context)
{
    int result;
    int body_len;
    char *body;
    FDIRServerContext *server_context;
    FDIRServerTaskArg *task_arg;
    FDIRProtoCreateDEntryFront *proto_front;
    unsigned int hash_code;
    unsigned int thread_index;
    int flags;
    int mode;

    server_context = (FDIRServerContext *)task_context->task->thread_data->arg;
    task_arg = (FDIRServerTaskArg *)task_context->task->arg;
    body = task_context->task->data + sizeof(FDIRProtoHeader);

    if (task_context->task->nio_stage == SF_NIO_STAGE_FORWARDED) {
        task_context->task->nio_stage = SF_NIO_STAGE_SEND;
    } else {
        if ((result=server_check_body_length(task_context,
                        sizeof(FDIRProtoCreateDEntryBody) + 1,
                        sizeof(FDIRProtoCreateDEntryBody) +
                        NAME_MAX + PATH_MAX)) != 0)
        {
            return result;
        }

        if ((result=server_parse_dentry_info(task_context,
                        body + sizeof(FDIRProtoCreateDEntryFront),
                        &task_arg->path_info)) != 0)
        {
            return result;
        }
         
        body_len = sizeof(FDIRProtoCreateDEntryBody) + task_arg->
            path_info.ns.len + task_arg->path_info.path.len;
        if (body_len != task_context->request.header.body_len)
        {
            task_context->response.error.length = sprintf(
                    task_context->response.error.message,
                    "body length: %d != expect: %d",
                    task_context->request.header.body_len, body_len);
            return EINVAL;
        }

        hash_code = server_get_parent_hashcode(&task_arg->path_info);
        thread_index = hash_code % g_sf_global_vars.work_threads;

        logInfo("hash_code: %u, thread_index: %d, current thread_index: %d",
                hash_code, thread_index, server_context->thread_index);

        if (thread_index != server_context->thread_index) {
            return sf_nio_notify(task_context->task, SF_NIO_STAGE_FORWARDED);
        }
    }

    proto_front = (FDIRProtoCreateDEntryFront *)body;
    flags = buff2int(proto_front->flags);
    mode = buff2int(proto_front->mode);
    return dentry_create(server_context, &task_arg->path_info, flags, mode);
}

int server_deal_task(struct fast_task_info *task)
{
    FDIRProtoHeader *proto_header;
    FDIRServerTaskArg *task_arg;
    ServerTaskContext task_context;
    int result;
    int r;
    int time_used;

    task_arg = (FDIRServerTaskArg *)task->arg;
    if (task->nio_stage != SF_NIO_STAGE_FORWARDED) {
        task_arg->req_start_time = get_current_time_us();
    }
    task_context.task = task;
    task_context.response.header.cmd = FDIR_PROTO_ACK;
    task_context.response.header.body_len = 0;
    task_context.response.error.length = 0;
    task_context.response.error.message[0] = '\0';
    task_context.log_error = true;
    task_context.response_done = false;

    task_context.request.header.cmd = ((FDIRProtoHeader *)task->data)->cmd;
    task_context.request.header.body_len = task->length - sizeof(FDIRProtoHeader);
    do {
        switch (task_context.request.header.cmd) {
            case FDIR_PROTO_ACTIVE_TEST_REQ:
                task_context.response.header.cmd = FDIR_PROTO_ACTIVE_TEST_RESP;
                result = server_deal_actvie_test(&task_context);
                break;
            case FDIR_PROTO_CREATE_DENTRY:
                result = server_deal_create_dentry(&task_context);
                break;
            default:
                task_context.response.error.length = sprintf(
                        task_context.response.error.message,
                        "unkown cmd: %d", task_context.request.header.cmd);
                result = -EINVAL;
                break;
        }
    } while(0);

    if (task_context.log_error && task_context.response.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, cmd: %d, req body length: %d, %s",
                __LINE__, task->client_ip, task_context.request.header.cmd,
                task_context.request.header.body_len,
                task_context.response.error.message);
    }

    if (task->nio_stage == SF_NIO_STAGE_FORWARDED) {
        return result;
    }

    proto_header = (FDIRProtoHeader *)task->data;
    if (!task_context.response_done) {
        task_context.response.header.body_len =
            task_context.response.error.length;
        if (task_context.response.error.length > 0) {
            memcpy(task->data + sizeof(FDIRProtoHeader),
                    task_context.response.error.message,
                    task_context.response.error.length);
        }
    }

    proto_header->status = result >= 0 ? result : -1 * result;
    proto_header->cmd = task_context.response.header.cmd;
    int2buff(task_context.response.header.body_len, proto_header->body_len);
    task->length = sizeof(FDIRProtoHeader) +
        task_context.response.header.body_len;

    r = sf_send_add_event(task);
    time_used = (int)(get_current_time_us() - task_arg->req_start_time);
    if (time_used > 50 * 1000) {
        lwarning("process a request timed used: %d us, "
                "cmd: %d, req body len: %d, resp body len: %d",
                time_used, task_context.request.header.cmd,
                task_context.request.header.body_len,
                task_context.response.header.body_len);
    }

    ldebug("client ip: %s, req cmd: %d, req body_len: %d, "
            "resp cmd: %d, status: %d, resp body_len: %d, "
            "time used: %d us",  task->client_ip,
            task_context.request.header.cmd,
            task_context.request.header.body_len,
            task_context.response.header.cmd, proto_header->status,
            task_context.response.header.body_len, time_used);

    return r == 0 ? result : r;
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
    if ((dentry_init_context(&server_context->dentry_context)) != 0) {
        free(server_context);
        return NULL;
    }

    if (fast_mblock_init(&server_context->delay_free_context.allocator,
                    sizeof(SkiplistDelayFreeNode), 16 * 1024) != 0)
    {
        free(server_context);
        return NULL;
    }

    server_context->thread_index = thread_index;
    return server_context;
}

int server_add_to_delay_free_queue(SkiplistDelayFreeContext *pContext,
        UniqSkiplist *skiplist, const int delay_seconds)
{
    SkiplistDelayFreeNode *node;

    node = (SkiplistDelayFreeNode *)fast_mblock_alloc_object(
            &pContext->allocator);
    if (node == NULL) {
        return ENOMEM;
    }

    node->expires = g_current_time + delay_seconds;
    node->skiplist = skiplist;
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
    return 0;
}

int server_thread_loop(struct nio_thread_data *thread_data)
{
    SkiplistDelayFreeContext *delay_context;
    SkiplistDelayFreeNode *node;
    SkiplistDelayFreeNode *deleted;

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
        uniq_skiplist_free(node->skiplist);

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

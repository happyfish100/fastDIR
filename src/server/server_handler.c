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

int server_deal_task(struct fast_task_info *task)
{
    FDIRProtoHeader *proto_header;
    FDIRServerTaskArg *task_arg;
    FDIRRequestInfo request;
    FDIRResponseInfo response;
    int result;
    int r;
    int64_t tbegin;
    int time_used;

    tbegin = get_current_time_ms();
    response.cmd = FDIR_PROTO_ACK;
    response.body_len = 0;
    response.log_error = true;
    response.error.length = 0;
    response.error.message[0] = '\0';
    response.response_done = false;

    task_arg = (FDIRServerTaskArg *)task->arg;
    request.cmd = ((FDIRProtoHeader *)task->data)->cmd;
    request.body_len = task->length - sizeof(FDIRProtoHeader);
    do {
        switch (request.cmd) {
            case FDIR_PROTO_ACTIVE_TEST_REQ:
                response.cmd = FDIR_PROTO_ACTIVE_TEST_RESP;
                result = fdir_proto_deal_actvie_test(task, &request, &response);
                break;
            default:
                response.error.length = sprintf(response.error.message,
                    "unkown cmd: %d", request.cmd);
                result = -EINVAL;
                break;
        }
    } while(0);

    if (response.log_error && response.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, cmd: %d, body length: %d, %s", __LINE__,
                task->client_ip, request.cmd, request.body_len,
                response.error.message);
    }

    if (request.cmd == FDIR_PROTO_PUSH_RESP ||
            request.cmd == FDIR_PROTO_ACTIVE_TEST_RESP)
    {
        return result > 0 ? -1 * result : result;
    }

    proto_header = (FDIRProtoHeader *)task->data;
    if (!response.response_done) {
        response.body_len = response.error.length;
        if (response.error.length > 0) {
            memcpy(task->data + sizeof(FDIRProtoHeader),
                    response.error.message, response.error.length);
        }
    }

    proto_header->status = result >= 0 ? result : -1 * result;
    proto_header->cmd = response.cmd;
    int2buff(response.body_len, proto_header->body_len);
    task->length = sizeof(FDIRProtoHeader) + response.body_len;

    r = sf_send_add_event(task);
    time_used = (int)(get_current_time_ms() - tbegin);
    if (time_used > 1000) {
        lwarning("timed used to process a request is %d ms, "
                "cmd: %d, req body len: %d, resp body len: %d",
                time_used, request.cmd,
                request.body_len, response.body_len);
    }

    ldebug("client ip: %s, req cmd: %d, req body_len: %d, "
            "resp cmd: %d, status: %d, resp body_len: %d, "
            "time used: %d ms",  task->client_ip,
            request.cmd, request.body_len,
            response.cmd, proto_header->status,
            response.body_len, time_used);

    return r == 0 ? result : r;
}

void *server_alloc_thread_extra_data(const int thread_index)
{
    FDIRServerContext *thread_extra_data;

    thread_extra_data = (FDIRServerContext *)malloc(sizeof(FDIRServerContext));
    if (thread_extra_data == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail, errno: %d, error info: %s",
                __LINE__, (int)sizeof(FDIRServerContext),
                errno, strerror(errno));
        return NULL;
    }

    memset(thread_extra_data, 0, sizeof(FDIRServerContext));
    if ((dentry_init_context(&thread_extra_data->dentry_context)) != 0) {
        free(thread_extra_data);
        return NULL;
    }

    if (fast_mblock_init(&thread_extra_data->delay_free_context.allocator,
                    sizeof(SkiplistDelayFreeNode), 16 * 1024) != 0)
    {
        free(thread_extra_data);
        return NULL;
    }

    return thread_extra_data;
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

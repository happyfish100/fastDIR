#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "server_global.h"
#include "dentry.h"
#include "data_thread.h"

FDIRDataThreadVariables g_data_thread_vars;
static volatile int running_thread_count = 0;
static void *data_thread_func(void *arg);

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

static int deal_delay_free_queque(FDIRDataThreadContext *thread_ctx)
{
    ServerDelayFreeContext *delay_context;
    ServerDelayFreeNode *node;
    ServerDelayFreeNode *deleted;

    delay_context = &thread_ctx->delay_free_context;
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
            logInfo("free ptr: %p", node->ptr);
        } else {
            node->free_func_ex(node->ctx, node->ptr);
            logInfo("free ex func, ctx: %p, ptr: %p", node->ctx, node->ptr);
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

static int init_thread_ctx(FDIRDataThreadContext *context)
{
    int result;
    if ((result=dentry_init_context(context)) != 0) {
        return result;
    }

    if ((result=fast_mblock_init(&context->delay_free_context.allocator,
                    sizeof(ServerDelayFreeNode), 16 * 1024)) != 0)
    {
        return result;
    }

    if ((result=common_blocked_queue_init_ex(&context->queue, 4096)) != 0) {
        return result;
    }
    return 0;
}

static int init_data_thread_array()
{
    int result;
    int bytes;
    FDIRDataThreadContext *context;
    FDIRDataThreadContext *end;

    bytes = sizeof(FDIRDataThreadContext) * DATA_THREAD_COUNT;
    g_data_thread_vars.thread_array.contexts =
        (FDIRDataThreadContext *)malloc(bytes);
    if (g_data_thread_vars.thread_array.contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(g_data_thread_vars.thread_array.contexts, 0, bytes);

    end = g_data_thread_vars.thread_array.contexts + DATA_THREAD_COUNT;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<end; context++)
    {
        if ((result=init_thread_ctx(context)) != 0) {
            return result;
        }
    }
    g_data_thread_vars.thread_array.count = DATA_THREAD_COUNT;
    return 0;
}

int data_thread_init()
{
    int result;
    int count;

    if ((result=init_data_thread_array()) != 0) {
        return result;
    }

    g_data_thread_vars.error_mode = FDIR_DATA_ERROR_MODE_LOOSE;
    count = g_data_thread_vars.thread_array.count;
    if ((result=create_work_threads_ex(&count, data_thread_func,
            g_data_thread_vars.thread_array.contexts,
            sizeof(FDIRDataThreadContext), NULL,
            SF_G_THREAD_STACK_SIZE)) == 0)
    {
        count = 0;
        while (__sync_add_and_fetch(&running_thread_count, 0) <
                g_data_thread_vars.thread_array.count && count++ < 100)
        {
            usleep(1000);
        }
    }
    return result;
}

void data_thread_destroy()
{
    if (g_data_thread_vars.thread_array.contexts != NULL) {
        FDIRDataThreadContext *context;
        FDIRDataThreadContext *end;

        end = g_data_thread_vars.thread_array.contexts +
            g_data_thread_vars.thread_array.count;
        for (context=g_data_thread_vars.thread_array.contexts;
                context<end; context++)
        {
            common_blocked_queue_destroy(&context->queue);
        }
        free(g_data_thread_vars.thread_array.contexts);
        g_data_thread_vars.thread_array.contexts = NULL;
    }
}

void data_thread_terminate()
{
    FDIRDataThreadContext *context;
    FDIRDataThreadContext *end;
    int count;

    end = g_data_thread_vars.thread_array.contexts +
        g_data_thread_vars.thread_array.count;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<end; context++)
    {
        common_blocked_queue_terminate(&context->queue);
    }

    count = 0;
    while (__sync_add_and_fetch(&running_thread_count, 0) != 0 &&
            count++ < 100)
    {
        usleep(1000);
    }
}

#define IGNORE_ERROR_BY_MODE(result, ignore_errno) \
    do { \
        if ((result == ignore_errno) && (g_data_thread_vars.error_mode == \
                FDIR_DATA_ERROR_MODE_LOOSE))  \
        {  \
            result = 0;  \
        }  \
    } while (0)

static int deal_binlog_one_record(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result = 0;

    /*
    logInfo("file: "__FILE__", line: %d, record: %p, "
            "operation: %d, hash code: %u, inode: %"PRId64", data_version: %"PRId64,
            __LINE__, record, record->operation, record->hash_code, record->inode, record->data_version);
    */
    switch (record->operation) {
        case BINLOG_OP_CREATE_DENTRY_INT:
            result = dentry_create(thread_ctx, record);
            IGNORE_ERROR_BY_MODE(result, EEXIST);
            break;
        case BINLOG_OP_REMOVE_DENTRY_INT:
            result = dentry_remove(thread_ctx, record);
            IGNORE_ERROR_BY_MODE(result, ENOENT);
            break;
        case BINLOG_OP_RENAME_DENTRY_INT:
            break;
        case BINLOG_OP_UPDATE_DENTRY_INT:
            break;
        default:
            break;
    }

    if (result == 0) {
        if (record->data_version == 0) {
            record->data_version = __sync_add_and_fetch(
                    &DATA_CURRENT_VERSION, 1);
        } else {
            int64_t old_version;
            old_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 0);
            if (record->data_version > old_version) {
                __sync_bool_compare_and_swap(&DATA_CURRENT_VERSION,
                        old_version, record->data_version);
            }
        }
    }

    if (record->notify.func != NULL) {
        record->notify.func(result, record);
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "result: %d, data_version: %"PRId64,
            __LINE__, result, record->data_version);
            */
    return result;
}

static void deal_binlog_records(FDIRDataThreadContext *thread_ctx,
        struct common_blocked_node *node)
{
    FDIRBinlogRecord *record;

    do {
        record = (FDIRBinlogRecord *)node->data;
        deal_binlog_one_record(thread_ctx, record);

        node = node->next;
    } while (node != NULL);
}

static void *data_thread_func(void *arg)
{
    struct common_blocked_queue *queue;
    struct common_blocked_node *node;
    FDIRDataThreadContext *thread_ctx;

    __sync_add_and_fetch(&running_thread_count, 1);
    thread_ctx = (FDIRDataThreadContext *)arg;
    queue = &thread_ctx->queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(queue);
        if (node == NULL) {
            continue;
        }

        deal_binlog_records(thread_ctx, node);
        common_blocked_queue_free_all_nodes(queue, node);

        deal_delay_free_queque(thread_ctx);
    }
    __sync_sub_and_fetch(&running_thread_count, 1);
    return NULL;
}

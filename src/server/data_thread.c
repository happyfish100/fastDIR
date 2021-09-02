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
#include "sf/sf_func.h"
#include "server_global.h"
#include "dentry.h"
#include "inode_index.h"
#include "db_updater.h"
#include "data_thread.h"

#define DATA_THREAD_RUNNING_COUNT g_data_thread_vars.running_count

FDIRDataThreadVariables g_data_thread_vars = {{NULL, 0}, 0, 0};
static void *data_thread_func(void *arg);

void data_thread_sum_counters(FDIRDentryCounters *counters)
{
    FDIRDataThreadContext *context;
    FDIRDataThreadContext *end;

    counters->ns = 0;
    counters->dir = 0;
    counters->file = 0;
    end = g_data_thread_vars.thread_array.contexts +
        g_data_thread_vars.thread_array.count;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<end; context++)
    {
        counters->ns += context->dentry_context.counters.ns;
        counters->dir += context->dentry_context.counters.dir;
        counters->file += context->dentry_context.counters.file;
    }
}

static inline void add_to_delay_free_queue(ServerDelayFreeContext *pContext,
        ServerDelayFreeNode *node, void *ptr, const int delay_seconds)
{
    node->expires = g_current_time + delay_seconds;
    node->ptr = ptr;
    node->next = NULL;
    if (pContext->queue.head == NULL) {
        pContext->queue.head = node;
    } else {
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
        void *ctx, void *ptr, server_free_func_ex free_func_ex,
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
        } else {
            node->free_func_ex(node->ctx, node->ptr);
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

    if ((result=fast_mblock_init_ex1(&context->delay_free_context.allocator,
                    "delay_free_node", sizeof(ServerDelayFreeNode), 16 * 1024,
                    0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&context->queue, (long)
                    (&((FDIRBinlogRecord *)NULL)->next))) != 0)
    {
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
        (FDIRDataThreadContext *)fc_malloc(bytes);
    if (g_data_thread_vars.thread_array.contexts == NULL) {
        return ENOMEM;
    }
    memset(g_data_thread_vars.thread_array.contexts, 0, bytes);

    end = g_data_thread_vars.thread_array.contexts + DATA_THREAD_COUNT;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<end; context++)
    {
        context->index = context - g_data_thread_vars.thread_array.contexts;
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
        while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) <
                g_data_thread_vars.thread_array.count && count++ < 100)
        {
            fc_sleep_ms(1);
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
            fc_queue_destroy(&context->queue);
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
        fc_queue_terminate(&context->queue);
    }

    count = 0;
    while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) != 0 &&
            count++ < 100)
    {
        fc_sleep_ms(1);
    }
}

static inline int check_parent(FDIRBinlogRecord *record)
{
    if (record->me.pname.parent_inode == 0) {
        return 0;
    }

    record->me.parent = inode_index_get_dentry(record->
            me.pname.parent_inode);
    return record->me.parent != NULL ? 0 : ENOENT;
}

static inline int set_hdlink_src_dentry(FDIRBinlogRecord *record)
{
    if ((record->hdlink.src_dentry=inode_index_get_dentry(
                    record->hdlink.src_inode)) == NULL)
    {
        return ENOENT;
    }

    if (S_ISDIR(record->hdlink.src_dentry->stat.mode) ||
            FDIR_IS_DENTRY_HARD_LINK(record->hdlink.src_dentry->stat.mode))
    {
        return EPERM;
    }

    return 0;
}

static inline int deal_record_rename_op(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    if ((record->rename.src.parent=inode_index_get_dentry(record->
                    rename.src.pname.parent_inode)) == NULL)
    {
        return ENOENT;
    }

    if ((record->rename.dest.parent=inode_index_get_dentry(record->
                    rename.dest.pname.parent_inode)) == NULL)
    {
        return ENOENT;
    }

    return dentry_rename(thread_ctx, record);
}

static int push_to_update_queue(FDIRBinlogRecord *record)
{
    if (record->removed.count > 0) {
    //record->removed.dentries[record->removed.count
    }
    return 0;
}

static int deal_binlog_one_record(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    int ignore_errno;
    bool set_data_verson;
    bool is_error;

    record->removed.count = 0;
    switch (record->operation) {
        case BINLOG_OP_CREATE_DENTRY_INT:
        case BINLOG_OP_REMOVE_DENTRY_INT:
            if ((result=check_parent(record)) != 0) {
                ignore_errno = 0;
                break;
            }
            if (record->operation == BINLOG_OP_CREATE_DENTRY_INT) {
                if (FDIR_IS_DENTRY_HARD_LINK(record->stat.mode)) {
                    if ((result=set_hdlink_src_dentry(record)) != 0) {
                        ignore_errno = 0;
                        break;
                    }
                }
                result = dentry_create(thread_ctx, record);
                ignore_errno = EEXIST;
            } else {
                result = dentry_remove(thread_ctx, record);
                ignore_errno = ENOENT;
            }
            break;
        case BINLOG_OP_RENAME_DENTRY_INT:
            ignore_errno = 0;
            result = deal_record_rename_op(thread_ctx, record);
            break;
        case BINLOG_OP_UPDATE_DENTRY_INT:
            record->me.dentry = inode_index_update_dentry(record);
            result = (record->me.dentry != NULL) ? 0 : ENOENT;
            ignore_errno = 0;
            break;
        case BINLOG_OP_SET_XATTR_INT:
            record->me.dentry = inode_index_set_xattr(
                    record, &result);
            ignore_errno = 0;
            break;
        case BINLOG_OP_REMOVE_XATTR_INT:
            record->me.dentry = inode_index_remove_xattr(
                    record->inode, &record->xattr.key, &result);
            ignore_errno = ENODATA;
            break;
        default:
            ignore_errno = 0;
            result = 0;
            break;
    }

    if (result == 0) {
        if (record->data_version == 0) {
            record->data_version = __sync_add_and_fetch(
                    &DATA_CURRENT_VERSION, 1);
            set_data_verson = false;
        } else {
            set_data_verson = true;
        }
        is_error = false;
    } else {
        set_data_verson = record->data_version > 0;
        is_error = !((result == ignore_errno) &&
                (g_data_thread_vars.error_mode ==
                 FDIR_DATA_ERROR_MODE_LOOSE));
    }

    if (set_data_verson && !is_error) {
        int64_t old_version;
        old_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 0);
        if (record->data_version > old_version) {
            __sync_bool_compare_and_swap(&DATA_CURRENT_VERSION,
                    old_version, record->data_version);
        }
    }

    if (result == 0 && STORAGE_ENABLED) {
        if (record->data_version > thread_ctx->last_data_version) {
            thread_ctx->last_data_version = record->data_version;
        }

        if (record->type == fdir_record_type_update) {
            if ((result=push_to_update_queue(record)) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "push_to_update_queue fail, "
                        "program exit!", __LINE__);
                sf_terminate_myself();
            }
        }
    }

    if (record->notify.func != NULL) {
        record->notify.func(record, result, is_error);
    }

    /*
    logInfo("file: "__FILE__", line: %d, record: %p, "
            "operation: %d, hash code: %u, inode: %"PRId64
             ", data_version: %"PRId64", result: %d, is_error: %d",
             __LINE__, record, record->operation, record->hash_code,
             record->inode, record->data_version, result, is_error);
             */

    return result;
}

static void *data_thread_func(void *arg)
{
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *current;
    FDIRDataThreadContext *thread_ctx;

    __sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    thread_ctx = (FDIRDataThreadContext *)arg;

#ifdef OS_LINUX
    {
        char thread_name[16];
        snprintf(thread_name, sizeof(thread_name),
                "data[%d]", thread_ctx->index);
        prctl(PR_SET_NAME, thread_name);
    }
#endif

    while (SF_G_CONTINUE_FLAG) {
        record = (FDIRBinlogRecord *)fc_queue_pop_all(&thread_ctx->queue);
        if (record == NULL) {
            continue;
        }

        do {
            current = record;
            record = record->next;
            deal_binlog_one_record(thread_ctx, current);
        } while (record != NULL);

        deal_delay_free_queque(thread_ctx);
    }
    __sync_sub_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    return NULL;
}

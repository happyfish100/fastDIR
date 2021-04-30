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

//data_thread.h

#ifndef _DATA_THREAD_H_
#define _DATA_THREAD_H_

#include "fastcommon/fc_queue.h"
#include "fastcommon/server_id_func.h"
#include "common/fdir_types.h"
#include "binlog/binlog_types.h"

#define FDIR_DATA_ERROR_MODE_STRICT   1   //for master update operations
#define FDIR_DATA_ERROR_MODE_LOOSE    2   //for data load or binlog replication

typedef struct fdir_dentry_counters {
    int64_t ns;
    int64_t dir;
    int64_t file;
} FDIRDentryCounters;

struct fdir_data_thread_context;
typedef struct fdir_dentry_context {
    UniqSkiplistFactory factory;
    struct fast_mblock_man dentry_allocator;
    struct fast_mblock_man kvarray_allocators[FDIR_XATTR_KVARRAY_ALLOCATOR_COUNT];
    struct fast_allocator_context name_acontext;
    struct fdir_data_thread_context *db_context;
    FDIRDentryCounters counters;
} FDIRDentryContext;

typedef struct server_delay_free_node {
    int expires;
    void *ctx;     //the context
    void *ptr;     //ptr to free
    server_free_func free_func;
    server_free_func_ex free_func_ex;
    struct server_delay_free_node *next;
} ServerDelayFreeNode;

typedef struct server_delay_free_queue {
    ServerDelayFreeNode *head;
    ServerDelayFreeNode *tail;
} ServerDelayFreeQueue;

typedef struct server_delay_free_context {
    time_t last_check_time;
    ServerDelayFreeQueue queue;
    struct fast_mblock_man allocator;
} ServerDelayFreeContext;

typedef struct fdir_data_thread_context {
    int index;
    struct fc_queue queue;
    FDIRDentryContext dentry_context;
    ServerDelayFreeContext delay_free_context;
} FDIRDataThreadContext;

typedef struct fdir_data_thread_array {
    FDIRDataThreadContext *contexts;
    int count;
} FDIRDataThreadArray;

typedef struct fdir_data_thread_variables {
    FDIRDataThreadArray thread_array;
    volatile int running_count;
    int error_mode;
} FDIRDataThreadVariables;

#define dentry_strdup(context, dest, src) \
    fast_allocator_alloc_string(&(context)->name_acontext, dest, src)

#define dentry_strfree(context, s) \
    fast_allocator_free(&(context)->name_acontext, (s)->str)

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRDataThreadVariables g_data_thread_vars;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    void data_thread_sum_counters(FDIRDentryCounters *counters);

    int server_add_to_delay_free_queue(ServerDelayFreeContext *pContext,
            void *ptr, server_free_func free_func, const int delay_seconds);

    int server_add_to_delay_free_queue_ex(ServerDelayFreeContext *pContext,
            void *ctx, void *ptr, server_free_func_ex free_func_ex,
            const int delay_seconds);

    static inline void server_delay_free_str(FDIRDentryContext
            *context, char *str)
    {
        server_add_to_delay_free_queue_ex(&context->db_context->
                delay_free_context, &context->name_acontext, str,
                (server_free_func_ex)fast_allocator_free,
                FDIR_DELAY_FREE_SECONDS);
    }

    static inline void push_to_data_thread_queue(FDIRBinlogRecord *record)
    {
        FDIRDataThreadContext *context;
        context = g_data_thread_vars.thread_array.contexts +
            record->hash_code % g_data_thread_vars.thread_array.count;
        fc_queue_push(&context->queue, record);
    }

#ifdef __cplusplus
}
#endif

#endif

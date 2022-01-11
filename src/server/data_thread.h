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
#include "fastcommon/fast_mblock.h"
#include "fastcommon/server_id_func.h"
#include "sf/sf_serializer.h"
#include "diskallocator/dio/trunk_read_thread.h"
#include "common/fdir_types.h"
#include "binlog/binlog_types.h"
#include "server_global.h"

#define FDIR_DATA_ERROR_MODE_STRICT   1   //for master update operations
#define FDIR_DATA_ERROR_MODE_LOOSE    2   //for data load or binlog replication

#define FDIR_DATA_DUMP_STATUS_NONE     0
#define FDIR_DATA_DUMP_STATUS_DUMPING  1
#define FDIR_DATA_DUMP_STATUS_DONE     2

typedef struct fdir_dentry_counters {
    int64_t ns;
    int64_t dir;
    int64_t file;
} FDIRDentryCounters;

struct fdir_server_dentry;
struct fdir_data_thread_context;

typedef struct fdir_dentry_pool {
    struct fdir_data_thread_context *thread_ctx;
    struct fast_mblock_chain chain;
    int limit;
    int count;
} FDIRDentryPool;

typedef struct fdir_dentry_context {
    UniqSkiplistFactory factory;
    struct {
        FDIRDentryPool local_alloc;
        FDIRDentryPool batch_free;
    } pools;
    struct fast_mblock_man kvarray_allocators[FDIR_XATTR_KVARRAY_ALLOCATOR_COUNT];
    struct fast_allocator_context name_acontext;
    struct fdir_data_thread_context *thread_ctx;
    FDIRDentryCounters counters;
} FDIRDentryContext;

typedef struct server_immediate_free_node {
    void *ctx;     //the context
    void *ptr;     //ptr to free
    server_free_func free_func;
    server_free_func_ex free_func_ex;
    struct server_immediate_free_node *next;
} ServerImmediateFreeNode;

typedef struct server_immediate_free_context {
    volatile int waiting_count;
    struct fc_queue queue;
} ServerImmediateFreeContext;

typedef struct server_free_context {
    struct fast_mblock_man allocator;
    ServerImmediateFreeContext immediate;
} ServerFreeContext;

typedef struct fdir_db_fetch_context {
    DASynchronizedReadContext read_ctx;
    SFSerializerIterator it;
} FDIRDBFetchContext;

typedef struct fdir_db_lru_context {
    volatile int64_t total_count;
    volatile int64_t target_reclaims;
    int64_t reclaim_count;
    struct fc_list_head head;  //for dentry LRU elimination
} FDIRDBLRUContext;

typedef struct fdir_data_thread_context {
    int index;
    struct {
        volatile int waiting_records;
        volatile int64_t last_version;
    } update_notify; //for data persistency
    struct fc_queue queue;
    FDIRDentryContext dentry_context;
    ServerFreeContext free_context;

    struct fdir_server_dentry *delay_free_head;

    /* following fields for storage engine */
    FDIRDBFetchContext db_fetch_ctx;
    struct {
        struct fast_mblock_man allocator;
        struct fast_mblock_chain chain;        //for batch free event
        struct fdir_data_thread_context *next; //for batch free event
    } event;  //for change notify when data persistency

    FDIRDBLRUContext lru_ctx;

#ifdef FDIR_DUMP_DATA_FOR_DEBUG
    volatile bool dump_flag;
#endif

} FDIRDataThreadContext;

typedef struct fdir_data_thread_array {
    FDIRDataThreadContext *contexts;
    int count;
} FDIRDataThreadArray;

typedef struct fdir_data_thread_variables {
    FDIRDataThreadArray thread_array;
    volatile int running_count;
    int error_mode;

    FDIRDataThreadContext *thread_end;
    struct fast_mblock_man dentry_allocator;
    struct {
        volatile int64_t current_id;
        int alloc_elements_once;
        int alloc_elements_limit;
    } event;  //for storage engine

#ifdef FDIR_DUMP_DATA_FOR_DEBUG
    volatile int dump_status;
    volatile int dump_threads;
    int64_t dump_start_time_ms;
#endif

} FDIRDataThreadVariables;

#define dentry_strdup(context, dest, src) \
    fast_allocator_alloc_string(&(context)->name_acontext, dest, src)

#define dentry_strfree(context, s) \
    fast_allocator_free(&(context)->name_acontext, (s)->str)

#define DATA_THREAD_LAST_VERSION  update_notify.last_version

#define EVENT_ALLOC_ELEMENTS_ONCE  g_data_thread_vars.event.alloc_elements_once
#define EVENT_ALLOC_ELEMENTS_LIMIT g_data_thread_vars.event.alloc_elements_limit
#define DATA_THREAD_END    g_data_thread_vars.thread_end
#define DENTRY_ALLOCATOR   g_data_thread_vars.dentry_allocator
#define TOTAL_DENTRY_COUNT DENTRY_ALLOCATOR.info.element_used_count
#define DATA_DUMP_STATUS   g_data_thread_vars.dump_status
#define DATA_DUMP_THREADS  g_data_thread_vars.dump_threads
#define DUMP_START_TIME_MS g_data_thread_vars.dump_start_time_ms

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRDataThreadVariables g_data_thread_vars;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    void data_thread_sum_counters(FDIRDentryCounters *counters);

    static inline void server_delay_free_dentry(FDIRDataThreadContext
            *thread_ctx, struct fdir_server_dentry *dentry)
    {
        dentry->free_next = thread_ctx->delay_free_head;
        thread_ctx->delay_free_head = dentry;
    }

    int server_add_to_immediate_free_queue_ex(ServerFreeContext *free_ctx,
            void *ctx, void *ptr, server_free_func_ex free_func_ex);

    int server_add_to_immediate_free_queue(ServerFreeContext *free_ctx,
            void *ptr, server_free_func free_func);

    static inline void server_immediate_free_str(FDIRDentryContext
            *context, char *str)
    {
        server_add_to_immediate_free_queue_ex(&context->thread_ctx->
                free_context, &context->name_acontext, str,
                (server_free_func_ex)fast_allocator_free);
    }

    static inline FDIRDataThreadContext *get_data_thread_context(
            const unsigned int hash_code)
    {
        return g_data_thread_vars.thread_array.contexts +
            hash_code % g_data_thread_vars.thread_array.count;
    }

    static inline void set_data_thread_index(FDIRBinlogRecord *record)
    {
        record->extra.data_thread_index = record->hash_code %
            g_data_thread_vars.thread_array.count;
    }

    static inline void push_to_data_thread_queue(FDIRBinlogRecord *record)
    {
        FDIRDataThreadContext *context;

        context = g_data_thread_vars.thread_array.contexts +
            record->hash_code % g_data_thread_vars.thread_array.count;
        if (STORAGE_ENABLED && record->record_type == fdir_record_type_update) {
            __sync_add_and_fetch(&context->update_notify.waiting_records, 1);
        }
        fc_queue_push(&context->queue, record);
    }

    static inline int64_t data_thread_get_last_data_version()
    {
        FDIRDataThreadContext *ctx;
        int64_t min_version;
        int64_t max_version;

        min_version = INT64_MAX;
        max_version = 0;
        for (ctx=g_data_thread_vars.thread_array.contexts;
                ctx<DATA_THREAD_END; ctx++)
        {
            if (__sync_add_and_fetch(&ctx->update_notify.
                        waiting_records, 0) > 0)
            {
                if (min_version > ctx->DATA_THREAD_LAST_VERSION) {
                    min_version = ctx->DATA_THREAD_LAST_VERSION;
                }
            } else {
                if (max_version < ctx->DATA_THREAD_LAST_VERSION) {
                    max_version = ctx->DATA_THREAD_LAST_VERSION;
                }
            }
        }

        return (min_version != INT64_MAX ?  min_version : max_version);
    }

    static inline int init_db_fetch_context(FDIRDBFetchContext *db_fetch_ctx)
    {
        int result;
        if ((result=da_init_read_context(&db_fetch_ctx->read_ctx)) != 0) {
            return result;
        }
        sf_serializer_iterator_init(&db_fetch_ctx->it);
        return 0;
    }

#ifdef FDIR_DUMP_DATA_FOR_DEBUG
    static inline int data_thread_set_dump_flag()
    {
        FDIRDataThreadContext *context;

        if (FC_ATOMIC_GET(DATA_DUMP_STATUS) ==
                FDIR_DATA_DUMP_STATUS_DUMPING)
        {
            logError("file: "__FILE__", line: %d, "
                    "already in progress.", __LINE__);
            return EINPROGRESS;
        }

        __sync_add_and_fetch(&DATA_DUMP_THREADS,
                g_data_thread_vars.thread_array.count);
        for (context=g_data_thread_vars.thread_array.contexts;
                context<DATA_THREAD_END; context++)
        {
            context->dump_flag = true;
            if (fc_queue_empty(&context->queue)) {
                fc_queue_notify(&context->queue);  //notify to dump
            }
        }

        return 0;
    }
#endif

#ifdef __cplusplus
}
#endif

#endif

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
#include "sf/sf_serializer.h"
#include "diskallocator/dio/trunk_read_thread.h"
#include "common/fdir_types.h"
#include "binlog/binlog_types.h"
#include "server_global.h"

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
} ServerDelayFreeContext;

typedef struct server_immediate_free_context {
    volatile int waiting_count;
    struct fc_queue queue;
} ServerImmediateFreeContext;

typedef struct server_free_context {
    struct fast_mblock_man allocator;
    ServerImmediateFreeContext immediate;
    ServerDelayFreeContext delay;
} ServerFreeContext;

typedef struct fdir_db_fetch_context {
    DASynchronizedReadContext read_ctx;
    SFSerializerIterator it;
} FDIRDBFetchContext;

typedef struct fdir_data_thread_context {
    int index;
    struct {
        volatile int waiting_records;
        struct {
            volatile int64_t dthread; //data thread
            volatile int64_t sthread; //service thread
        } last_versions;
    } update_notify; //for data persistency
    struct fc_queue queue;
    FDIRDentryContext dentry_context;
    ServerFreeContext free_context;

    FDIRDBFetchContext db_fetch_ctx;  //for storage engine
} FDIRDataThreadContext;

typedef struct fdir_data_thread_array {
    FDIRDataThreadContext *contexts;
    int count;
} FDIRDataThreadArray;

typedef struct fdir_data_thread_variables {
    FDIRDataThreadArray thread_array;
    volatile int running_count;
    int error_mode;
    volatile int64_t current_event_id;
    struct fast_mblock_man event_allocator;  //for change notify when data persistency
} FDIRDataThreadVariables;

#define dentry_strdup(context, dest, src) \
    fast_allocator_alloc_string(&(context)->name_acontext, dest, src)

#define dentry_strfree(context, s) \
    fast_allocator_free(&(context)->name_acontext, (s)->str)

#define DATA_THREAD_LAST_VERSION     update_notify.last_versions.dthread
#define SERVICE_THREAD_LAST_VERSION  update_notify.last_versions.sthread

#define NOTIFY_EVENT_ALLOCATOR  g_data_thread_vars.event_allocator

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRDataThreadVariables g_data_thread_vars;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    void data_thread_sum_counters(FDIRDentryCounters *counters);

    int server_add_to_delay_free_queue(ServerFreeContext *free_ctx,
            void *ptr, server_free_func free_func, const int delay_seconds);

    int server_add_to_delay_free_queue_ex(ServerFreeContext *free_ctx,
            void *ctx, void *ptr, server_free_func_ex free_func_ex,
            const int delay_seconds);

    int server_add_to_immediate_free_queue_ex(ServerFreeContext *free_ctx,
            void *ctx, void *ptr, server_free_func_ex free_func_ex);

    int server_add_to_immediate_free_queue(ServerFreeContext *free_ctx,
            void *ptr, server_free_func free_func);

    static inline void server_delay_free_str(FDIRDentryContext
            *context, char *str)
    {
        server_add_to_delay_free_queue_ex(&context->db_context->
                free_context, &context->name_acontext, str,
                (server_free_func_ex)fast_allocator_free,
                FDIR_DELAY_FREE_SECONDS);
    }

    static inline void server_immediate_free_str(FDIRDentryContext
            *context, char *str)
    {
        server_add_to_immediate_free_queue_ex(&context->db_context->
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
        if (STORAGE_ENABLED) {
            __sync_add_and_fetch(&context->update_notify.waiting_records, 1);
        }
        fc_queue_push(&context->queue, record);
    }

    int push_to_db_update_queue(FDIRBinlogRecord *record);

    static inline int push_to_db_update_queue_by_service(
            FDIRBinlogRecord *record)
    {
        FDIRDataThreadContext *thread_ctx;

        thread_ctx = g_data_thread_vars.thread_array.contexts +
            record->hash_code % g_data_thread_vars.thread_array.count;
        FC_ATOMIC_SET_LARGER(thread_ctx->SERVICE_THREAD_LAST_VERSION,
                record->data_version);
        return push_to_db_update_queue(record);
    }

    static inline int64_t data_thread_get_last_data_version()
    {
        FDIRDataThreadContext *ctx;
        FDIRDataThreadContext *end;
        int64_t min_version;
        int64_t max_version;
        int64_t service_version;
        int64_t last_version;

        min_version = INT64_MAX;
        max_version = 0;
        end = g_data_thread_vars.thread_array.contexts +
            g_data_thread_vars.thread_array.count;
        for (ctx=g_data_thread_vars.thread_array.contexts; ctx<end; ctx++) {
            service_version = FC_ATOMIC_GET(ctx->SERVICE_THREAD_LAST_VERSION);
            if (__sync_add_and_fetch(&ctx->update_notify.
                        waiting_records, 0) > 0)
            {
                if (CLUSTER_MASTER_PTR == CLUSTER_MYSELF_PTR) {
                    last_version = FC_MIN(service_version,
                            ctx->DATA_THREAD_LAST_VERSION);
                    if (min_version > last_version) {
                        min_version = last_version;
                    }
                } else {
                    if (min_version > ctx->DATA_THREAD_LAST_VERSION) {
                        min_version = ctx->DATA_THREAD_LAST_VERSION;
                    }
                }
            } else {
                if (CLUSTER_MASTER_PTR == CLUSTER_MYSELF_PTR) {
                    last_version = FC_MAX(service_version,
                            ctx->DATA_THREAD_LAST_VERSION);
                    if (max_version < last_version) {
                        max_version = last_version;
                    }
                } else {
                    if (max_version < ctx->DATA_THREAD_LAST_VERSION) {
                        max_version = ctx->DATA_THREAD_LAST_VERSION;
                    }
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

#ifdef __cplusplus
}
#endif

#endif

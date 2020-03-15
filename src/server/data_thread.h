//data_thread.h

#ifndef _DATA_THREAD_H_
#define _DATA_THREAD_H_

#include "fastcommon/server_id_func.h"
#include "common/fdir_types.h"
#include "binlog/binlog_types.h"

#define FDIR_DATA_ERROR_MODE_STRICT   1
#define FDIR_DATA_ERROR_MODE_LOOSE    2

struct fdir_data_thread_context;
typedef struct fdir_dentry_context {
    UniqSkiplistFactory factory;
    struct fast_mblock_man dentry_allocator;
    struct fast_allocator_context name_acontext;
    struct fdir_data_thread_context *db_context;
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
    struct common_blocked_queue queue;
    FDIRDentryContext dentry_context;
    ServerDelayFreeContext delay_free_context;
} FDIRDataThreadContext;

typedef struct fdir_data_thread_array {
    FDIRDataThreadContext *contexts;
    int count;
} FDIRDataThreadArray;

typedef struct fdir_data_thread_variables {
    FDIRDataThreadArray thread_array;
    int error_mode;
} FDIRDataThreadVariables;

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRDataThreadVariables g_data_thread_vars;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    int server_add_to_delay_free_queue(ServerDelayFreeContext *pContext,
            void *ptr, server_free_func free_func, const int delay_seconds);

    int server_add_to_delay_free_queue_ex(ServerDelayFreeContext *pContext,
            void *ctx, void *ptr, server_free_func_ex free_func_ex,
            const int delay_seconds);


    static inline int push_to_data_thread_queue(FDIRBinlogRecord *record)
    {
        FDIRDataThreadContext *context;
        context = g_data_thread_vars.thread_array.contexts +
            record->hash_code % g_data_thread_vars.thread_array.count;
        return common_blocked_queue_push(&context->queue, record);
    }

#ifdef __cplusplus
}
#endif

#endif

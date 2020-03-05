//data_thread.h

#ifndef _DATA_THREAD_H_
#define _DATA_THREAD_H_

#include "binlog/binlog_types.h"

typedef struct data_thread_context {
    struct common_blocked_queue queue;
} DataThreadContext;

typedef struct data_thread_array {
    DataThreadContext *contexts;
    int count;
} DataThreadArray;

#ifdef __cplusplus
extern "C" {
#endif

    extern DataThreadArray g_data_thread_array;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    static inline int push_to_data_thread_queue(FDIRBinlogRecord *record)
    {
        DataThreadContext *context;
        context = g_data_thread_array.contexts + 
            record->hash_code % g_data_thread_array.count;
        return common_blocked_queue_push(&context->queue, record);
    }

#ifdef __cplusplus
}
#endif

#endif

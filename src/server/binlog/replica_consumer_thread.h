//replica_consumer_thread.h

#ifndef _REPLICA_CONSUMER_THREAD_H_
#define _REPLICA_CONSUMER_THREAD_H_

#include "fastcommon/fast_mblock.h"
#include "binlog_types.h"

#define REPLICA_CONSUMER_THREAD_BUFFER_COUNT   4  //double buffers

typedef struct replica_consumer_thread_result {
    int err_no;
    int64_t data_version;
} RecordProcessResult;

typedef struct replica_consumer_thread_context {
    volatile bool continue_flag;
    bool runnings[2];
    pthread_t tids[2];
    struct fast_mblock_man result_allocater;
    ServerBinlogRecordBuffer binlog_buffers[REPLICA_CONSUMER_THREAD_BUFFER_COUNT];
    struct {
        struct common_blocked_queue free;   //free ServerBinlogRecordBuffer ptr
        struct common_blocked_queue input;  //input ServerBinlogRecordBuffer ptr
        struct common_blocked_queue output; //output ServerBinlogRecordBuffer ptr

        struct common_blocked_queue result; //record deal result
    } queues;
} ReplicaConsumerThreadContext;

#ifdef __cplusplus
extern "C" {
#endif

ReplicaConsumerThreadContext *replica_consumer_thread_init(
        const int buffer_size, int *err_no);

static inline ServerBinlogRecordBuffer *replica_consumer_thread_alloc_binlog_buffer(
        ReplicaConsumerThreadContext *ctx)
{
    return (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
            &ctx->queues.free, false);
}

static inline int replica_consumer_thread_free_binlog_buffer(
        ReplicaConsumerThreadContext *ctx, ServerBinlogRecordBuffer *rbuffer)
{
    return common_blocked_queue_push(&ctx->queues.free, rbuffer);
}

int push_to_replica_consumer_queues(ReplicaConsumerThreadContext *ctx,
        ServerBinlogRecordBuffer *rbuffer);

static inline ServerBinlogRecordBuffer *replica_consumer_thread_fetch_result_buffer(
        ReplicaConsumerThreadContext *ctx)
{
    return (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
            &ctx->queues.output, false);
}

void replica_consumer_thread_terminate(ReplicaConsumerThreadContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

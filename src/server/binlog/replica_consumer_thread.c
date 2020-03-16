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
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "common/fdir_proto.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "binlog_producer.h"
#include "binlog_write_thread.h"
#include "replica_consumer_thread.h"

static void *deal_binlog_thread_func(void *arg);
static void *collect_results_thread_func(void *arg);

static inline ServerBinlogRecordBuffer *replica_consumer_thread_alloc_binlog_buffer(
        ReplicaConsumerThreadContext *ctx)
{
    return (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
            &ctx->queues.free, false);
}

static void release_record_buffer(ServerBinlogRecordBuffer *rbuffer, void *args)
{
    if (__sync_sub_and_fetch(&rbuffer->reffer_count, 1) == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rbuffer);

        common_blocked_queue_push(&((ReplicaConsumerThreadContext *)
                    args)->queues.free, rbuffer);
    }
}

static int alloc_record_buffer(ServerBinlogRecordBuffer *rb,
        const int buffer_size)
{
    rb->release_func = release_record_buffer;
    return fast_buffer_init_ex(&rb->buffer, buffer_size);
}

static void replay_done_callback(const int result,
        FDIRBinlogRecord *record, void *args)
{
    ReplicaConsumerThreadContext *ctx;
    RecordProcessResult *r;

    ctx = (ReplicaConsumerThreadContext *)args;
    r = fast_mblock_alloc_object(&ctx->result_allocater);
    if (r != NULL) {
        r->err_no = result;
        r->data_version = record->data_version;
        common_blocked_queue_push(&ctx->queues.result, r);
    }
}

ReplicaConsumerThreadContext *replica_consumer_thread_init(
        struct fast_task_info *task, const int buffer_size, int *err_no)
{
#define BINLOG_REPLAY_BATCH_SIZE  32

    ReplicaConsumerThreadContext *ctx;
    ServerBinlogRecordBuffer *rbuffer;
    int i;

    ctx = (ReplicaConsumerThreadContext *)malloc(
            sizeof(ReplicaConsumerThreadContext));
    if (ctx == NULL) {

        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, (int)
                sizeof(ReplicaConsumerThreadContext));
        *err_no = ENOMEM;
        return NULL;
    }

    memset(ctx, 0, sizeof(ReplicaConsumerThreadContext));
    if ((*err_no=fast_mblock_init_ex(&ctx->result_allocater,
                    sizeof(RecordProcessResult), 4096,
                    NULL, NULL, true)) != 0)
    {
        return NULL;
    }

    ctx->runnings[0] = ctx->runnings[1] = false;
    ctx->continue_flag = true;
    ctx->task = task;

    if ((*err_no=binlog_replay_init_ex(&ctx->replay_ctx,
                    replay_done_callback, ctx,
                    BINLOG_REPLAY_BATCH_SIZE)) != 0)
    {
        return NULL;
    }

    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.free,
                    REPLICA_CONSUMER_THREAD_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.input,
                    REPLICA_CONSUMER_THREAD_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.output,
                    REPLICA_CONSUMER_THREAD_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.result,
                    4096)) != 0)
    {
        return NULL;
    }

    for (i=0; i<REPLICA_CONSUMER_THREAD_BUFFER_COUNT; i++) {
        rbuffer = ctx->binlog_buffers + i;
        if ((*err_no=alloc_record_buffer(rbuffer, buffer_size)) != 0) {
            return NULL;
        }
        rbuffer->args = ctx;
        common_blocked_queue_push(&ctx->queues.free, rbuffer);
    }

    if ((*err_no=fc_create_thread(&ctx->tids[0], deal_binlog_thread_func,
        ctx, SF_G_THREAD_STACK_SIZE)) != 0)
    {
        return NULL;
    }
    if ((*err_no=fc_create_thread(&ctx->tids[1], collect_results_thread_func,
        ctx, SF_G_THREAD_STACK_SIZE)) != 0)
    {
        return NULL;
    }

    return ctx;
}

void replica_consumer_thread_terminate(ReplicaConsumerThreadContext *ctx)
{
    int count;
    int i;

    ctx->continue_flag = false;
    common_blocked_queue_terminate(&ctx->queues.free);
    common_blocked_queue_terminate(&ctx->queues.input);
    common_blocked_queue_terminate(&ctx->queues.output);
    common_blocked_queue_terminate(&ctx->queues.result);

    count = 0;
    while ((ctx->runnings[0] || ctx->runnings[1]) && count++ < 10) {
        usleep(200);
    }

    if (ctx->runnings[0] || ctx->runnings[1]) {
        logWarning("file: "__FILE__", line: %d, "
                "wait thread exit timeout", __LINE__);
    }
    for (i=0; i<REPLICA_CONSUMER_THREAD_BUFFER_COUNT; i++) {
        fast_buffer_destroy(&ctx->binlog_buffers[i].buffer);
    }

    common_blocked_queue_destroy(&ctx->queues.free);
    common_blocked_queue_destroy(&ctx->queues.input);
    common_blocked_queue_destroy(&ctx->queues.output);
    common_blocked_queue_destroy(&ctx->queues.result);

    binlog_replay_destroy(&ctx->replay_ctx);

    free(ctx);
    logInfo("file: "__FILE__", line: %d, "
            "replica_consumer_thread_terminated", __LINE__);
}

static inline int push_to_replica_consumer_queues(
        ReplicaConsumerThreadContext *ctx,
        ServerBinlogRecordBuffer *rbuffer)
{
    int result;

    __sync_add_and_fetch(&rbuffer->reffer_count, 2);
    if ((result=push_to_binlog_write_queue(rbuffer)) != 0) {
        logCrit("file: "__FILE__", line: %d, "
                "push_to_binlog_write_queue fail, program exit!",
                __LINE__);
        SF_G_CONTINUE_FLAG = false;
        return result;
    }

    return common_blocked_queue_push(&ctx->queues.input, rbuffer);
}

int deal_replica_push_request(ReplicaConsumerThreadContext *ctx)
{
    ServerBinlogRecordBuffer *rb;
    if ((rb=replica_consumer_thread_alloc_binlog_buffer(ctx)) == 0) {
        return EAGAIN;
    }

    rb->buffer.length = ctx->task->length - sizeof(FDIRProtoHeader);
    memcpy(rb->buffer.data, ctx->task->data + sizeof(FDIRProtoHeader),
            rb->buffer.length);
    return push_to_replica_consumer_queues(ctx, rb);
}

int deal_replica_push_result(ReplicaConsumerThreadContext *ctx)
{
    ServerBinlogRecordBuffer *rb;

    if (!(ctx->task->offset == 0 && ctx->task->length == 0)) {
        return 0;
    }

    rb = (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
            &ctx->queues.output, false);
    if (rb == NULL) {
        return EAGAIN;
    }

    memcpy(ctx->task->data + sizeof(FDIRProtoHeader), rb->buffer.data,
            rb->buffer.length);
    ctx->task->length = sizeof(FDIRProtoHeader) + rb->buffer.length;

    FDIR_PROTO_SET_HEADER((FDIRProtoHeader *)ctx->task->data,
            FDIR_CLUSTER_PROTO_MASTER_PUSH_BINLOG_RESP,
            rb->buffer.length);

    common_blocked_queue_push(&ctx->queues.free, rb);
    sf_send_add_event(ctx->task);
    return 0;
}

static void *deal_binlog_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    ServerBinlogRecordBuffer *rb;

    logInfo("file: "__FILE__", line: %d, "
            "deal_binlog_thread_func start", __LINE__);

    ctx = (ReplicaConsumerThreadContext *)arg;
    ctx->runnings[0] = true;
    while (ctx->continue_flag) {
        rb = (ServerBinlogRecordBuffer *)common_blocked_queue_pop(
                &ctx->queues.input);
        if (rb == NULL) {
            continue;
        }

        binlog_replay_deal_buffer(&ctx->replay_ctx,
                rb->buffer.data, rb->buffer.length);

        rb->release_func(rb, ctx);
    }

    ctx->runnings[0] = false;
    return NULL;
}

static inline ServerBinlogRecordBuffer *alloc_binlog_buffer(
        ReplicaConsumerThreadContext *ctx)
{
    ServerBinlogRecordBuffer *rbuffer = NULL;

    while (ctx->continue_flag) {
        rbuffer = replica_consumer_thread_alloc_binlog_buffer(ctx);
        if (rbuffer == NULL) {
            usleep(1000);
            continue;
        }
    }
    return rbuffer;
}

static void combine_push_results(ReplicaConsumerThreadContext *ctx,
        struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rbuffer;
    RecordProcessResult *r;
    char *p;
    int count;

    if ((rbuffer=alloc_binlog_buffer(ctx)) == NULL) {
        return;
    }

    count = 0;
    p = rbuffer->buffer.data + sizeof(FDIRProtoPushBinlogRespBodyHeader);
    do {
        r = (RecordProcessResult *)node->data;

        long2buff(r->data_version, ((FDIRProtoPushBinlogRespBodyPart *)
                    p)->data_version);
        short2buff(r->err_no, ((FDIRProtoPushBinlogRespBodyPart *)p)->
                err_no);
        p += sizeof(FDIRProtoPushBinlogRespBodyPart);
        
        fast_mblock_free_object(&ctx->result_allocater, r);
        node = node->next;
        ++count;
    } while (node != NULL);


    logInfo("file: "__FILE__", line: %d, "
            "result count: %d", __LINE__, count);

    int2buff(count, ((FDIRProtoPushBinlogRespBodyHeader *)
                rbuffer->buffer.data)->count);
    rbuffer->buffer.length = p - rbuffer->buffer.data;
    common_blocked_queue_push(&ctx->queues.output, rbuffer);
}

static void *collect_results_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    struct common_blocked_node *node;

    ctx = (ReplicaConsumerThreadContext *)arg;
    ctx->runnings[1] = true;
    while (ctx->continue_flag) {
        node = common_blocked_queue_pop_all_nodes(&ctx->queues.result);
        if (node == NULL) {
            continue;
        }

        combine_push_results(ctx, node);
        common_blocked_queue_free_all_nodes(&ctx->queues.result, node);
    }

    ctx->runnings[1] = false;
    return NULL;
}

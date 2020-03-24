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
#include "fastcommon/ioevent_loop.h"
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

static void release_record_buffer(ServerBinlogRecordBuffer *rbuffer)
{
    if (__sync_sub_and_fetch(&rbuffer->reffer_count, 1) == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rbuffer);

        common_blocked_queue_push((struct common_blocked_queue *)
                rbuffer->args, rbuffer);
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
                    sizeof(RecordProcessResult), 8192,
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

    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.input_free,
                    REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.output_free,
                    REPLICA_CONSUMER_THREAD_OUTPUT_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }

    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.input,
                    REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.output,
                    REPLICA_CONSUMER_THREAD_OUTPUT_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.result,
                    8192)) != 0)
    {
        return NULL;
    }

    rbuffer = ctx->binlog_buffers;
    for (i=0; i<REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT; i++, rbuffer++) {
        if ((*err_no=alloc_record_buffer(rbuffer, buffer_size)) != 0) {
            return NULL;
        }
        rbuffer->args = &ctx->queues.input_free;
        common_blocked_queue_push(&ctx->queues.input_free, rbuffer);
    }
    for (i=0; i<REPLICA_CONSUMER_THREAD_OUTPUT_BUFFER_COUNT; i++, rbuffer++) {
        if ((*err_no=alloc_record_buffer(rbuffer, buffer_size)) != 0) {
            return NULL;
        }
        rbuffer->args = &ctx->queues.output_free;
        common_blocked_queue_push(&ctx->queues.output_free, rbuffer);
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
    common_blocked_queue_terminate(&ctx->queues.input_free);
    common_blocked_queue_terminate(&ctx->queues.output_free);
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

    common_blocked_queue_destroy(&ctx->queues.input_free);
    common_blocked_queue_destroy(&ctx->queues.output_free);
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
    if ((rb=(ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
            &ctx->queues.input_free, false)) == NULL) {
        return EAGAIN;
    }

    rb->buffer.length = ctx->task->length - sizeof(FDIRProtoHeader);
    memcpy(rb->buffer.data, ctx->task->data + sizeof(FDIRProtoHeader),
            rb->buffer.length);
    return push_to_replica_consumer_queues(ctx, rb);
}

int deal_replica_push_result(ReplicaConsumerThreadContext *ctx)
{
    struct common_blocked_node *node;
    ServerBinlogRecordBuffer *rb;
    char *p;
    int count;

    if (!(ctx->task->offset == 0 && ctx->task->length == 0)) {
        return 0;
    }

    if ((node=common_blocked_queue_try_pop_all_nodes(
                    &ctx->queues.output)) == NULL)
    {
        return EAGAIN;
    }

    count = 0;
    p = ctx->task->data + sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoPushBinlogRespBodyHeader);
    do {
        rb = (ServerBinlogRecordBuffer *)node->data;
        if ((p - ctx->task->data) + rb->buffer.length > ctx->task->size) {
            common_blocked_queue_return_nodes(&ctx->queues.output, node);
            break;
        }

        count += rb->buffer.length / sizeof(FDIRProtoPushBinlogRespBodyPart);
        memcpy(p, rb->buffer.data, rb->buffer.length);
        p += rb->buffer.length;

        common_blocked_queue_push(&ctx->queues.output_free, rb);
        node = node->next;
    } while (node != NULL);

    int2buff(count, ((FDIRProtoPushBinlogRespBodyHeader *)
                (ctx->task->data + sizeof(FDIRProtoHeader)))->count);

    ctx->task->length = p - ctx->task->data;
    FDIR_PROTO_SET_HEADER((FDIRProtoHeader *)ctx->task->data,
            FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP,
            ctx->task->length - sizeof(FDIRProtoHeader));
    sf_send_add_event(ctx->task);
    return 0;
}

static void *deal_binlog_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    struct common_blocked_node *node;
    ServerBinlogRecordBuffer *rb;

    logInfo("file: "__FILE__", line: %d, "
            "deal_binlog_thread_func start", __LINE__);

    ctx = (ReplicaConsumerThreadContext *)arg;
    ctx->runnings[0] = true;
    while (ctx->continue_flag) {
        node = common_blocked_queue_pop_all_nodes(&ctx->queues.input);
        if (node == NULL) {
            continue;
        }

        do {
            /*
               logInfo("file: "__FILE__", line: %d, "
               "replay binlog buffer length: %d", __LINE__, rb->buffer.length);
             */

            rb = (ServerBinlogRecordBuffer *)node->data;
            binlog_replay_deal_buffer(&ctx->replay_ctx,
                    rb->buffer.data, rb->buffer.length);

            rb->release_func(rb);
            node = node->next;
        } while (node != NULL);

        common_blocked_queue_free_all_nodes(&ctx->queues.input, node);
    }

    ctx->runnings[0] = false;
    return NULL;
}

static inline ServerBinlogRecordBuffer *alloc_output_binlog_buffer(
        ReplicaConsumerThreadContext *ctx)
{
    ServerBinlogRecordBuffer *rbuffer;

    while (ctx->continue_flag) {
        rbuffer = (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
            &ctx->queues.output_free, false);
        if (rbuffer != NULL) {
            return rbuffer;
        }

        usleep(1000);
        continue;
    }

    return NULL;
}

static void combine_push_results(ReplicaConsumerThreadContext *ctx,
        struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rbuffer;
    RecordProcessResult *r;
    char *p;
    int count;

    do {
        if ((rbuffer=alloc_output_binlog_buffer(ctx)) == NULL) {
            return;
        }

        count = 0;
        p = rbuffer->buffer.data;
        do {
            if (((p - rbuffer->buffer.data) +
                    sizeof(FDIRProtoPushBinlogRespBodyPart)) >
                    rbuffer->buffer.alloc_size)
            {
                break;
            }

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

        rbuffer->buffer.length = p - rbuffer->buffer.data;

        logInfo("file: "__FILE__", line: %d, "
                "result count: %d, data length: %d",
                __LINE__, count, rbuffer->buffer.length);

        common_blocked_queue_push(&ctx->queues.output, rbuffer);
        iovent_notify_thread(ctx->task->thread_data);
    } while (node != NULL);
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

        /*
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "node: %p", __LINE__, __FUNCTION__, node);
                */

        combine_push_results(ctx, node);
        common_blocked_queue_free_all_nodes(&ctx->queues.result, node);
    }

    ctx->runnings[1] = false;
    return NULL;
}

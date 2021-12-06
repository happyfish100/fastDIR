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
#include "fastcommon/pthread_func.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "common/fdir_proto.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "binlog_producer.h"
#include "binlog_write.h"
#include "replica_consumer_thread.h"

static void *deal_binlog_thread_func(void *arg);

static void release_record_buffer(ServerBinlogRecordBuffer *rbuffer)
{
    ReplicaConsumerThreadContext *ctx;
    bool notify;

    ctx = (ReplicaConsumerThreadContext *)rbuffer->args;
    common_blocked_queue_push_ex(&ctx->queues.free, rbuffer, &notify);
    if (notify) {
        ioevent_notify_thread(ctx->task->thread_data);
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
    bool notify;

    ctx = (ReplicaConsumerThreadContext *)args;
    r = fast_mblock_alloc_object(&ctx->result_allocator);
    if (r != NULL) {
        r->err_no = result;
        r->data_version = record->data_version;
        common_blocked_queue_push_ex(&ctx->queues.result, r, &notify);
        if (notify) {
            ioevent_notify_thread(ctx->task->thread_data);
        }
    }
}
   
ReplicaConsumerThreadContext *replica_consumer_thread_init(
        struct fast_task_info *task, const int buffer_size, int *err_no)
{
#define BINLOG_REPLAY_BATCH_SIZE  32

    ReplicaConsumerThreadContext *ctx;
    ServerBinlogRecordBuffer *rbuffer;
    int i;

    ctx = (ReplicaConsumerThreadContext *)fc_malloc(
            sizeof(ReplicaConsumerThreadContext));
    if (ctx == NULL) {

        *err_no = ENOMEM;
        return NULL;
    }

    memset(ctx, 0, sizeof(ReplicaConsumerThreadContext));
    if ((*err_no=fast_mblock_init_ex1(&ctx->result_allocator,
                    "process_result", sizeof(RecordProcessResult),
                    8192, 0, NULL, NULL, true)) != 0)
    {
        return NULL;
    }

    ctx->running = false;
    ctx->continue_flag = true;
    ctx->task = task;

    if ((*err_no=binlog_replay_init_ex(&ctx->replay_ctx,
                    replay_done_callback, ctx,
                    BINLOG_REPLAY_BATCH_SIZE)) != 0)
    {
        return NULL;
    }

    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.free,
                    REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }

    if ((*err_no=common_blocked_queue_init_ex(&ctx->queues.input,
                    REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT)) != 0)
    {
        return NULL;
    }
    if ((*err_no=common_blocked_queue_init_ex(
                    &ctx->queues.result, 8192)) != 0)
    {
        return NULL;
    }

    rbuffer = ctx->binlog_buffers;
    for (i=0; i<REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT; i++, rbuffer++) {
        if ((*err_no=alloc_record_buffer(rbuffer, buffer_size)) != 0) {
            return NULL;
        }
        rbuffer->args = ctx;
        common_blocked_queue_push(&ctx->queues.free, rbuffer);
    }

    ctx->recv_rbuffer = (ServerBinlogRecordBuffer *)common_blocked_queue_pop(
                &ctx->queues.free);
    if ((*err_no=fc_create_thread(&ctx->tid, deal_binlog_thread_func,
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
    common_blocked_queue_terminate(&ctx->queues.result);

    count = 0;
    while (ctx->running && count++ < 300) {
        fc_sleep_ms(10);
    }

    if (ctx->running) {
        logWarning("file: "__FILE__", line: %d, "
                "wait thread exit timeout", __LINE__);
    }
    for (i=0; i<REPLICA_CONSUMER_THREAD_BUFFER_COUNT; i++) {
        fast_buffer_destroy(&ctx->binlog_buffers[i].buffer);
    }

    common_blocked_queue_destroy(&ctx->queues.free);
    common_blocked_queue_destroy(&ctx->queues.input);
    common_blocked_queue_destroy(&ctx->queues.result);

    binlog_replay_destroy(&ctx->replay_ctx);
    fast_mblock_destroy(&ctx->result_allocator);

    free(ctx);
    logDebug("file: "__FILE__", line: %d, "
            "replica_consumer_thread_terminated", __LINE__);
}

static inline int push_and_set_next_recv_buffer(
        ReplicaConsumerThreadContext *ctx,
        ServerBinlogRecordBuffer *rb)
{
    int result;
    int binlog_length;

    binlog_length = ctx->recv_rbuffer->buffer.length;
    if ((result=common_blocked_queue_push(&ctx->queues.input,
                    ctx->recv_rbuffer)) != 0)
    {
        common_blocked_queue_push(&ctx->queues.free, rb);
        return result;
    }

    rb->buffer.length = 0;
    if ((rb->buffer.alloc_size > 2 * BINLOG_BUFFER_INIT_SIZE) &&
            (binlog_length * 10 < rb->buffer.alloc_size))
    {
        if ((result=fast_buffer_set_capacity(&rb->buffer,
                        binlog_length > BINLOG_BUFFER_INIT_SIZE ?
                        binlog_length : BINLOG_BUFFER_INIT_SIZE)) != 0) {
            return result;
        }
        logDebug("file: "__FILE__", line: %d, "
                "data length: %d, shrink buffer size to %d",
                __LINE__, binlog_length, rb->buffer.alloc_size);
    }

    ctx->recv_rbuffer = rb;
    return 0;
}

int deal_replica_push_request(ReplicaConsumerThreadContext *ctx,
        char *binlog_buff, const int length,
        const SFVersionRange *data_version)
{
    ServerBinlogRecordBuffer *rb;
    static int max_waiting_count = 0;
    int result;
    int waiting_count;

    ctx->recv_rbuffer->data_version = *data_version;
    if ((result=fast_buffer_check(&ctx->recv_rbuffer->buffer,
                    length)) != 0)
    {
        return result;
    }

    memcpy(ctx->recv_rbuffer->buffer.data +
            ctx->recv_rbuffer->buffer.length,
            binlog_buff, length);
    ctx->recv_rbuffer->buffer.length += length;

    rb = (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
                &ctx->queues.free, false);
    if (rb != NULL) {
        return push_and_set_next_recv_buffer(ctx, rb);
    }

    if (ctx->recv_rbuffer->buffer.length < ctx->task->size) { //flow control
        return 0;
    }

    waiting_count = 0;
    while (ctx->continue_flag) {
        rb = (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
                &ctx->queues.free, false);
        if (rb != NULL) {
            break;
        }

        ++waiting_count;
        fc_sleep_ms(10);
    }

    if (waiting_count > max_waiting_count) {
        max_waiting_count = waiting_count;
        logWarning("file: "__FILE__", line: %d, "
                "alloc record buffer reachs max waiting count: %d, "
                "buffer length: %d", __LINE__, max_waiting_count, length);
    }
    if (rb == NULL) {
        return EAGAIN;
    }

    return push_and_set_next_recv_buffer(ctx, rb);
}

static inline int check_retry_push_request(ReplicaConsumerThreadContext *ctx)
{
    ServerBinlogRecordBuffer *rb;

    if (ctx->recv_rbuffer->buffer.length > 0) {
        rb = (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
                &ctx->queues.free, false);
        if (rb != NULL) {
            return push_and_set_next_recv_buffer(ctx, rb);
        }
    }

    return 0;
}

static int deal_replica_push_result(ReplicaConsumerThreadContext *ctx)
{
    struct common_blocked_node *node;
    struct common_blocked_node *current;
    struct common_blocked_node *last;
    RecordProcessResult *r;
    char *p;
    int count;

    if (!(ctx->task->offset == 0 && ctx->task->length == 0)) {
        return 0;
    }

    if ((node=common_blocked_queue_try_pop_all_nodes(
                    &ctx->queues.result)) == NULL)
    {
        return EAGAIN;
    }

    count = 0;
    p = ctx->task->data + sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoPushBinlogRespBodyHeader);

    last = NULL;
    current = node;
    do {
        if ((p - ctx->task->data) + sizeof(FDIRProtoPushBinlogRespBodyPart) >
                ctx->task->size)
        {
            last->next = NULL;
            common_blocked_queue_return_nodes(
                    &ctx->queues.result, current);
            break;
        }

        r = (RecordProcessResult *)current->data;
        long2buff(r->data_version, ((FDIRProtoPushBinlogRespBodyPart *)
                    p)->data_version);
        short2buff(r->err_no, ((FDIRProtoPushBinlogRespBodyPart *)p)->
                err_no);
        p += sizeof(FDIRProtoPushBinlogRespBodyPart);

        fast_mblock_free_object(&ctx->result_allocator, r);
        ++count;

        last = current;
        current = current->next;
    } while (current != NULL);
    common_blocked_queue_free_all_nodes(&ctx->queues.result, node);

    int2buff(count, ((FDIRProtoPushBinlogRespBodyHeader *)
                (ctx->task->data + sizeof(FDIRProtoHeader)))->count);

    ctx->task->length = p - ctx->task->data;
    SF_PROTO_SET_HEADER((FDIRProtoHeader *)ctx->task->data,
            FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP,
            ctx->task->length - sizeof(FDIRProtoHeader));
    sf_send_add_event(ctx->task);
    return 0;
}

int deal_replica_push_task(ReplicaConsumerThreadContext *ctx)
{
    int result;

    if ((result=check_retry_push_request(ctx)) != 0) {
        return result;
    }
    return deal_replica_push_result(ctx);
}

static void *deal_binlog_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    struct common_blocked_node *node;
    struct common_blocked_node *current;
    ServerBinlogRecordBuffer *rb;

    logDebug("file: "__FILE__", line: %d, "
            "deal_binlog_thread_func start", __LINE__);

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "replica-consumer");
#endif

    ctx = (ReplicaConsumerThreadContext *)arg;
    ctx->running = true;
    while (ctx->continue_flag) {
        node = common_blocked_queue_pop_all_nodes(&ctx->queues.input);
        if (node == NULL) {
            continue;
        }

        current = node;
        do {
            rb = (ServerBinlogRecordBuffer *)current->data;

            /*
            logInfo("file: "__FILE__", line: %d, "
                    "replay binlog buffer: %p, length: %d, "
                    "data_version: {%"PRId64", %"PRId64"}", __LINE__,
                    rb, rb->buffer.length, rb->data_version.first,
                    rb->data_version.last);
                    */

            if (binlog_replay_deal_buffer(&ctx->replay_ctx,
                    rb->buffer.data, rb->buffer.length, NULL) == 0)
            {
                if (push_to_binlog_write_queue(rb) != 0) {
                    logCrit("file: "__FILE__", line: %d, "
                            "push_to_binlog_write_queue fail, "
                            "program exit!", __LINE__);
                    ctx->continue_flag = false;
                    sf_terminate_myself();
                    break;
                }
            } else {
                logCrit("file: "__FILE__", line: %d, "
                        "binlog replay deal buffer fail, "
                        "program exit!", __LINE__);
                ctx->continue_flag = false;
                sf_terminate_myself();
                break;
            }

            rb->release_func(rb);
            current = current->next;
        } while (current != NULL);
        common_blocked_queue_free_all_nodes(&ctx->queues.input, node);
    }

    ctx->running = false;
    return NULL;
}

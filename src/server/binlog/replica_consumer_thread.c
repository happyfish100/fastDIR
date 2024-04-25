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
#include "sf/sf_service.h"
#include "common/fdir_proto.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "binlog_producer.h"
#include "binlog_write.h"
#include "replica_consumer_thread.h"

static void *deal_binlog_thread_func(void *arg);

static void release_record_buffer(ServerBinlogRecordBuffer *rbuffer,
        const int reffer_count)
{
    ReplicaConsumerThreadContext *ctx;
    bool notify;

    ctx = (ReplicaConsumerThreadContext *)rbuffer->args;
    common_blocked_queue_push_ex(&ctx->queues.free, rbuffer, &notify);
    if (notify) {
        ioevent_notify_thread(ctx->task->thread_data);
    }
}

ReplicaConsumerThreadContext *replica_consumer_thread_init(
        struct fast_task_info *task, int *err_no)
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

    ctx->running = true;
    ctx->continue_flag = true;
    ctx->task = task;

    if ((*err_no=binlog_replay_init_ex(&ctx->replay_ctx, NULL,
                    ctx, BINLOG_REPLAY_BATCH_SIZE)) != 0)
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

    rbuffer = ctx->binlog_buffers;
    for (i=0; i<REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT; i++, rbuffer++) {
        rbuffer->release_func = release_record_buffer;
        rbuffer->args = ctx;
        common_blocked_queue_push(&ctx->queues.free, rbuffer);
    }

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
    int sleep_ms;
    int64_t start_time;
    int64_t time_used;
    char time_buff[32];

    start_time = get_current_time_ms();
    ctx->continue_flag = false;
    common_blocked_queue_terminate(&ctx->queues.free);
    common_blocked_queue_terminate(&ctx->queues.input);

    count = 0;
    sleep_ms = 1;
    while (ctx->running && count++ < 1800) {
        common_blocked_queue_terminate(&ctx->queues.free);
        common_blocked_queue_terminate(&ctx->queues.input);
        if (sleep_ms < 1000) {
            sleep_ms *= 2;
        }
        fc_sleep_ms(sleep_ms);
    }

    if (ctx->running) {
        logWarning("file: "__FILE__", line: %d, "
                "wait replica consumer thread exit timeout", __LINE__);
    } else {
        time_used = get_current_time_ms() - start_time;
        long_to_comma_str(time_used, time_buff);
        logInfo("file: "__FILE__", line: %d, "
                "wait replica consumer thread exit, time used: %s ms",
                __LINE__, time_buff);
    }

    common_blocked_queue_destroy(&ctx->queues.free);
    common_blocked_queue_destroy(&ctx->queues.input);

    binlog_replay_destroy(&ctx->replay_ctx);

    free(ctx);
    logDebug("file: "__FILE__", line: %d, "
            "replica_consumer_thread_terminated", __LINE__);
}

static ServerBinlogRecordBuffer *alloc_record_buffer(
        ReplicaConsumerThreadContext *ctx)
{
    ServerBinlogRecordBuffer *rb;
    static int max_waiting_count = 0;
    int waiting_count;

    rb = (ServerBinlogRecordBuffer *)common_blocked_queue_pop_ex(
                &ctx->queues.free, false);
    if (rb != NULL) {
        return rb;
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
                "alloc record buffer reaches max waiting count: %d",
                __LINE__, max_waiting_count);
    }
    return rb;
}

int deal_replica_push_request(ReplicaConsumerThreadContext *ctx,
        char *binlog_buff, const int binlog_len,
        const SFVersionRange *data_version)
{
    ServerBinlogRecordBuffer *rb;
    int result;

    if ((rb=alloc_record_buffer(ctx)) == NULL) {
        return EAGAIN;
    }

    rb->data_version = *data_version;
    rb->buffer.data = binlog_buff;
    rb->buffer.length = binlog_len;
    sf_hold_task(ctx->task);
    if ((result=common_blocked_queue_push(&ctx->queues.input, rb)) != 0) {
        sf_release_task(ctx->task);
        return result;
    }

    return 0;
}

static void *deal_binlog_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    struct common_blocked_node *node;
    struct common_blocked_node *current;
    ServerBinlogRecordBuffer *rb;
    struct fast_task_info *task;
    FDIRProtoPushBinlogResp *resp;
    int record_count;

    logDebug("file: "__FILE__", line: %d, "
            "deal_binlog_thread_func start", __LINE__);

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "replica-consumer");
#endif

    ctx = (ReplicaConsumerThreadContext *)arg;
    task = ctx->task;
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

            if (binlog_replay_deal_buffer(&ctx->replay_ctx, NULL,
                        rb->buffer.data, rb->buffer.length,
                        &record_count) == 0)
            {
                if (push_to_binlog_write_queue(rb, record_count) != 0) {
                    logCrit("file: "__FILE__", line: %d, "
                            "push_to_binlog_write_queue fail, "
                            "program exit!", __LINE__);
                    ctx->continue_flag = false;
                    sf_terminate_myself();
                    break;
                }

                FC_ATOMIC_SET(MY_CONFIRMED_VERSION, rb->data_version.last);

                resp = (FDIRProtoPushBinlogResp *)SF_PROTO_SEND_BODY(task);
                int2buff(record_count, resp->record_count);
                TASK_CTX.common.response_done = true;
                RESPONSE.header.body_len = sizeof(*resp);

                sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
                sf_release_task(task);
            } else {
                logCrit("file: "__FILE__", line: %d, "
                        "binlog replay deal buffer fail, "
                        "program exit!", __LINE__);
                ctx->continue_flag = false;
                sf_terminate_myself();
                break;
            }

            rb->release_func(rb, 1);
            current = current->next;
        } while (current != NULL);
        common_blocked_queue_free_all_nodes(&ctx->queues.input, node);
    }

    ctx->running = false;
    return NULL;
}

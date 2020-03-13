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
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "binlog_producer.h"
#include "replica_consumer_thread.h"

static void *deal_binlog_thread_func(void *arg);
static void *collect_results_thread_func(void *arg);

int replica_consumer_thread_init(ReplicaConsumerThreadContext *ctx,
        const int buffer_size)
{
    int result;
    int i;

    if ((result=fast_mblock_init_ex(&ctx->result_allocater,
                    sizeof(RecordProcessResult), 4096,
                    NULL, NULL, true)) != 0)
    {
        return result;
    }

    ctx->runnings[0] = ctx->runnings[1] = false;
    ctx->continue_flag = true;

    if ((result=common_blocked_queue_init_ex(&ctx->queues.free,
                    REPLICA_CONSUMER_THREAD_BUFFER_COUNT)) != 0)
    {
        return result;
    }
    if ((result=common_blocked_queue_init_ex(&ctx->queues.input,
                    REPLICA_CONSUMER_THREAD_BUFFER_COUNT)) != 0)
    {
        return result;
    }
    if ((result=common_blocked_queue_init_ex(&ctx->queues.output,
                    REPLICA_CONSUMER_THREAD_BUFFER_COUNT)) != 0)
    {
        return result;
    }
    if ((result=common_blocked_queue_init_ex(&ctx->queues.result,
                    4096)) != 0)
    {
        return result;
    }

    for (i=0; i<REPLICA_CONSUMER_THREAD_BUFFER_COUNT; i++) {
        if ((result=fc_init_buffer(ctx->binlog_buffer + i,
                        buffer_size)) != 0)
        {
            return result;
        }
    }

    if ((result=fc_create_thread(&ctx->tids[0], deal_binlog_thread_func,
        ctx, SF_G_THREAD_STACK_SIZE)) != 0)
    {
        return result;
    }
    if ((result=fc_create_thread(&ctx->tids[1], collect_results_thread_func,
        ctx, SF_G_THREAD_STACK_SIZE)) != 0)
    {
        return result;
    }
    return 0;
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
        free(ctx->binlog_buffer[i].buff);
        ctx->binlog_buffer[i].buff = NULL;
    }

    common_blocked_queue_destroy(&ctx->queues.free);
    common_blocked_queue_destroy(&ctx->queues.input);
    common_blocked_queue_destroy(&ctx->queues.output);
    common_blocked_queue_destroy(&ctx->queues.result);
}

static void *deal_binlog_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    BufferInfo *buffer;

    ctx = (ReplicaConsumerThreadContext *)arg;
    ctx->runnings[0] = true;
    while (ctx->continue_flag) {
        buffer = (BufferInfo *)common_blocked_queue_pop(
                &ctx->queues.input);
        if (buffer == NULL) {
            continue;
        }

        //TODO
        replica_consumer_thread_free_binlog_buffer(ctx, buffer);
    }

    ctx->runnings[0] = false;
    return NULL;
}

static void *collect_results_thread_func(void *arg)
{
    ReplicaConsumerThreadContext *ctx;
    BufferInfo *buffer;

    ctx = (ReplicaConsumerThreadContext *)arg;
    ctx->runnings[1] = true;
    while (ctx->continue_flag) {
        buffer = replica_consumer_thread_alloc_binlog_buffer(ctx);
        buffer = (BufferInfo *)common_blocked_queue_pop(
                &ctx->queues.result);
        if (buffer == NULL) {
            continue;
        }

        //TODO
        replica_consumer_thread_free_binlog_buffer(ctx, buffer);
    }

    ctx->runnings[1] = false;
    return NULL;
}

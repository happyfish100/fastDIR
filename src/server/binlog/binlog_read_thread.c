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
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "binlog_producer.h"
#include "binlog_read_thread.h"

static void *binlog_read_thread_func(void *arg);

int binlog_read_thread_init1(BinlogReadThreadContext *ctx,
        const BinlogReaderParams params[2], const int buffer_size,
        const int buffer_count)
{
    int result;
    int i;

    ctx->results = (BinlogReadThreadResult *)fc_malloc(
            sizeof(BinlogReadThreadResult) * buffer_count);
    if (ctx->results == NULL) {
        return ENOMEM;
    }

    if ((result=binlog_reader_init_ex(&ctx->reader, params[0].subdir_name,
                    &params[0].hint_pos, params[0].last_data_version)) != 0)
    {
        free(ctx->results);
        ctx->results = NULL;
        return result;
    }

    ctx->buffer_count = buffer_count;
    ctx->next_reader_params = params[1];
    ctx->reader_index = 0;
    ctx->running = 0;
    ctx->continue_flag = 1;
    if ((result=common_blocked_queue_init_ex(&ctx->queues.waiting,
                    ctx->buffer_count)) != 0)
    {
        return result;
    }
    if ((result=common_blocked_queue_init_ex(&ctx->queues.done,
                    ctx->buffer_count)) != 0)
    {
        return result;
    }

    for (i=0; i<ctx->buffer_count; i++) {
        if ((result=fc_init_buffer(&ctx->results[i].
                        buffer, buffer_size)) != 0)
        {
            return result;
        }

        binlog_read_thread_return_result_buffer(ctx, ctx->results + i);
    }

    return fc_create_thread(&ctx->tid, binlog_read_thread_func,
        ctx, SF_G_THREAD_STACK_SIZE);
}

int binlog_read_thread_init_ex(BinlogReadThreadContext *ctx,
        const char *subdir_name, const SFBinlogFilePosition *hint_pos,
        const int64_t last_data_version, const int buffer_size,
        const int buffer_count)
{
    BinlogReaderParams params[2];

    BINLOG_READER_SET_PARAMS(params[0], subdir_name,
            *hint_pos, last_data_version);
    params[1].subdir_name = NULL;
    return binlog_read_thread_init1(ctx, params, buffer_size, buffer_count);
}

void binlog_read_thread_terminate(BinlogReadThreadContext *ctx)
{
    int count;
    int i;

    FC_ATOMIC_SET(ctx->continue_flag, 0);
    common_blocked_queue_terminate(&ctx->queues.waiting);
    common_blocked_queue_terminate(&ctx->queues.done);

    count = 0;
    while (FC_ATOMIC_GET(ctx->running) && count++ < 300) {
        if (count % 10 == 0) {
            common_blocked_queue_terminate(&ctx->queues.waiting);
            common_blocked_queue_terminate(&ctx->queues.done);
        }
        fc_sleep_ms(10);
    }

    if (FC_ATOMIC_GET(ctx->running)) {
        logWarning("file: "__FILE__", line: %d, "
                "wait thread exit timeout", __LINE__);
    }
    for (i=0; i<ctx->buffer_count; i++) {
        fc_free_buffer(&ctx->results[i].buffer);
    }

    common_blocked_queue_destroy(&ctx->queues.waiting);
    common_blocked_queue_destroy(&ctx->queues.done);
    binlog_reader_destroy(&ctx->reader);
    free(ctx->results);
}

static void *binlog_read_thread_func(void *arg)
{
    BinlogReadThreadContext *ctx;
    BinlogReadThreadResult *r;
    BinlogReaderParams *params;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "binlog-reader");
#endif

    ctx = (BinlogReadThreadContext *)arg;
    FC_ATOMIC_SET(ctx->running, 1);
    while (FC_ATOMIC_GET(ctx->continue_flag)) {
        r = (BinlogReadThreadResult *)common_blocked_queue_pop(
                &ctx->queues.waiting);
        if (r == NULL) {
            continue;
        }

        while (1) {
            r->binlog_position = ctx->reader.position;
            r->err_no = binlog_reader_integral_read(&ctx->reader,
                    r->buffer.buff, r->buffer.alloc_size,
                    &r->buffer.length, &r->data_version);

            if (r->err_no != ENOENT) {
                break;
            }

            params = &ctx->next_reader_params;
            if (!(++ctx->reader_index < 2 && params->subdir_name != NULL)) {
                break;
            }

            fc_sleep_ms(10);
            binlog_reader_destroy(&ctx->reader);
            if (binlog_reader_init_ex(&ctx->reader, params->subdir_name,
                        &params->hint_pos, params->last_data_version) != 0)
            {
                break;
            }
        }

        common_blocked_queue_push(&ctx->queues.done, r);
    }

    FC_ATOMIC_SET(ctx->running, 0);
    return NULL;
}

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

//binlog_read_thread.h

#ifndef _BINLOG_READ_THREAD_H_
#define _BINLOG_READ_THREAD_H_

#include "binlog_types.h"
#include "binlog_reader.h"

#define BINLOG_READ_THREAD_BUFFER_COUNT   2  //double buffers

typedef struct binlog_read_thread_result {
    int err_no;
    SFBinlogFilePosition binlog_position;
    SFVersionRange data_version;
    BufferInfo buffer;
} BinlogReadThreadResult;

typedef struct binlog_read_thread_context {
    ServerBinlogReader reader;
    BinlogReaderParams next_reader_params;
    unsigned char reader_index;
    volatile char continue_flag;
    volatile char running;
    int buffer_count;
    pthread_t tid;
    BinlogReadThreadResult *results;
    struct {
        struct common_blocked_queue waiting;
        struct common_blocked_queue done;
    } queues;
} BinlogReadThreadContext;

#define binlog_read_thread_init(ctx, hint_pos, last_data_version, buffer_size) \
    binlog_read_thread_init_ex(ctx, FDIR_BINLOG_SUBDIR_NAME, hint_pos, \
            last_data_version, buffer_size, BINLOG_READ_THREAD_BUFFER_COUNT)


#ifdef __cplusplus
extern "C" {
#endif

int binlog_read_thread_init1(BinlogReadThreadContext *ctx,
        const BinlogReaderParams params[2], const int buffer_size,
        const int buffer_count);

int binlog_read_thread_init_ex(BinlogReadThreadContext *ctx,
        const char *subdir_name, const SFBinlogFilePosition *hint_pos,
        const int64_t last_data_version, const int buffer_size,
        const int buffer_count);

static inline int binlog_read_thread_return_result_buffer(
        BinlogReadThreadContext *ctx, BinlogReadThreadResult *r)
{
    return common_blocked_queue_push(&ctx->queues.waiting, r);
}

static inline BinlogReadThreadResult *binlog_read_thread_fetch_result_ex(
        BinlogReadThreadContext *ctx, const bool block)
{
    return (BinlogReadThreadResult *)common_blocked_queue_pop_ex(
            &ctx->queues.done, block);
}

#define binlog_read_thread_fetch_result(ctx) \
    binlog_read_thread_fetch_result_ex(ctx, true)

void binlog_read_thread_terminate(BinlogReadThreadContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

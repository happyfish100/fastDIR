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

//binlog_replay_mt.h

#ifndef _BINLOG_REPLAY_MT_H_
#define _BINLOG_REPLAY_MT_H_

#include <pthread.h>
#include "binlog_types.h"
#include "binlog_read_thread.h"

#define BINLOG_REPLAY_DOUBLE_BUFFER_COUNT   2

typedef struct data_thread_counter {
    volatile int64_t total;
    volatile int64_t done;
} DataThreadCounter;

typedef struct binlog_batch_context {
    FDIRBinlogRecord *records;
    BinlogReadThreadResult **results;
    DataThreadCounter *counters;
} BinlogBatchContext;

typedef struct binlog_record_chain {
    FDIRBinlogRecord *head;
    FDIRBinlogRecord *tail;
} BinlogRecordChain;

typedef struct binlog_parse_thread_context {
    int64_t total_count;
    short thread_index;

    struct {
        bool parse_done;
        pthread_lock_cond_pair_t lcp;
    } notify;

    BinlogRecordChain records;  //for output
    BinlogReadThreadResult *r;
    struct binlog_replay_mt_context *replay_ctx;
} BinlogParseThreadContext;

typedef struct binlog_parse_thread_ctx_array {
    BinlogParseThreadContext *contexts;
    int count;
} BinlogParseThreadCtxArray;

typedef struct binlog_replay_mt_context {
    BinlogReadThreadContext *read_thread_ctx;

    struct {
        BinlogBatchContext bcontexts[BINLOG_REPLAY_DOUBLE_BUFFER_COUNT];
        volatile int elt_index;
        volatile short arr_index;
    } record_allocator;

    volatile short parse_thread_count;
    short dealing_threads;
    volatile bool parse_continue_flag;
    BinlogParseThreadCtxArray parse_thread_array;

    int64_t data_current_version;
    int last_errno;
    int64_t record_count;
    int64_t skip_count;
    int64_t warning_count;
    volatile int64_t fail_count;
} BinlogReplayMTContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replay_mt_init(BinlogReplayMTContext *replay_ctx,
        BinlogReadThreadContext *read_thread_ctx, const int parse_threads);

void binlog_replay_mt_destroy(BinlogReplayMTContext *replay_ctx);

int binlog_replay_mt_parse_buffer(BinlogReplayMTContext *replay_ctx,
        BinlogReadThreadResult *r);

void binlog_replay_mt_read_done(BinlogReplayMTContext *replay_ctx);

#ifdef __cplusplus
}
#endif

#endif

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
#include "fastcommon/fc_atomic.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../shared_thread_pool.h"
#include "../service_handler.h"
#include "../data_thread.h"
#include "binlog_pack.h"
#include "binlog_reader.h"
#include "binlog_replay_mt.h"

static void data_thread_deal_done_callback(
        struct fdir_binlog_record *record,
        const int result, const bool is_error)
{
    BinlogReplayMTContext *replay_ctx;
    DataThreadCounter *counter;

    replay_ctx = (BinlogReplayMTContext *)record->notify.args;
    if (result != 0) {
        if (is_error) {
            replay_ctx->last_errno = result;
            __sync_add_and_fetch(&replay_ctx->fail_count, 1);
            SF_G_CONTINUE_FLAG = false;
        } else {
            __sync_add_and_fetch(&replay_ctx->warning_count, 1);
        }
        service_record_deal_error_log(record, result, is_error);
    }

    counter = replay_ctx->record_allocator.bcontexts[record->extra.arr_index].
        counters + record->extra.data_thread_index;
    FC_ATOMIC_INC(counter->done);
}

#define RECORD_ADD_TO_CHAIN(thread_ctx, record) \
    if (thread_ctx->records.head == NULL) { \
        thread_ctx->records.head = record;  \
    } else { \
        thread_ctx->records.tail->next = record; \
    } \
    thread_ctx->records.tail = record; \
    thread_ctx->total_count++

static int parse_buffer(BinlogParseThreadContext *thread_ctx)
{
    int result;
    BinlogBatchContext *bctx;
    const char *p;
    char *buff_end;
    const char *rend;
    char error_info[SF_ERROR_INFO_SIZE];
    FDIRBinlogRecord *record;

    bctx = thread_ctx->replay_ctx->record_allocator.bcontexts + FC_ATOMIC_GET(
            thread_ctx->replay_ctx->record_allocator.arr_index);

    result = 0;
    buff_end = thread_ctx->r->buffer.buff + thread_ctx->r->buffer.length;
    p = thread_ctx->r->buffer.buff;
    while (p < buff_end) {
        record = bctx->records + __sync_fetch_and_add(&thread_ctx->
                replay_ctx->record_allocator.elt_index, 1);
        if ((result=binlog_unpack_record(p, buff_end - p, record,
                        &rend, error_info, sizeof(error_info))) != 0)
        {
            char filename[PATH_MAX];
            int64_t line_count;

            sf_binlog_writer_get_filename(DATA_PATH_STR,
                    FDIR_BINLOG_SUBDIR_NAME, thread_ctx->r->
                    binlog_position.index, filename, sizeof(filename));
            if (fc_get_file_line_count_ex(filename, thread_ctx->r->
                        binlog_position.offset + (p - thread_ctx->r->
                            buffer.buff), &line_count) == 0)
            {
                ++line_count;
            }
            logError("file: "__FILE__", line: %d, "
                    "binlog file: %s, line no: %"PRId64", %s",
                    __LINE__, filename, line_count, error_info);
            return result;
        }

        p = rend;
        RECORD_ADD_TO_CHAIN(thread_ctx, record);
    }

    if (thread_ctx->records.tail != NULL) {
        thread_ctx->records.tail->next = NULL;
    }
    return result;
}

static void binlog_parse_thread_run(BinlogParseThreadContext *thread_ctx,
        void *thread_data)
{
    __sync_add_and_fetch(&thread_ctx->replay_ctx->parse_thread_count, 1);

    while (SF_G_CONTINUE_FLAG && thread_ctx->
            replay_ctx->parse_continue_flag)
    {
        PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
        if (thread_ctx->r == NULL) {
            pthread_cond_wait(&thread_ctx->notify.lcp.cond,
                    &thread_ctx->notify.lcp.lock);
        }
        PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);

        if (thread_ctx->r != NULL) {
            if (parse_buffer(thread_ctx) != 0) {
                sf_terminate_myself();
                break;
            }

            PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
            thread_ctx->r = NULL;
            thread_ctx->notify.parse_done = true;
            pthread_cond_signal(&thread_ctx->notify.lcp.cond);
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);
        }
    }

    __sync_sub_and_fetch(&thread_ctx->replay_ctx->parse_thread_count, 1);
}

static int init_parse_thread_context(BinlogParseThreadContext *thread_ctx)
{
    int result;

    if ((result=init_pthread_lock_cond_pair(&thread_ctx->notify.lcp)) != 0) {
        return result;
    }

    thread_ctx->total_count = 0;
    thread_ctx->notify.parse_done = false;
    thread_ctx->records.head = thread_ctx->records.tail = NULL;
    thread_ctx->r = NULL;
    return 0;
}

static int init_parse_thread_ctx_array(BinlogReplayMTContext *replay_ctx,
        BinlogParseThreadCtxArray *ctx_array, const int thread_count)
{
    int result;
    int bytes;
    BinlogParseThreadContext *ctx;
    BinlogParseThreadContext *end;

    bytes = sizeof(BinlogParseThreadContext) * thread_count;
    ctx_array->contexts = (BinlogParseThreadContext *)fc_malloc(bytes);
    if (ctx_array->contexts == NULL) {
        return ENOMEM;
    }

    ctx_array->count = thread_count;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        if ((result=init_parse_thread_context(ctx)) != 0) {
            return result;
        }

        ctx->thread_index = ctx - ctx_array->contexts;
        ctx->replay_ctx = replay_ctx;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        binlog_parse_thread_run, ctx)) != 0)
        {
            return result;
        }
    }

    return 0;
}


static inline int init_records(BinlogReplayMTContext *ctx,
        BinlogBatchContext *bctx, const int arr_index,
        const int parse_threads, const int elements_limit)
{
    int bytes;
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *end;

    bytes = sizeof(DataThreadCounter) * DATA_THREAD_COUNT;
    bctx->counters = (DataThreadCounter *)fc_malloc(bytes);
    if (bctx->counters == NULL) {
        return ENOMEM;
    }
    memset(bctx->counters, 0, bytes);

    bytes = sizeof(BinlogReadThreadResult *) * parse_threads;
    bctx->results = (BinlogReadThreadResult **)fc_malloc(bytes);
    if (bctx->results == NULL) {
        return ENOMEM;
    }
    memset(bctx->results, 0, bytes);

    end = bctx->records + elements_limit;
    for (record=bctx->records; record<end; record++) {
        record->notify.func = data_thread_deal_done_callback;
        record->notify.args = ctx;
        record->extra.arr_index = arr_index;
    }

    return 0;
}

static int init_thread_ctx_array(BinlogReplayMTContext *ctx,
        const int parse_threads)
{
    int elements_limit;
    int bytes;
    int result;
    BinlogBatchContext *bctx;
    BinlogBatchContext *end;

    elements_limit = (int)(((int64_t)parse_threads *
                BINLOG_BUFFER_SIZE) / BINLOG_RECORD_MIN_SIZE);
    end = ctx->record_allocator.bcontexts + BINLOG_REPLAY_DOUBLE_BUFFER_COUNT;
    for (bctx=ctx->record_allocator.bcontexts; bctx<end; bctx++) {
        bytes = sizeof(FDIRBinlogRecord) * elements_limit;
        bctx->records = (FDIRBinlogRecord *)fc_malloc(bytes);
        if (bctx->records == NULL) {
            return ENOMEM;
        }
        memset(bctx->records, 0, bytes);

        if ((result=init_records(ctx, bctx, bctx - ctx->record_allocator.
                        bcontexts, parse_threads, elements_limit)) != 0)
        {
            return result;
        }
    }
    ctx->record_allocator.elt_index = 0;
    ctx->record_allocator.arr_index = 0;

    return init_parse_thread_ctx_array(ctx, &ctx->
            parse_thread_array, parse_threads);
}

static void destroy_parse_thread_ctx_array(BinlogParseThreadCtxArray *ctx_array)
{
    BinlogParseThreadContext *ctx;
    BinlogParseThreadContext *end;

    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        destroy_pthread_lock_cond_pair(&ctx->notify.lcp);
    }

    free(ctx_array->contexts);
}

void binlog_replay_mt_destroy(BinlogReplayMTContext *ctx)
{
    BinlogBatchContext *bctx;
    BinlogBatchContext *end;

    destroy_parse_thread_ctx_array(&ctx->parse_thread_array);
    end = ctx->record_allocator.bcontexts + BINLOG_REPLAY_DOUBLE_BUFFER_COUNT;
    for (bctx=ctx->record_allocator.bcontexts; bctx<end; bctx++) {
        free(bctx->records);
        free(bctx->counters);
    }
}

int binlog_replay_mt_init(BinlogReplayMTContext *replay_ctx,
        BinlogReadThreadContext *read_thread_ctx, const int parse_threads)
{
    int result;

    replay_ctx->record_count = 0;
    replay_ctx->skip_count = 0;
    replay_ctx->warning_count = 0;
    replay_ctx->fail_count = 0;
    replay_ctx->last_errno = 0;
    replay_ctx->read_thread_ctx = read_thread_ctx;
    replay_ctx->data_current_version = __sync_add_and_fetch(
            &DATA_CURRENT_VERSION, 0);

    replay_ctx->parse_continue_flag = true;
    replay_ctx->dealing_threads = 0;
    replay_ctx->parse_thread_count = 0;
    if ((result=init_thread_ctx_array(replay_ctx, parse_threads)) != 0) {
        return result;
    }

    return 0;
}

static void waiting_and_process_parse_result(BinlogReplayMTContext
        *replay_ctx, BinlogParseThreadContext *parse_thread)
{
    FDIRBinlogRecord *current;
    FDIRBinlogRecord *record;
    DataThreadCounter *counter;

    PTHREAD_MUTEX_LOCK(&parse_thread->notify.lcp.lock);
    while (!parse_thread->notify.parse_done && SF_G_CONTINUE_FLAG) {
        pthread_cond_wait(&parse_thread->notify.lcp.cond,
                &parse_thread->notify.lcp.lock);
    }
    PTHREAD_MUTEX_UNLOCK(&parse_thread->notify.lcp.lock);

    if (!SF_G_CONTINUE_FLAG) {
        return;
    }

    current = parse_thread->records.head;
    while (current != NULL) {
        record = current;
        current = current->next;

        set_data_thread_index(record);
        counter = replay_ctx->record_allocator.bcontexts[record->extra.
            arr_index].counters + record->extra.data_thread_index;
        counter->total++;

        if (record->data_version <= replay_ctx->data_current_version) {
            replay_ctx->skip_count++;
            FC_ATOMIC_INC(counter->done);
        } else {
            replay_ctx->data_current_version = record->data_version;
            push_to_data_thread_queue(record);
        }
    }

    parse_thread->records.head = parse_thread->records.tail = NULL;
}

static void waiting_and_process_parse_outputs(BinlogReplayMTContext *replay_ctx)
{
    BinlogParseThreadContext *parse_thread;
    BinlogParseThreadContext *parse_end;

    parse_end = replay_ctx->parse_thread_array.contexts +
        replay_ctx->dealing_threads;
    for (parse_thread=replay_ctx->parse_thread_array.contexts;
            parse_thread<parse_end; parse_thread++)
    {
        waiting_and_process_parse_result(replay_ctx, parse_thread);
    }

    if (!SF_G_CONTINUE_FLAG) {
        return;
    }

    replay_ctx->dealing_threads = 0;
}

static void waiting_data_thread_done(BinlogReplayMTContext *replay_ctx,
        const short arr_index)
{
    BinlogBatchContext *bctx;
    DataThreadCounter *counter;
    DataThreadCounter *end;
    int i;

    bctx = replay_ctx->record_allocator.bcontexts + arr_index;
    end = bctx->counters + DATA_THREAD_COUNT;
    for (counter=bctx->counters; counter<end &&
            SF_G_CONTINUE_FLAG; counter++)
    {
        while ((FC_ATOMIC_GET(counter->done) < FC_ATOMIC_GET(
                        counter->total)) && SF_G_CONTINUE_FLAG)
        {
            fc_sleep_ms(1);
        }
    }

    for (i=0; i<replay_ctx->parse_thread_array.count; i++) {
        if (bctx->results[i] != NULL) {
            binlog_read_thread_return_result_buffer(replay_ctx->
                    read_thread_ctx, bctx->results[i]);
            bctx->results[i] = NULL;
        }
    }
}

int binlog_replay_mt_parse_buffer(BinlogReplayMTContext *replay_ctx,
        BinlogReadThreadResult *r)
{
    BinlogParseThreadContext *thread_ctx;
    BinlogBatchContext *bctx;
    short new_arr_index;

    thread_ctx = replay_ctx->parse_thread_array.contexts +
        replay_ctx->dealing_threads;

    PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
    thread_ctx->r = r;
    thread_ctx->notify.parse_done = false;
    pthread_cond_signal(&thread_ctx->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);

    bctx = replay_ctx->record_allocator.bcontexts + FC_ATOMIC_GET(
            replay_ctx->record_allocator.arr_index);
    bctx->results[thread_ctx->thread_index] = r;

    if (++(replay_ctx->dealing_threads) ==
            replay_ctx->parse_thread_array.count)
    {
        waiting_and_process_parse_outputs(replay_ctx);

        new_arr_index = (FC_ATOMIC_GET(replay_ctx->record_allocator.
                    arr_index) + 1) % BINLOG_REPLAY_DOUBLE_BUFFER_COUNT;
        waiting_data_thread_done(replay_ctx, new_arr_index);

        FC_ATOMIC_SET(replay_ctx->record_allocator.arr_index, new_arr_index);
        FC_ATOMIC_SET(replay_ctx->record_allocator.elt_index, 0);
    }

    return 0;
}

static void terminate_parse_threads(BinlogReplayMTContext *replay_ctx)
{
    BinlogParseThreadContext *parse_thread;
    BinlogParseThreadContext *end;

    replay_ctx->parse_continue_flag = false;
    end = replay_ctx->parse_thread_array.contexts +
        replay_ctx->parse_thread_array.count;
    while (SF_G_CONTINUE_FLAG) {
        for (parse_thread=replay_ctx->parse_thread_array.contexts;
                parse_thread<end; parse_thread++)
        {
            pthread_cond_signal(&parse_thread->notify.lcp.cond);
        }

        if (__sync_add_and_fetch(&replay_ctx->parse_thread_count, 0) == 0) {
            break;
        }
        fc_sleep_ms(1);
    }
}

void binlog_replay_mt_read_done(BinlogReplayMTContext *replay_ctx)
{
    int index;
    BinlogParseThreadContext *parse_thread;
    BinlogParseThreadContext *end;

    if (replay_ctx->dealing_threads > 0) {
        waiting_and_process_parse_outputs(replay_ctx);
    }

    terminate_parse_threads(replay_ctx);

    for (index=0; index<BINLOG_REPLAY_DOUBLE_BUFFER_COUNT; index++) {
        waiting_data_thread_done(replay_ctx, index);
    }

    end = replay_ctx->parse_thread_array.contexts +
        replay_ctx->parse_thread_array.count;
    for (parse_thread=replay_ctx->parse_thread_array.contexts;
            parse_thread<end; parse_thread++)
    {
        replay_ctx->record_count += parse_thread->total_count;
    }
}

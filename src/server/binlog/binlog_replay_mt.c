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
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "../shared_thread_pool.h"
#include "binlog_pack.h"
#include "binlog_reader.h"
#include "binlog_replay_mt.h"

#define MBLOCK_BATCH_ALLOC_SIZE  1024

static void data_thread_deal_done_callback(
        struct fdir_binlog_record *record,
        const int result, const bool is_error)
{
    BinlogParseThreadContext *parse_ctx;
    int log_level;

    parse_ctx = (BinlogParseThreadContext *)record->notify.args;
    if (result != 0) {
        if (is_error) {
            log_level = LOG_ERR;
            parse_ctx->replay_ctx->last_errno = result;
            __sync_add_and_fetch(&parse_ctx->replay_ctx->fail_count, 1);
        } else {
            log_level = LOG_WARNING;
            __sync_add_and_fetch(&parse_ctx->replay_ctx->warning_count, 1);
        }

        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, "
                "%s dentry fail, errno: %d, error info: %s, "
                "namespace: %.*s, inode: %"PRId64", parent inode: %"PRId64
                ", subname: %.*s", __LINE__,
                get_operation_caption(record->operation),
                result, STRERROR(result),
                record->ns.len, record->ns.str,
                record->inode, record->me.pname.parent_inode,
                record->me.pname.name.len, record->me.pname.name.str);
    }
}

static int parse_buffer(BinlogParseThreadContext *thread_ctx)
{
    int result;
    const char *p;
    char *buff_end;
    const char *rend;
    char error_info[SF_ERROR_INFO_SIZE];
    struct fast_mblock_node *node;
    FDIRBinlogRecord *record;

    result = 0;
    thread_ctx->records.head = thread_ctx->records.tail = NULL;
    buff_end = thread_ctx->r->buffer.buff + thread_ctx->r->buffer.length;
    p = thread_ctx->r->buffer.buff;
    while (p < buff_end) {
        if (thread_ctx->freelist == NULL) {
            thread_ctx->freelist = fast_mblock_batch_alloc(
                    &thread_ctx->record_allocator,
                    MBLOCK_BATCH_ALLOC_SIZE);
            if (thread_ctx->freelist == NULL) {
                result = ENOMEM;
                break;
            }
        }
        node = thread_ctx->freelist;
        thread_ctx->freelist = thread_ctx->freelist->next;
        record = (FDIRBinlogRecord *)node->data;

        if ((result=binlog_unpack_record(p, buff_end - p, record,
                        &rend, error_info, sizeof(error_info))) != 0)
        {
            char filename[PATH_MAX];
            int64_t line_count;

            sf_binlog_writer_get_filename(FDIR_BINLOG_SUBDIR_NAME,
                    thread_ctx->r->binlog_position.index,
                    filename, sizeof(filename));
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
    }

    if (thread_ctx->records.tail != NULL) {
        thread_ctx->records.tail->next = NULL;
    }

    binlog_read_thread_return_result_buffer(thread_ctx->
            replay_ctx->read_thread_ctx, thread_ctx->r);
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


static int binlog_record_alloc_init(FDIRBinlogRecord *record,
        BinlogParseThreadContext *parse_ctx)
{
    record->notify.func = data_thread_deal_done_callback;
    record->notify.args = parse_ctx;
    return 0;
}

static int init_parse_thread_context(BinlogParseThreadContext *thread_ctx)
{
    const int alloc_elements_once = 8 * 1024;
    int elements_limit;
    int result;

    if ((result=init_pthread_lock_cond_pair(&thread_ctx->notify.lcp)) != 0) {
        return result;
    }

    elements_limit = (8 * BINLOG_BUFFER_SIZE) / BINLOG_RECORD_MIN_SIZE;
    if ((result=fast_mblock_init_ex1(&thread_ctx->record_allocator,
                    "replay-brecord", sizeof(FDIRBinlogRecord),
                    alloc_elements_once, elements_limit,
                    (fast_mblock_alloc_init_func)binlog_record_alloc_init,
                    thread_ctx, true)) != 0)
    {
        return result;
    }
    fast_mblock_set_need_wait(&thread_ctx->record_allocator,
            true, (bool *)&SF_G_CONTINUE_FLAG);

    thread_ctx->total_count = 0;
    thread_ctx->notify.parse_done = false;
    thread_ctx->records.head = thread_ctx->records.tail = NULL;
    thread_ctx->r = NULL;
    thread_ctx->freelist = NULL;
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

        ctx->replay_ctx = replay_ctx;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        binlog_parse_thread_run, ctx)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static int init_thread_ctx_array(BinlogReplayMTContext *ctx)
{
    int parse_threads;

    parse_threads = 2 * DATA_THREAD_COUNT;
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
        fast_mblock_destroy(&ctx->record_allocator);
    }

    free(ctx_array->contexts);
}

void binlog_replay_mt_destroy(BinlogReplayMTContext *replay_ctx)
{
    destroy_parse_thread_ctx_array(&replay_ctx->parse_thread_array);
}

int binlog_replay_mt_init(BinlogReplayMTContext *replay_ctx,
        BinlogReadThreadContext *read_thread_ctx)
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
    if ((result=init_thread_ctx_array(replay_ctx)) != 0) {
        return result;
    }

    return 0;
}

static void waiting_and_process_parse_result(BinlogReplayMTContext
        *replay_ctx, BinlogParseThreadContext *parse_thread)
{
    FDIRBinlogRecord *current;
    FDIRBinlogRecord *record;

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
        push_to_data_thread_queue(record);
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

int binlog_replay_mt_parse_buffer(BinlogReplayMTContext *replay_ctx,
        BinlogReadThreadResult *r)
{
    BinlogParseThreadContext *thread_ctx;

    thread_ctx = replay_ctx->parse_thread_array.contexts +
        replay_ctx->dealing_threads;

    PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
    thread_ctx->r = r;
    thread_ctx->notify.parse_done = false;
    pthread_cond_signal(&thread_ctx->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);

    if (++(replay_ctx->dealing_threads) ==
            replay_ctx->parse_thread_array.count)
    {
        waiting_and_process_parse_outputs(replay_ctx);
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
    BinlogParseThreadContext *parse_thread;
    BinlogParseThreadContext *end;

    if (replay_ctx->dealing_threads > 0) {
        waiting_and_process_parse_outputs(replay_ctx);
    }

    terminate_parse_threads(replay_ctx);

    end = replay_ctx->parse_thread_array.contexts +
        replay_ctx->parse_thread_array.count;
    for (parse_thread=replay_ctx->parse_thread_array.contexts;
            parse_thread<end; parse_thread++)
    {
        replay_ctx->record_count += parse_thread->total_count;
    }
}

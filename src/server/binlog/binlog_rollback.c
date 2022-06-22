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

#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "../data_thread.h"
#include "binlog_write.h"
#include "binlog_reader.h"
#include "binlog_pack.h"
#include "binlog_rollback.h"

typedef struct {
    ServerBinlogReader reader;
    struct fast_mblock_man allocator;  //element: FDIRBinlogRecord
    struct fast_mpool_man mpool;
    pthread_lock_cond_pair_t lcp;
    FDIRBinlogRecord *head;
    int64_t record_count;
    volatile int waiting_count;
    int last_errno;
    volatile int64_t warning_count;
    volatile int64_t fail_count;
} BinlogRollbackContext;

static int parse_buffer_to_chain(BinlogRollbackContext
        *rollback_ctx, const char *buff, const int len)
{
    const char *p;
    const char *end;
    const char *rend;
    FDIRBinlogRecord *record;
    char error_info[SF_ERROR_INFO_SIZE];
    int result;

    *error_info = '\0';
    p = buff;
    end = p + len;
    while (p < end) {
        if ((record=fast_mblock_alloc_object(&rollback_ctx->
                        allocator)) == NULL)
        {
            return ENOMEM;
        }
        if ((result=binlog_unpack_record_ex(NULL, p, end - p, record,
                        &rend, error_info, sizeof(error_info),
                        &rollback_ctx->mpool)) != 0)
        {
            int64_t line_count;

            if (fc_get_file_line_count_ex(rollback_ctx->reader.filename,
                        (rollback_ctx->reader.position.offset - len) +
                        (p - buff), &line_count) == 0)
            {
                ++line_count;
            }
            logError("file: "__FILE__", line: %d, "
                    "binlog file: %s, line no: %"PRId64", %s",
                    __LINE__, rollback_ctx->reader.filename,
                    line_count, error_info);
            return result;
        }
        p = rend;

        record->next = rollback_ctx->head;
        rollback_ctx->head = record;
        rollback_ctx->record_count++;
    }

    return 0;
}

static void data_thread_deal_done_callback(FDIRBinlogRecord *record,
        const int result, const bool is_error)
{
    BinlogRollbackContext *rollback_ctx;
    int log_level;

    rollback_ctx = (BinlogRollbackContext *)record->notify.args;
    if (result != 0) {
        if (is_error) {
            log_level = LOG_ERR;
            rollback_ctx->last_errno = result;
            FC_ATOMIC_INC(rollback_ctx->fail_count);
        } else {
            log_level = LOG_WARNING;
            FC_ATOMIC_INC(rollback_ctx->warning_count);
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

    PTHREAD_MUTEX_LOCK(&rollback_ctx->lcp.lock);
    if (--rollback_ctx->waiting_count == 0) {
        pthread_cond_signal(&rollback_ctx->lcp.cond);
    }
    PTHREAD_MUTEX_UNLOCK(&rollback_ctx->lcp.lock);
}

static int rollback_record_alloc_init(FDIRBinlogRecord *record,
        BinlogRollbackContext *rollback_ctx)
{
    record->notify.func = data_thread_deal_done_callback;
    record->notify.args = rollback_ctx;
    return 0;
}

static int rollback_context_init(BinlogRollbackContext *rollback_ctx,
        const int64_t my_confirmed_version)
{
    const int alloc_size_once = 256 * 1024;
    const int discard_size = 64;
    int result;
    SFBinlogFilePosition hint_pos;

    rollback_ctx->head = NULL;
    rollback_ctx->record_count = 0;
    rollback_ctx->warning_count = 0;
    rollback_ctx->fail_count = 0;
    rollback_ctx->waiting_count = 0;
    rollback_ctx->last_errno = 0;
    if ((result=fast_mblock_init_ex1(&rollback_ctx->allocator,
                    "rollback_record", sizeof(FDIRBinlogRecord),
                    4096, 0, (fast_mblock_object_init_func)
                    rollback_record_alloc_init, rollback_ctx,
                    false)) != 0)
    {
        return result;
    }

    if ((result=fast_mpool_init(&rollback_ctx->mpool,
                    alloc_size_once, discard_size)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(&rollback_ctx->lcp)) != 0) {
        return result;
    }

    hint_pos.index = binlog_get_current_write_index();
    hint_pos.offset = 0;
    if ((result=binlog_reader_init(&rollback_ctx->reader,
                    &hint_pos, my_confirmed_version)) != 0)
    {
        return result;
    }

    return 0;
}

static void rollback_context_destroy(BinlogRollbackContext *rollback_ctx)
{
    fast_mblock_destroy(&rollback_ctx->allocator);
    fast_mpool_destroy(&rollback_ctx->mpool);
    destroy_pthread_lock_cond_pair(&rollback_ctx->lcp);
    binlog_reader_destroy(&rollback_ctx->reader);
}

static int rollback_data(BinlogRollbackContext *rollback_ctx)
{
    FDIRBinlogRecord *record;

    PTHREAD_MUTEX_LOCK(&rollback_ctx->lcp.lock);
    rollback_ctx->waiting_count = rollback_ctx->record_count;
    PTHREAD_MUTEX_UNLOCK(&rollback_ctx->lcp.lock);

    while (rollback_ctx->head != NULL) {
        record = rollback_ctx->head;
        rollback_ctx->head = rollback_ctx->head->next;
        push_to_data_thread_queue(record);
    }

    PTHREAD_MUTEX_LOCK(&rollback_ctx->lcp.lock);
    while (rollback_ctx->waiting_count != 0) {
        pthread_cond_wait(&rollback_ctx->lcp.cond,
                &rollback_ctx->lcp.lock);
    }
    PTHREAD_MUTEX_UNLOCK(&rollback_ctx->lcp.lock);

    if (FC_ATOMIC_GET(rollback_ctx->fail_count) > 0) {
        return (rollback_ctx->last_errno != 0 ?
                rollback_ctx->last_errno : EIO);
    } else {
        return 0;
    }
}

int binlog_rollback_data_and_binlog(const int64_t my_confirmed_version,
        const int64_t current_data_version)
{
    int result;
    int read_bytes;
    BinlogRollbackContext rollback_ctx;
    SFVersionRange version_range;

    if ((result=rollback_context_init(&rollback_ctx,
                    my_confirmed_version)) != 0)
    {
        return result;
    }

    while (1) {
        result = binlog_reader_integral_read(&rollback_ctx.reader,
                rollback_ctx.reader.binlog_buffer.buff, rollback_ctx.
                reader.binlog_buffer.size, &read_bytes, &version_range);
        if (result != 0) {
            if (result == ENOENT) {
                fc_sleep_ms(100);
                continue;
            } else {
                break;
            }
        }

        if ((result=parse_buffer_to_chain(&rollback_ctx, rollback_ctx.
                        reader.binlog_buffer.buff, read_bytes)) != 0)
        {
            break;
        }

        if (version_range.last >= current_data_version) {
            break;
        }
    }

    if (result == 0) {
        result = rollback_data(&rollback_ctx);
    }

    rollback_context_destroy(&rollback_ctx);
    return result;
}

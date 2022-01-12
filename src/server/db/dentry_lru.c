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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "../inode_index.h"
#include "../dentry.h"
#include "dentry_lru.h"

typedef struct {
    int64_t total_limit;
    int64_t thread_limit;  //per thread
} DentryLRUContext;

static DentryLRUContext dentry_lru_ctx;

static int elimination_calc_func(void *args)
{
    int64_t eliminate_total;
    int64_t eliminate_count;
    int64_t remain_count;
    int eliminate_threads;
    FDIRDataThreadContext *thread;

    eliminate_total = TOTAL_DENTRY_COUNT - dentry_lru_ctx.total_limit;
    if (eliminate_total <= 0) {
        return 0;
    }

    eliminate_threads = 0;
    remain_count = eliminate_total;
    for (thread=g_data_thread_vars.thread_array.contexts;
            thread<DATA_THREAD_END; thread++)
    {
        eliminate_count = thread->lru_ctx.total_count -
            dentry_lru_ctx.thread_limit;
        if (eliminate_count > 0) {
            ++eliminate_threads;
            if (eliminate_count >= remain_count) {
                thread->lru_ctx.target_reclaims = remain_count;
                remain_count = 0;
            } else {
                thread->lru_ctx.target_reclaims = eliminate_count;
                remain_count -= eliminate_count;
            }

            if (fc_queue_empty(&thread->queue)) {
                fc_queue_notify(&thread->queue);  //notify to reclaim dentries
            }

            if (remain_count == 0) {
                break;
            }
        }
    }

    logDebug("total count: %"PRId64", eliminate_total: %"PRId64", "
            "eliminate_threads: %d", TOTAL_DENTRY_COUNT,
            eliminate_total, eliminate_threads);
    return 0;
}

int dentry_lru_init()
{
    int64_t min_dentries;
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    if (DENTRY_ELIMINATE_INTERVAL <= 0) {
        return 0;
    }

    min_dentries = 65536 * DATA_THREAD_COUNT;
    dentry_lru_ctx.total_limit = (int64_t)(SYSTEM_TOTAL_MEMORY *
            STORAGE_MEMORY_LIMIT * MEMORY_LIMIT_DENTRY_RATIO) / 300;
    if (dentry_lru_ctx.total_limit < min_dentries) {
        logWarning("file: "__FILE__", line: %d, "
                "dentry limit: %"PRId64" is too small, set to %"PRId64,
                __LINE__, dentry_lru_ctx.total_limit, min_dentries);
        dentry_lru_ctx.total_limit = min_dentries;
    }

    dentry_lru_ctx.thread_limit = dentry_lru_ctx.total_limit /
        DATA_THREAD_COUNT;

    logDebug("file: "__FILE__", line: %d, dentry total limit: %"PRId64", "
            "data thread count: %d, dentry thread limit: %"PRId64,
            __LINE__, dentry_lru_ctx.total_limit, DATA_THREAD_COUNT,
            dentry_lru_ctx.thread_limit);

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(), 0, 0, 0,
            DENTRY_ELIMINATE_INTERVAL, elimination_calc_func, NULL);
    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

static int dentry_reclaim(FDIRServerDentry *dentry)
{
    int count;

    if ((dentry->parent == NULL) || ((dentry->db_args->loaded_flags &
                    FDIR_DENTRY_LOADED_FLAGS_BASIC) == 0))
    {
        fc_list_move_tail(&dentry->db_args->lru_dlink,
                &dentry->context->thread_ctx->lru_ctx.head);
        return 0;
    }

    /* hardlink source dentry */
    if (!S_ISDIR(dentry->stat.mode) && dentry->stat.nlink > 1) {
        fc_list_move_tail(&dentry->db_args->lru_dlink,
                &dentry->context->thread_ctx->lru_ctx.head);
        return 0;
    }

    if (__sync_add_and_fetch(&dentry->reffer_count, 0) > 1) {
        return 0;
    }

    if (!FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        inode_index_del_dentry(dentry);
    }

    if (S_ISDIR(dentry->stat.mode)) {
        if (dentry->children != NULL) {
            count = uniq_skiplist_count(dentry->children);
            uniq_skiplist_free(dentry->children);
            dentry->children = NULL;
        } else {
            count = 0;
        }
    } else {
        count = 0;
    }

    dentry_free_for_elimination(dentry);
    dentry->db_args->loaded_flags = 0;
    return count;
}

void dentry_lru_eliminate(struct fdir_data_thread_context *thread_ctx,
        const int64_t target_reclaims)
{
    int64_t total_count;
    int64_t eliminate_count;
    int64_t loop_count;
    int count;
    FDIRServerDentryDBArgs *db_args;
    FDIRServerDentryDBArgs *tmp_args;
    FDIRServerDentry *dentry;

    thread_ctx->lru_ctx.reclaim_count = 0;
    eliminate_count = thread_ctx->lru_ctx.total_count -
        dentry_lru_ctx.thread_limit;
    if (eliminate_count <= 0) {
        return;
    }

    if (eliminate_count > target_reclaims) {
        eliminate_count = target_reclaims;
    }

    total_count = thread_ctx->lru_ctx.total_count;
    loop_count = 0;
    fc_list_for_each_entry_safe(db_args, tmp_args,
            &thread_ctx->lru_ctx.head, lru_dlink)
    {
        if (loop_count++ == total_count) {
            break;
        }

        dentry = fc_list_entry(db_args, FDIRServerDentry, db_args);
        if (S_ISDIR(dentry->stat.mode)) {
            if ((dentry->db_args->loaded_count != 0) ||
                    (dentry->parent == NULL))
            {
                fc_list_move_tail(&dentry->db_args->lru_dlink,
                        &thread_ctx->lru_ctx.head);
                continue;
            }
        }

        if ((count=dentry_reclaim(dentry)) > 0) {
            thread_ctx->lru_ctx.reclaim_count += count;
            if (thread_ctx->lru_ctx.reclaim_count >= eliminate_count) {
                break;
            }
        }
    }

    logDebug("file: "__FILE__", line: %d, thread index: %d, "
            "total_count {old: %"PRId64", new: %"PRId64"}, "
            "loop_count: %"PRId64", target_reclaims: %"PRId64", "
            "reclaim_count: %"PRId64, __LINE__, thread_ctx->index,
            total_count, thread_ctx->lru_ctx.total_count, loop_count,
            eliminate_count, thread_ctx->lru_ctx.reclaim_count);
}

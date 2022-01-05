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
#include "dentry_lru.h"

DentryLRUContext g_dentry_lru_ctx;

static int elimination_calc_func(void *args)
{
    int64_t eliminate_total;
    int64_t eliminate_count;
    int64_t remain_count;
    int eliminate_threads;
    FDIRDataThreadContext *thread;

    eliminate_total = g_dentry_lru_ctx.total_count -
        g_dentry_lru_ctx.total_limit;
    if (eliminate_total <= 0) {
        return 0;
    }

    eliminate_threads = 0;
    remain_count = eliminate_total;
    for (thread=g_data_thread_vars.thread_array.contexts;
            thread<g_dentry_lru_ctx.thread_end; thread++)
    {
        eliminate_count = thread->lru_ctx.total_count -
            g_dentry_lru_ctx.thread_limit;
        if (eliminate_count > 0) {
            ++eliminate_threads;
            if (eliminate_count >= remain_count) {
                thread->lru_ctx.eliminate_count = remain_count;
                break;
            } else {
                thread->lru_ctx.eliminate_count = eliminate_count;
                remain_count -= eliminate_count;
            }
        }
    }

    logInfo("total count: %"PRId64", eliminate_total: %"PRId64", "
            "eliminate_threads: %d", g_dentry_lru_ctx.total_count,
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
    g_dentry_lru_ctx.total_limit = (int64_t)(SYSTEM_TOTAL_MEMORY *
            STORAGE_MEMORY_LIMIT * MEMORY_LIMIT_DENTRY_RATIO) / 300;
    if (g_dentry_lru_ctx.total_limit < min_dentries) {
        logWarning("file: "__FILE__", line: %d, "
                "dentry limit: %"PRId64" is too small, set to %"PRId64,
                __LINE__, g_dentry_lru_ctx.total_limit, min_dentries);
        g_dentry_lru_ctx.total_limit = min_dentries;
    }

    g_dentry_lru_ctx.thread_limit = g_dentry_lru_ctx.total_limit /
        DATA_THREAD_COUNT;
    g_dentry_lru_ctx.thread_end = g_data_thread_vars.
        thread_array.contexts + DATA_THREAD_COUNT;

    logInfo("file: "__FILE__", line: %d, dentry total limit: %"PRId64", "
            "data thread count: %d, dentry thread limit: %"PRId64,
            __LINE__, g_dentry_lru_ctx.total_limit, DATA_THREAD_COUNT,
            g_dentry_lru_ctx.thread_limit);

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(), 0, 0, 0,
            DENTRY_ELIMINATE_INTERVAL, elimination_calc_func, NULL);
    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

int dentry_lru_eliminate(struct fc_list_head *head, const int target_count)
{
    return 0;
}

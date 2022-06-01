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
#include "fastcommon/fc_atomic.h"
#include "sf/sf_binlog_writer.h"
#include "../data_thread.h"
#include "binlog_dump.h"
#include "binlog_dedup.h"

static inline int check_make_subdir()
{
    char path[PATH_MAX];

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_DEDUP_SUBDIR_NAME, path, sizeof(path));
    return fc_check_mkdir(path, 0755);
}

static int binlog_dedup_func(void *args)
{
    static volatile bool dedup_in_progress = false;
    int result;
    double dedup_ratio;
    int64_t dentry_count;
    int64_t old_binlog_count;
    int64_t new_binlog_count;
    int64_t start_time_ms;
    int64_t time_used;
    char filename[PATH_MAX];
    char time_buff[32];

    if (!dedup_in_progress) {
        dedup_in_progress = true;

        dentry_count = data_thread_get_total_dentry_count();
        if (dentry_count > 0) {
            dedup_ratio = (double)(BINLOG_RECORD_COUNT -
                    dentry_count) / (double)dentry_count;
        } else {
            dedup_ratio = 0.00;
        }

        if (dedup_ratio >= BINLOG_DEDUP_RATIO) {
            start_time_ms = get_current_time_ms();
            logInfo("file: "__FILE__", line: %d, "
                    "dentry count: %"PRId64", dentry_count: %"PRId64", "
                    "dedup_ratio: %.2f%%, dedup binlog ...", __LINE__,
                    dentry_count, BINLOG_RECORD_COUNT, dedup_ratio * 100.00);

            old_binlog_count = FC_ATOMIC_GET(BINLOG_RECORD_COUNT);
            binlog_dedup_get_filename(filename, sizeof(filename));
            result = binlog_dump_all(FDIR_DEDUP_SUBDIR_NAME, filename);
            new_binlog_count = FC_ATOMIC_GET(BINLOG_RECORD_COUNT);
            if (result == 0) {
                int64_t inc_count;
                inc_count =  new_binlog_count - old_binlog_count;
                new_binlog_count = dentry_count + inc_count;
                FC_ATOMIC_SET(BINLOG_RECORD_COUNT, new_binlog_count);
            }
            time_used = get_current_time_ms() - start_time_ms;
            long_to_comma_str(time_used, time_buff);
            logInfo("file: "__FILE__", line: %d, "
                    "dump data to %s %s, dentry count: %"PRId64", "
                    "binlog record count: %"PRId64", time used: %s ms",
                    __LINE__, filename, (result == 0 ? "success" : "fail"),
                    dentry_count, new_binlog_count, time_buff);
        }

        dedup_in_progress = false;
    }

    return 0;
}

int binlog_dedup_add_schedule()
{
    int result;
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntry;

    if (!BINLOG_DEDUP_ENABLED) {
        return 0;
    }

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    INIT_SCHEDULE_ENTRY_EX1(scheduleEntry, sched_generate_next_id(),
            BINLOG_DEDUP_TIME, /*86400*/60, binlog_dedup_func, NULL, true);
    scheduleArray.entries = &scheduleEntry;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}

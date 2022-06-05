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
#include "fastcommon/sched_thread.h"
#include "../db/event_dealer.h"
#include "binlog_write.h"
#include "binlog_reader.h"
#include "binlog_clean.h"

static int remove_old_binlogs(const time_t before_time, int *remove_count)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    time_t last_timestamp;
    int64_t last_data_version;
    SFBinlogFilePosition hint_pos;
    SFBinlogFilePosition position;
    char binlog_filename[PATH_MAX];

    *remove_count = 0;
    if ((result=binlog_get_indexes(&start_index, &last_index)) != 0) {
        return result;
    }

    if (BINLOG_DEDUP_ENABLED) {
        last_data_version = event_dealer_get_last_data_version();
        binlog_get_current_write_position(&hint_pos);
        if ((result=binlog_find_position(&hint_pos,
                        last_data_version, &position)) != 0)
        {
            return result;
        }
        if (last_index > position.index) {
            last_index = position.index;
        }
    } else if (DUMP_LAST_DATA_VERSION > 0) {
        if (last_index > DUMP_NEXT_POSITION.index) {
            last_index = DUMP_NEXT_POSITION.index;
        }
    }

    logInfo("binlog start_index: %d, last_index: %d",
            start_index, last_index);

    for (binlog_index=start_index; binlog_index<last_index; binlog_index++) {
        if ((result=binlog_get_last_timestamp(FDIR_BINLOG_SUBDIR_NAME,
                        binlog_index, &last_timestamp)) != 0)
        {
            return result;
        }

        /*
        {
            char buff[32];
            formatDatetime(last_timestamp, "%Y-%m-%d %H:%M:%S",
                    buff, sizeof(buff));
            logInfo("binlog_index: %d, last time: %s, timestamp: %ld",
                    binlog_index, buff, last_timestamp);
        }
            */

        if (last_timestamp >= before_time) {
            break;
        }

        if ((result=binlog_set_start_index(binlog_index + 1)) != 0) {
            return result;
        }

        binlog_get_filename(binlog_index, binlog_filename,
                sizeof(binlog_filename));
        if ((result=fc_delete_file_ex(binlog_filename, "binlog")) != 0) {
            return result;
        }

        (*remove_count)++;
    }

    return 0;
}

static int clean_binlogs(int *remove_count)
{
    time_t before_time;
    struct tm tm;

    before_time = time(NULL) - BINLOG_KEEP_DAYS * 86400;
    localtime_r(&before_time, &tm);
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    before_time = mktime(&tm);
    return remove_old_binlogs(before_time, remove_count);
}

static int binlog_clean_func(void *args)
{
    static volatile bool clean_in_progress = false;
    int result;
    int total_remove_count;
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];

    if (!clean_in_progress) {
        clean_in_progress = true;

        start_time_ms = get_current_time_ms();
        logInfo("file: "__FILE__", line: %d, "
                "clean binlogs ...", __LINE__);

        result = clean_binlogs(&total_remove_count);
        time_used = get_current_time_ms() - start_time_ms;
        logInfo("file: "__FILE__", line: %d, "
                "clean binlogs %s, remove binlog count: %d, "
                "time used: %s ms", __LINE__, (result == 0 ? "success" :
                    "fail"), total_remove_count,
                long_to_comma_str(time_used, time_buff));

        clean_in_progress = false;
    }

    return 0;
}

int binlog_clean_add_schedule()
{
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntry;

    if (BINLOG_KEEP_DAYS <= 0) {
        return 0;
    }

    INIT_SCHEDULE_ENTRY_EX1(scheduleEntry, sched_generate_next_id(),
            BINLOG_DELETE_TIME, 86400, binlog_clean_func, NULL, true);
    scheduleArray.entries = &scheduleEntry;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}

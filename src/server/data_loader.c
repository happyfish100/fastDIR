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
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "sf/sf_util.h"
#include "db/event_dealer.h"
#include "server_global.h"
#include "server_binlog.h"
#include "data_thread.h"
#include "data_loader.h"

static int load_data(const int parse_threads, const BinlogReaderParams *params,
        BinlogReplayMTStat *stat, const bool show_prompt)
{
    int result;
    BinlogReplayMTContext replay_ctx;
    BinlogReadThreadContext reader_ctx;
    BinlogReadThreadResult *r;

    if ((result=binlog_read_thread_init1(&reader_ctx, params,
                    BINLOG_BUFFER_SIZE, parse_threads * 2)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "binlog_read_thread_init fail, "
                "errno: %d, error info: %s",
                __LINE__, result, sf_strerror(result));
        return result;
    }

    if ((result=binlog_replay_mt_init(&replay_ctx, stat,
                    &reader_ctx, parse_threads)) != 0)
    {
        return result;
    }

    if (show_prompt) {
        logInfo("file: "__FILE__", line: %d, "
                "loading data, parse thread count: %d ...",
                __LINE__, parse_threads);
    }

    result = 0;
    while (SF_G_CONTINUE_FLAG && replay_ctx.stat->fail_count == 0) {
        if ((r=binlog_read_thread_fetch_result(&reader_ctx)) == NULL) {
            result = EINTR;
            break;
        }

        //logInfo("errno: %d, buffer length: %d", r->err_no, r->buffer.length);
        if (r->err_no == ENOENT) {
            break;
        } else if (r->err_no != 0) {
            result = r->err_no;
            break;
        }

        if ((result=binlog_replay_mt_parse_buffer(&replay_ctx, r)) != 0) {
            break;
        }
    }

    binlog_replay_mt_read_done(&replay_ctx);
    binlog_replay_mt_destroy(&replay_ctx);
    binlog_read_thread_terminate(&reader_ctx);

    if (result == 0) {
        if (replay_ctx.stat->fail_count > 0) {
            result = (replay_ctx.last_errno != 0 ?
                    replay_ctx.last_errno : EBUSY);
        }
    }

    return 0;
}

static int parse_add_inodes(const string_t *content)
{
    char *p;
    char *bend;
    char *endptr;
    int64_t inode;
    int result;

    bend = content->str + content->len;
    p = content->str;
    while (p < bend) {
        inode = strtoll(p, &endptr, 10);
        if (*endptr != '\n') {
            logError("file: "__FILE__", line: %d, "
                    "unexpect end char: 0x%02x",
                    __LINE__, *endptr);
            return EINVAL;
        }

        if ((result=STORAGE_ENGINE_ADD_INODE_API(inode)) != 0) {
            return result;
        }

        p = endptr + 1;
    }

    return 0;
}

static int add_sorted_inodes()
{
    const int buffer_size = 512 * 1024;
    int result;
    int fd;
    int bytes;
    char filename[PATH_MAX];
    char *buff;
    char *last_newline;
    string_t content;
    string_t remain;

    binlog_sort_get_inodes_filename(filename, sizeof(filename));
    if ((fd=open(filename, O_RDONLY | O_CLOEXEC)) < 0) {
        result = (errno != 0 ? errno : ENOENT);
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    if ((buff=fc_malloc(buffer_size)) == NULL) {
        return ENOMEM;
    }

    result = 0;
    content.str = buff;
    remain.len = 0;
    while ((bytes=read(fd, buff + remain.len,
                    buffer_size - remain.len)) > 0)
    {
        content.len = remain.len + bytes;
        if ((last_newline=(char *)fc_memrchr(buff, '\n',
                        content.len)) == NULL)
        {
            logError("file: "__FILE__", line: %d, "
                    "sorted inode file: %s, expect new line!",
                    __LINE__, filename);
            result = EINVAL;
            break;
        }

        remain.str =  last_newline + 1;
        remain.len = (buff + content.len) - remain.str;
        content.len -= remain.len;
        if ((result=parse_add_inodes(&content)) != 0) {
            break;
        }

        if (remain.len > 0) {
            memcpy(buff, remain.str, remain.len);
        }
    }

    if (bytes < 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "read from file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
    }

    free(buff);
    close(fd);
    return result;
}

static inline const char *get_check_dump_inode_binlogs_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/.dump_inode_binlogs.flags",
            DATA_PATH_STR, FDIR_DATA_DUMP_SUBDIR_NAME);
    return filename;
}

static int check_dump_inode_binlogs()
{
    int result;
    int64_t start_time;
    char mark_filename[PATH_MAX];
    char time_buff[32];

    get_check_dump_inode_binlogs_mark_filename(
            mark_filename, sizeof(mark_filename));
    if (access(mark_filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            return 0;
        }

        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, mark_filename, result, STRERROR(result));
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "dumping inode binlogs ...", __LINE__);
    start_time = get_current_time_ms();
    if ((result=STORAGE_ENGINE_DUMP_INODE_BINLOGS_API()) != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "dump inode binlogs done. time used: %s ms", __LINE__,
            long_to_comma_str(get_current_time_ms() - start_time, time_buff));
    return fc_delete_file(mark_filename);
}

static int load_full_dump_data(const int parse_threads,
        BinlogReaderParams *params, BinlogReplayMTStat *stat)
{
    int result;
    SFBinlogFilePosition hint_pos;
    char mark_filename[PATH_MAX];

    if (params[0].last_data_version > 0) {
        if (DUMP_INODE_ADD_STATUS != inode_add_mark_status_done) {
            logError("file: "__FILE__", line: %d, "
                    "invalid inode add status: %d, expect: %d",
                    __LINE__, DUMP_INODE_ADD_STATUS,
                    inode_add_mark_status_done);
            return EINVAL;
        }

        hint_pos.index = 0;
        hint_pos.offset = 0;
        if ((result=binlog_find_position_ex(FDIR_DATA_DUMP_SUBDIR_NAME,
                        &hint_pos, params[0].last_data_version,
                        &params[0].hint_pos)) != 0)
        {
            return result;
        }
    } else {
        params[0].hint_pos.index = 0;
        params[0].hint_pos.offset = 0;
        if (DUMP_INODE_ADD_STATUS == inode_add_mark_status_none) {
            if ((result=binlog_sort_generate_inodes_file()) != 0) {
                return result;
            }

            DUMP_INODE_ADD_STATUS = inode_add_mark_status_doing;
            if ((result=inode_add_mark_save(DUMP_INODE_ADD_STATUS)) != 0) {
                return result;
            }
        }

        if (DUMP_INODE_ADD_STATUS == inode_add_mark_status_doing) {
            if ((result=add_sorted_inodes()) != 0) {
                return result;
            }

            if ((result=STORAGE_ENGINE_SAVE_SEGMENT_INDEX_API()) != 0) {
                return result;
            }

            DUMP_INODE_ADD_STATUS = inode_add_mark_status_done;
            if ((result=inode_add_mark_save(DUMP_INODE_ADD_STATUS)) != 0) {
                return result;
            }

            if ((result=binlog_sort_delete_inodes_file()) != 0) {
                return result;
            }
        }
    }

    get_check_dump_inode_binlogs_mark_filename(
            mark_filename, sizeof(mark_filename));
    if ((result=writeToFile(mark_filename, "OK", 2)) != 0) {
        return result;
    }

    if ((result=load_data(parse_threads, params, stat, true)) != 0) {
        return result;
    }

    while (event_dealer_get_last_data_version() < DUMP_DENTRY_COUNT) {
        change_notify_load_done_signal();
    }

    fc_sleep_ms(100);
    return check_dump_inode_binlogs();
}

int server_load_data()
{
    BinlogReaderParams params[2];
    BinlogReplayMTStat stats[2];
    BinlogReplayMTStat stat;
    SFBinlogFilePosition hint_pos;
    int64_t start_time;
    char time_buff[32];
    int64_t data_version;
    int parse_threads;
    int replay_count;
    bool show_prompt;
    int result;

    start_time = get_current_time_ms();

    if (5 * DATA_THREAD_COUNT <= SYSTEM_CPU_COUNT) {
        parse_threads = 4 * DATA_THREAD_COUNT;
    } else if (3 * DATA_THREAD_COUNT <= SYSTEM_CPU_COUNT) {
        parse_threads = 2 * DATA_THREAD_COUNT;
    } else {
        parse_threads = FC_MIN(DATA_THREAD_COUNT, SYSTEM_CPU_COUNT);
    }

    replay_count = 0;
    if (STORAGE_ENABLED) {
        params[0].last_data_version = event_dealer_get_last_data_version();
        if (params[0].last_data_version > 0) {
            FC_ATOMIC_SET(DATA_CURRENT_VERSION, params[0].last_data_version);
        }

        if (params[0].last_data_version >= DUMP_LAST_DATA_VERSION) {
            binlog_get_current_write_position(&params[0].hint_pos);
            params[0].subdir_name = FDIR_BINLOG_SUBDIR_NAME;
            params[1].subdir_name = NULL;
        } else {
            params[1].subdir_name = NULL;
            if (params[0].last_data_version < DUMP_DENTRY_COUNT) {
                params[0].subdir_name = FDIR_DATA_DUMP_SUBDIR_NAME;
                if ((result=load_full_dump_data(parse_threads, params,
                                stats + replay_count++)) != 0)
                {
                    return result;
                }
            } else {
                if ((result=check_dump_inode_binlogs()) != 0) {
                    return result;
                }

                FC_ATOMIC_SET(DATA_CURRENT_VERSION, DUMP_LAST_DATA_VERSION);
            }

            BINLOG_READER_SET_PARAMS(params[0], FDIR_BINLOG_SUBDIR_NAME,
                    DUMP_NEXT_POSITION, DUMP_LAST_DATA_VERSION);
        }
    } else {
        params[0].last_data_version = 0;
        if (DUMP_LAST_DATA_VERSION > 0) {
            hint_pos.index = 0;
            hint_pos.offset = 0;
            BINLOG_READER_SET_PARAMS(params[0], FDIR_DATA_DUMP_SUBDIR_NAME,
                    hint_pos, params[0].last_data_version);
            BINLOG_READER_SET_PARAMS(params[1], FDIR_BINLOG_SUBDIR_NAME,
                    DUMP_NEXT_POSITION, DUMP_LAST_DATA_VERSION);
        } else {
            BINLOG_READER_INIT_PARAMS(params[0], FDIR_BINLOG_SUBDIR_NAME);
            params[1].subdir_name = NULL;
        }
    }

    LOAD_DUMP_DONE = true;
    show_prompt = (replay_count == 0);
    if ((result=load_data(parse_threads, params, stats +
                    replay_count++, show_prompt)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED && DUMP_LAST_DATA_VERSION > 0) {
        if (event_dealer_get_last_data_version() <
                FC_ATOMIC_GET(DATA_CURRENT_VERSION))
        {
            change_notify_load_done_signal();
        }

        if (event_dealer_get_last_data_version() >= DUMP_LAST_DATA_VERSION) {
            fc_sleep_ms(10);
            if ((result=inode_add_mark_delete()) != 0) {
                return result;
            }

            if ((result=binlog_dump_clear_files()) != 0) {
                return result;
            }
        }
    }

    if (replay_count == 1) {
        stat = stats[0];
    } else {
        stat.record_count = stats[0].record_count + stats[1].record_count;
        stat.skip_count = stats[0].skip_count + stats[1].skip_count;
        stat.warning_count = stats[0].warning_count + stats[1].warning_count;
    }

    data_version = FC_ATOMIC_GET(DATA_CURRENT_VERSION);
    FC_ATOMIC_SET(BINLOG_RECORD_COUNT, stat.record_count);
    logInfo("file: "__FILE__", line: %d, "
            "load data done. record count: %"PRId64", "
            "skip count: %"PRId64", warning count: %"PRId64", "
            "current data version: %"PRId64", time used: %s ms",
            __LINE__, stat.record_count, stat.skip_count,
            stat.warning_count, data_version, long_to_comma_str(
                get_current_time_ms() - start_time, time_buff));

    binlog_write_set_next_version();
    FC_ATOMIC_SET(MY_CONFIRMED_VERSION, data_version);
    return 0;
}

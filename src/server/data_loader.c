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
#include "db/event_dealer.h"
#include "server_global.h"
#include "server_binlog.h"
#include "data_thread.h"
#include "data_loader.h"

int server_load_data()
{
    BinlogReplayMTContext replay_ctx;
    SFBinlogFilePosition pos;
    int64_t last_data_version;
    SFBinlogFilePosition *hint_pos;
    BinlogReadThreadContext reader_ctx;
    BinlogReadThreadResult *r;
    int64_t start_time;
    int64_t end_time;
    char time_buff[32];
    int parse_threads;
    int result;

    start_time = get_current_time_ms();

    if (5 * DATA_THREAD_COUNT <= SYSTEM_CPU_COUNT) {
        parse_threads = 4 * DATA_THREAD_COUNT;
    } else if (3 * DATA_THREAD_COUNT <= SYSTEM_CPU_COUNT) {
        parse_threads = 2 * DATA_THREAD_COUNT;
    } else {
        parse_threads = FC_MIN(DATA_THREAD_COUNT, SYSTEM_CPU_COUNT);
    }

    if (STORAGE_ENABLED) {
        last_data_version = event_dealer_get_last_data_version();
        if (last_data_version > 0) {
            hint_pos = &pos;
            binlog_get_current_write_position(hint_pos);
            FC_ATOMIC_SET(DATA_CURRENT_VERSION, last_data_version);
        } else {
            hint_pos = NULL;
        }
    } else {
        hint_pos = NULL;
        last_data_version = 0;
    }

    if ((result=binlog_read_thread_init_ex(&reader_ctx, hint_pos,
                    last_data_version, BINLOG_BUFFER_SIZE,
                    parse_threads * 2)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "binlog_read_thread_init fail, "
                "errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((result=binlog_replay_mt_init(&replay_ctx, &reader_ctx,
                    parse_threads)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "loading data, parse thread count: %d ...",
            __LINE__, parse_threads);

    result = 0;
    while (SF_G_CONTINUE_FLAG && replay_ctx.fail_count == 0) {
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
        if (replay_ctx.fail_count > 0) {
            result = replay_ctx.last_errno;
        }
    }

    if (result == 0) {
        end_time = get_current_time_ms();
        logInfo("file: "__FILE__", line: %d, "
                "load data done. record count: %"PRId64", "
                "skip count: %"PRId64", warning count: %"PRId64
                ", fail count: %"PRId64", current data version: %"PRId64
                ", time used: %s ms", __LINE__, replay_ctx.record_count,
                replay_ctx.skip_count, replay_ctx.warning_count,
                replay_ctx.fail_count, __sync_add_and_fetch(
                    &DATA_CURRENT_VERSION, 0), long_to_comma_str(
                        end_time - start_time, time_buff));
    }
    return result;
}

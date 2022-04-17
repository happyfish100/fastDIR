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
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_write.h"

SFBinlogWriterContext g_binlog_writer_ctx;

int binlog_write_init()
{
    const int binlog_init_buffer_size = 1024;
    const int writer_count = 1;
    const bool use_fixed_buffer_size = false;
    int result;

    if ((result=sf_binlog_writer_init_by_version(&g_binlog_writer_ctx.
                    writer, DATA_PATH_STR, FDIR_BINLOG_SUBDIR_NAME,
                    DATA_CURRENT_VERSION + 1, BINLOG_BUFFER_SIZE,
                    4096)) != 0)
    {
        return result;
    }
    sf_binlog_writer_set_flags(&g_binlog_writer_ctx.writer,
            SF_FILE_WRITER_FLAGS_WANT_DONE_VERSION);

    if ((result=sf_binlog_writer_init_thread_ex(&g_binlog_writer_ctx.thread,
                    FDIR_BINLOG_SUBDIR_NAME, &g_binlog_writer_ctx.writer,
                    SF_BINLOG_THREAD_ORDER_MODE_VARY, binlog_init_buffer_size,
                    writer_count, use_fixed_buffer_size)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_change_order_by(&g_binlog_writer_ctx.writer,
            SF_BINLOG_WRITER_TYPE_ORDER_BY_NONE);
}

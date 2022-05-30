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
#include "binlog_pack.h"
#include "binlog_dump.h"

static int init_dump_ctx(FDIRBinlogDumpContext *dump_ctx)
{
    const uint64_t next_version = 1;
    const int buffer_size = 4 * 1024 * 1024;
    const int ring_size = 1024;
    const char *subdir_name = "binlog/dump";
    char filepath[PATH_MAX];
    int result;

    sf_binlog_writer_get_filepath(DATA_PATH_STR, subdir_name,
            filepath, sizeof(filepath));
    if ((result=fc_check_mkdir(filepath, 0755)) != 0) {
        return result;
    }

    if ((result=sf_binlog_writer_init_by_version_ex(&dump_ctx->bwctx.writer,
                    DATA_PATH_STR, subdir_name, next_version, buffer_size,
                    ring_size, SF_BINLOG_NEVER_ROTATE_FILE)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_init_thread(&dump_ctx->bwctx.thread,
            subdir_name, &dump_ctx->bwctx.writer, FDIR_BINLOG_RECORD_MAX_SIZE);
}

static void destroy_dump_ctx(FDIRBinlogDumpContext *dump_ctx)
{
}

int binlog_dump_all(const char *filename)
{
    int result;
    FDIRBinlogDumpContext dump_ctx;

    if ((result=init_dump_ctx(&dump_ctx)) != 0) {
        return result;
    }

    destroy_dump_ctx(&dump_ctx);
    return result;
}

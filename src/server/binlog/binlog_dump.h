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

//binlog_dump.h

#ifndef _BINLOG_DUMP_H_
#define _BINLOG_DUMP_H_

#include "sf/sf_func.h"
#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "binlog_types.h"

struct fdir_data_thread_context;

typedef struct fdir_binlog_dump_context {
    int64_t last_data_version;   //for padding
    SFBinlogFilePosition hint_pos;
    volatile int64_t current_version;
    volatile int64_t orphan_count;
    volatile int64_t hardlink_count;
    char subdir_name[32];
    int result;
    SFSynchronizeContext sctx;
    SFBinlogWriterContext bwctx;
} FDIRBinlogDumpContext;

#ifdef __cplusplus
extern "C" {
#endif

    static inline const char *fdir_get_dump_data_filename(
            char *filename, const int size)
    {
        return sf_binlog_writer_get_filename(DATA_PATH_STR,
                FDIR_DATA_DUMP_SUBDIR_NAME, 0, filename, size);
    }

    static inline const char *fdir_get_dump_mark_filename(
            char *filename, const int size)
    {
        char filepath[PATH_MAX];
        sf_binlog_writer_get_filepath(DATA_PATH_STR,
                FDIR_DATA_DUMP_SUBDIR_NAME,
                filepath, sizeof(filepath));
        snprintf(filename, size, "%s/%s.mark",
                filepath, SF_BINLOG_FILE_PREFIX);
        return filename;
    }

    int binlog_dump_load_from_mark_file();

    int binlog_dump_all();

    /* this function is called by data thread only */
    int binlog_dump_data(struct fdir_data_thread_context *thread_ctx,
            FDIRBinlogDumpContext *dump_ctx);

#ifdef __cplusplus
}
#endif

#endif

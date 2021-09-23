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

#include "diskallocator/binlog/common/write_fd_cache.h"
#include "inode/binlog_reader.h"
#include "inode/binlog_writer.h"
#include "trunk/binlog_reader.h"
#include "trunk/binlog_writer.h"
#include "data_sync_thread.h"
#include "storage_engine.h"

static int init_write_fd_cache()
{
    const int max_idle_time = 3600;
    const int capacity = 1361;
    DABinlogTypeSubdirPair pairs[FDIR_STORAGE_BINLOG_TYPE_COUNT];
    DABinlogTypeSubdirArray type_subdir_array;

    DA_BINLOG_SET_TYPE_SUBDIR_PAIR(pairs[FDIR_STORAGE_BINLOG_TYPE_INODE],
            FDIR_STORAGE_BINLOG_TYPE_INODE, "inode",
            inode_binlog_pack_record_callback,
            inode_binlog_reader_unpack_record,
            inode_binlog_batch_update_callback,
            inode_binlog_shrink_callback);


    DA_BINLOG_SET_TYPE_SUBDIR_PAIR(pairs[FDIR_STORAGE_BINLOG_TYPE_TRUNK],
            FDIR_STORAGE_BINLOG_TYPE_TRUNK, "trunk",
            slice_binlog_pack_record_callback,
            slice_binlog_reader_unpack_record,
            NULL, NULL);

    type_subdir_array.pairs = pairs;
    type_subdir_array.count = FDIR_STORAGE_BINLOG_TYPE_COUNT;
    return da_write_fd_cache_init(&type_subdir_array,
            max_idle_time, capacity);
}

int fdir_storage_engine_init(IniFullContext *ini_ctx)
{
    int result;

    if ((result=init_write_fd_cache()) != 0) {
        return result;
    }

    if ((result=data_sync_thread_init()) != 0) {
        return result;
    }

    return 0;
}

int fdir_storage_engine_start()
{
    int result;

    if ((result=data_sync_thread_start()) != 0) {
        return result;
    }

    return 0;
}

void fdir_storage_engine_terminate()
{
}

int fdir_storage_engine_store(FDIRDBUpdateDentryArray *array)
{
    return 0;
}

int fdir_storage_engine_fetch(FDIRDBFetchDentry *dentry)
{
    return 0;
}

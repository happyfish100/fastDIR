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
#include "storage_module.h"

int storage_module_init(const int max_idle_time, const int capacity)
{
    DABinlogTypeSubdirPair pairs[FDIR_STORAGE_BINLOG_TYPE_COUNT];
    DABinlogTypeSubdirArray type_subdir_array;

    DA_BINLOG_SET_TYPE_SUBDIR_PAIR(pairs[FDIR_STORAGE_BINLOG_TYPE_INODE],
            FDIR_STORAGE_BINLOG_TYPE_INODE, "inode", NULL,
            binlog_reader_unpack_record, NULL, NULL);
    //    _pack_record, _unpack_record, _batch_update, _shrink);


    DA_BINLOG_SET_TYPE_SUBDIR_PAIR(pairs[FDIR_STORAGE_BINLOG_TYPE_TRUNK],
            FDIR_STORAGE_BINLOG_TYPE_TRUNK, "trunk", NULL, NULL, NULL, NULL);

    type_subdir_array.pairs = pairs;
    type_subdir_array.count = FDIR_STORAGE_BINLOG_TYPE_COUNT;
    return da_write_fd_cache_init(&type_subdir_array,
            max_idle_time, capacity);
}

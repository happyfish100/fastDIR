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

#ifndef _FDIR_DB_INTERFACE_H
#define _FDIR_DB_INTERFACE_H

#include "fastcommon/ini_file_reader.h"
#include "diskallocator/dio/trunk_read_thread.h"
#include "diskallocator/global.h"
#include "common/fdir_server_types.h"

typedef int (*fdir_storage_engine_init_func)(IniFullContext *ini_ctx,
        const int my_server_id, const FDIRStorageEngineConfig *db_cfg,
        const DADataConfig *data_cfg, const bool clear_segment_index);

typedef int (*fdir_storage_engine_start_func)();

typedef void (*fdir_storage_engine_terminate_func)();

typedef int (*fdir_storage_engine_add_inode_func)(const int64_t inode);

typedef int (*fdir_storage_engine_save_segment_index_func)();

typedef int (*fdir_storage_engine_dump_inode_binlogs_func)();

typedef int (*fdir_storage_engine_store_func)(const FDIRDBUpdateFieldArray *array,
        const int add_inode_flags);

typedef int (*fdir_storage_engine_redo_func)(const FDIRDBUpdateFieldArray *array);

typedef int (*fdir_storage_engine_fetch_func)(const int64_t inode,
        const int field_index, DASynchronizedReadContext *rctx);

typedef void (*fdir_storage_engine_spaces_stat_func)(DASpaceStat *ss,
        int64_t *inode_used_space, const bool include_indexes);

typedef struct fdir_storage_engine_interface {
    fdir_storage_engine_init_func init;
    fdir_storage_engine_start_func start;
    fdir_storage_engine_terminate_func terminate;
    fdir_storage_engine_add_inode_func add_inode;
    fdir_storage_engine_save_segment_index_func save_segment_index;
    fdir_storage_engine_dump_inode_binlogs_func dump_inode_binlogs;
    fdir_storage_engine_store_func store;
    fdir_storage_engine_redo_func redo;
    fdir_storage_engine_fetch_func fetch;
    fdir_storage_engine_spaces_stat_func spaces_stat;
} FDIRStorageEngineInterface;

#endif

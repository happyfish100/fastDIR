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

#ifndef _FDIR_SERVER_COMMON_TYPES_H
#define _FDIR_SERVER_COMMON_TYPES_H

#include "fastcommon/fast_buffer.h"

//piece storage field indexes
#define FDIR_PIECE_FIELD_INDEX_BASIC       0
#define FDIR_PIECE_FIELD_INDEX_CHILDREN    1
#define FDIR_PIECE_FIELD_INDEX_XATTR       2
#define FDIR_PIECE_FIELD_COUNT             3

//virtual field index for sort and check
#define FDIR_PIECE_FIELD_INDEX_FOR_REMOVE 10

#define FDIR_PIECE_FIELD_CLEAR(fields) \
    DA_PIECE_FIELD_DELETE(fields[FDIR_PIECE_FIELD_INDEX_BASIC]);    \
    DA_PIECE_FIELD_DELETE(fields[FDIR_PIECE_FIELD_INDEX_CHILDREN]); \
    DA_PIECE_FIELD_DELETE(fields[FDIR_PIECE_FIELD_INDEX_XATTR])

typedef struct fdir_db_update_field_info {
    int64_t version;   //field version, NOT data version!
    uint64_t inode;
    int64_t inc_alloc; //for dump namespaces
    int namespace_id;  //for dump namespaces
    mode_t mode;       //for dump namespaces
    DABinlogOpType op_type;
    int merge_count;
    int field_index;
    FastBuffer *buffer;
    void *args;   //dentry
    struct fdir_db_update_field_info *next;  //for queue
} FDIRDBUpdateFieldInfo;

typedef struct fdir_db_update_field_array {
    FDIRDBUpdateFieldInfo *entries;
    int count;
    int alloc;
} FDIRDBUpdateFieldArray;

typedef struct fdir_storage_engine_config {
    int inode_binlog_subdirs;
    int index_dump_interval;
    int64_t memory_limit;   //limit for inode
    TimeInfo index_dump_base_time;
    string_t path;   //data path
} FDIRStorageEngineConfig;

#endif

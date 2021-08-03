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

#ifndef _FDIR_STORAGE_TYPES_H
#define _FDIR_STORAGE_TYPES_H

#include "fastcommon/common_define.h"
#include "sf/sf_types.h"

#define FDIR_STORAGE_BATCH_INODES     (64 * 1024)
#define FDIR_STORAGE_INODE_BINLOGS_PER_DIR  1024

typedef struct fdir_storage_inode_index_info {
    uint64_t inode;
    int file_id;
    int offset;
} FDIRStorageInodeIndexInfo;

typedef struct fdir_storage_inode_index_array {
    FDIRStorageInodeIndexInfo *inodes;
    int count;
} FDIRStorageInodeIndexArray;

typedef enum fdir_storage_inode_index_op_type {
    inode_index_op_type_create = 'c',
    inode_index_op_type_remove = 'r'
} FDIRStorageInodeIndexOpType;

typedef struct fdir_storage_inode_segment_info {
    int binlog_id;
    struct {
        volatile uint64_t first;
        volatile uint64_t last;
    } inodes;
} FDIRStorageInodeSegmentInfo;

typedef struct fdir_storage_inode_segment_array {
    FDIRStorageInodeSegmentInfo *segments;
    int count;
} FDIRStorageInodeSegmentArray;

#endif

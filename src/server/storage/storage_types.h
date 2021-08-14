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

#define FDIR_STORAGE_BATCH_INODE_BITS   16
#define FDIR_STORAGE_BATCH_INODE_COUNT  (1 << FDIR_STORAGE_BATCH_INODE_BITS)

#define FDIR_STORAGE_INODE_STATUS_NORMAL   0
#define FDIR_STORAGE_INODE_STATUS_DELETED  1

#define FDIR_STORAGE_SEGMENT_STATUS_CLEAN    0
#define FDIR_STORAGE_SEGMENT_STATUS_LOADING  1
#define FDIR_STORAGE_SEGMENT_STATUS_READY    2

#define FDIR_INODE_BINLOG_RECORD_MAX_SIZE  64

typedef struct fdir_storage_inode_index_info {
    int64_t version;
    uint64_t inode;
    int64_t file_id;
    int offset;
    int status;
} FDIRStorageInodeIndexInfo;

typedef struct fdir_storage_inode_index_array {
    FDIRStorageInodeIndexInfo *inodes;
    int alloc;
    struct {
        int total;
        int deleted;
        int adding;
    } counts;
} FDIRStorageInodeIndexArray;

typedef enum fdir_storage_inode_index_op_type {
    inode_index_op_type_create = 'c',
    inode_index_op_type_remove = 'r',
    inode_index_op_type_synchronize = 's'
} FDIRStorageInodeIndexOpType;

typedef struct fdir_inode_binlog_record {
    uint64_t binlog_id;
    int64_t version;  //for stable sort
    FDIRStorageInodeIndexOpType op_type;
    FDIRStorageInodeIndexInfo inode_index;
    void *args;
    struct fdir_inode_binlog_record *next;  //for queue
} FDIRInodeBinlogRecord;

typedef struct fdir_inode_segment_index_info {
    int64_t binlog_id;
    struct {
        uint64_t first;
        uint64_t last;
        FDIRStorageInodeIndexArray array;
        short status;
        union {
            short value;
            struct {
                bool in_memory : 1;
                bool dirty : 1;
            };
        } flags;
        volatile int updating_count;
    } inodes;
    time_t last_access_time;
    pthread_lock_cond_pair_t lcp;
} FDIRInodeSegmentIndexInfo;


#define FDIR_BINLOG_PARSE_INT_SILENCE(var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            sprintf(error_info, "invalid %s: %.*s",  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)


#endif

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

//binlog_index.h

#ifndef _INODE_BINLOG_INDEX_H_
#define _INODE_BINLOG_INDEX_H_

#include "inode_types.h"

typedef struct fdir_inode_binlog_index_info {
    int64_t binlog_id;
    struct {
        volatile uint64_t first;
        volatile uint64_t last;
    } inodes;
} FDIRInodeBinlogIndexInfo;

typedef struct fdir_inode_binlog_index_array {
    FDIRInodeBinlogIndexInfo *indexes;
    int alloc;
    int count;
} FDIRInodeBinlogIndexArray;

typedef struct fdir_inode_binlog_index_context {
    FDIRInodeBinlogIndexArray index_array;
    int64_t last_version;
} FDIRInodeBinlogIndexContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_index_load(FDIRInodeBinlogIndexContext *ctx);

int binlog_index_save(FDIRInodeBinlogIndexContext *ctx);

static inline void binlog_index_free(FDIRInodeBinlogIndexContext *ctx)
{
    if (ctx->index_array.indexes != NULL) {
        free(ctx->index_array.indexes);
        ctx->index_array.indexes = NULL;
        ctx->index_array.alloc = ctx->index_array.count = 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif

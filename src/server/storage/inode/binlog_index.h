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

#include "sf/sf_binlog_index.h"
#include "inode_types.h"

typedef struct fdir_inode_binlog_index_info {
    int64_t binlog_id;
    struct {
        volatile uint64_t first;
        volatile uint64_t last;
    } inodes;
} FDIRInodeBinlogIndexInfo;

#ifdef __cplusplus
extern "C" {
#endif

extern SFBinlogIndexContext g_binlog_index_ctx;

void binlog_index_init();

static inline int binlog_index_load()
{
    return sf_binlog_index_load(&g_binlog_index_ctx);
}

static inline int binlog_index_save()
{
    return sf_binlog_index_save(&g_binlog_index_ctx);
}

static inline int binlog_index_expand()
{
    return sf_binlog_index_expand(&g_binlog_index_ctx);
}

static inline void binlog_index_free()
{
    sf_binlog_index_free(&g_binlog_index_ctx);
}

#ifdef __cplusplus
}
#endif

#endif

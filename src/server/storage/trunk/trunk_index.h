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

//trunk_index.h

#ifndef _TRUNK_INDEX_H_
#define _TRUNK_INDEX_H_

#include "sf/sf_binlog_index.h"
#include "trunk_types.h"

typedef struct fdir_trunk_index_info {
    int64_t version;
    int trunk_id;
    int file_size;
    int used_bytes;
    int free_start;
} FDIRTrunkIndexInfo;

#ifdef __cplusplus
extern "C" {
#endif

extern SFBinlogIndexContext g_trunk_index_ctx;

void trunk_index_init();

static inline int trunk_index_load()
{
    return sf_binlog_index_load(&g_trunk_index_ctx);
}

static inline int trunk_index_save()
{
    return sf_binlog_index_save(&g_trunk_index_ctx);
}

static inline int trunk_index_expand()
{
    return sf_binlog_index_expand(&g_trunk_index_ctx);
}

static inline void trunk_index_free()
{
    sf_binlog_index_free(&g_trunk_index_ctx);
}

#ifdef __cplusplus
}
#endif

#endif

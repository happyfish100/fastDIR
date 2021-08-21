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

#include "trunk_types.h"

typedef struct fdir_trunk_index_info {
    int64_t version;
    int trunk_id;
    int file_size;
    int used_bytes;
    int free_start;
} FDIRTrunkIndexInfo;

typedef struct fdir_trunk_index_array {
    FDIRTrunkIndexInfo *trunks;
    int alloc;
    int count;
} FDIRTrunkIndexArray;

typedef struct fdir_trunk_index_context {
    FDIRTrunkIndexArray index_array;
    int64_t last_version;
} FDIRTrunkIndexContext;

#ifdef __cplusplus
extern "C" {
#endif

int trunk_index_load(FDIRTrunkIndexContext *ctx);

int trunk_index_save(FDIRTrunkIndexContext *ctx);

int trunk_index_expand(FDIRTrunkIndexContext *ctx);

static inline void trunk_index_free(FDIRTrunkIndexContext *ctx)
{
    if (ctx->index_array.trunks != NULL) {
        free(ctx->index_array.trunks);
        ctx->index_array.trunks = NULL;
        ctx->index_array.alloc = ctx->index_array.count = 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif

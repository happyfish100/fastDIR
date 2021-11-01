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

#include "fastcommon/fast_allocator.h"
#include "segment_index.h"
#include "inode_array_allocator.h"

#define STORAGE_BATCH_INODE_START_BIT     6
#define STORAGE_BATCH_INODE_START_COUNT  (1 << STORAGE_BATCH_INODE_START_BIT)

static struct fast_allocator_context array_allocator_ctx;

int inode_array_allocator_init(const int64_t memory_limit)
{
    const int reclaim_interval = 0;
    struct fast_region_info regions[FDIR_STORAGE_BATCH_INODE_BITS];
    struct fast_region_info *region;
    int bit;
    int start;
    int end;
    int alloc_elements_once;
    int step;

    start = 0;
    alloc_elements_once = 2 * FDIR_STORAGE_BATCH_INODE_BITS;
    for (bit=STORAGE_BATCH_INODE_START_BIT, region=regions;
            bit<=FDIR_STORAGE_BATCH_INODE_BITS;
            bit++, region++)
    {
        end = (1 << bit) * sizeof(FDIRStorageInodeIndexInfo);
        step = end - start;
        FAST_ALLOCATOR_INIT_REGION(*region, start,
                end, step, alloc_elements_once);
        if ((bit + 1) % 2 == 0 && alloc_elements_once >= 2) {
            alloc_elements_once /= 2;
        }

        start = end;
    }

    return fast_allocator_init_ex(&array_allocator_ctx,
            "inode-idx-arr", regions, region - regions,
            memory_limit, 0.9999, reclaim_interval, true);
}

FDIRStorageInodeIndexInfo *inode_array_allocator_alloc(
        const int count, int *alloc)
{
    int bytes;
    int64_t total_reclaim_bytes;
    FDIRStorageInodeIndexInfo *inodes;

    if (count <= STORAGE_BATCH_INODE_START_COUNT) {
        *alloc = STORAGE_BATCH_INODE_START_COUNT;
    } else if (is_power2(count)) {
        *alloc = count;
    } else {
        *alloc = STORAGE_BATCH_INODE_START_COUNT;
        while (*alloc < count) {
            *alloc *= 2;
        }
    }

    bytes = (*alloc) * sizeof(FDIRStorageInodeIndexInfo);
    while (1) {
        if ((inodes=(FDIRStorageInodeIndexInfo *)fast_allocator_alloc(
                        &array_allocator_ctx, bytes)) != NULL)
        {
            return inodes;
        }
        if (inode_segment_index_eliminate(8 *
                    FDIR_STORAGE_BATCH_INODE_COUNT) != 0)
        {
            return NULL;
        }

        fast_allocator_retry_reclaim(&array_allocator_ctx,
                &total_reclaim_bytes);
    }
}

void inode_array_allocator_free(FDIRStorageInodeIndexInfo *inodes)
{
    fast_allocator_free(&array_allocator_ctx, inodes);
}

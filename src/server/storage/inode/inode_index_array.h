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

//inode_index_array.h

#ifndef _INODE_INDEX_ARRAY_H_
#define _INODE_INDEX_ARRAY_H_

#include "../storage_types.h"
#include "inode_array_allocator.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline int inode_index_array_alloc(FDIRStorageInodeIndexArray
        *index_array, const int count)
{
    if ((index_array->inodes=inode_array_allocator_alloc(count,
                    &index_array->alloc)) == NULL)
    {
        return ENOMEM;
    }

    return 0;
}

int inode_index_array_add(FDIRStorageInodeIndexArray *array,
        const FDIRStorageInodeIndexInfo *node);

#define inode_index_array_delete(array, inode) \
    inode_index_array_delete_ex(array, inode, false)

int inode_index_array_delete_ex(FDIRStorageInodeIndexArray *array,
        const uint64_t inode, const bool shrink_memory);

int inode_index_array_check_shrink(FDIRStorageInodeIndexArray *array);

int inode_index_array_find(FDIRStorageInodeIndexArray *array,
        FDIRStorageInodeIndexInfo *inode);

#ifdef __cplusplus
}
#endif

#endif

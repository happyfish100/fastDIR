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

#ifndef _INODE_ARRAY_ALLOCATOR_H
#define _INODE_ARRAY_ALLOCATOR_H

#include "../storage_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int inode_array_allocator_init(const int64_t memory_limit);

    FDIRStorageInodeIndexInfo *inode_array_allocator_alloc(
            const int count, int *alloc);

    void inode_array_allocator_free(FDIRStorageInodeIndexInfo *inodes);

#ifdef __cplusplus
}
#endif

#endif
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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "inode_index_array.h"

int inode_index_array_add(FDIRStorageInodeIndexArray *array,
        const FDIRStorageInodeIndexInfo *node)
{
    FDIRStorageInodeIndexInfo *dest;

    if (array->alloc <= array->counts.total) {
        logError("file: "__FILE__", line: %d, "
                "too many inodes exceeds allocated: %d",
                __LINE__, array->alloc);
        return EOVERFLOW;
    }

    dest = array->inodes + array->counts.total++;
    *dest = *node;
    dest->status = FDIR_STORAGE_INODE_STATUS_NORMAL;
    return 0;
}

static int inode_index_array_compare(const FDIRStorageInodeIndexInfo *node1,
        const FDIRStorageInodeIndexInfo *node2)
{
    return fc_compare_int64(node1->inode, node2->inode);
}

static inline FDIRStorageInodeIndexInfo *inode_index_array_get(
        FDIRStorageInodeIndexArray *array, const uint64_t inode)
{
    FDIRStorageInodeIndexInfo target;
    FDIRStorageInodeIndexInfo *node;

    target.inode = inode;
    node = (FDIRStorageInodeIndexInfo *)bsearch(&target, array->inodes,
            array->counts.total, sizeof(FDIRStorageInodeIndexInfo),
            (int (*)(const void *, const void *))inode_index_array_compare);
    if (node == NULL || node->status != FDIR_STORAGE_INODE_STATUS_NORMAL) {
        return NULL;
    }

    return node;
}

int inode_index_array_check_shrink(FDIRStorageInodeIndexArray *array)
{
    int result;
    int count;
    FDIRStorageInodeIndexArray new_array;
    FDIRStorageInodeIndexInfo *src;
    FDIRStorageInodeIndexInfo *end;
    FDIRStorageInodeIndexInfo *dest;

    if ((count=array->counts.total - array->counts.deleted) > 0) {
        if (count > array->counts.deleted) {
            return 0;
        }
        if ((result=inode_index_array_alloc(&new_array, count)) != 0) {
            return result;
        }

        dest = new_array.inodes;
        end = array->inodes + array->counts.total;
        for (src=array->inodes; src<end; src++) {
            if (src->status == FDIR_STORAGE_INODE_STATUS_NORMAL) {
                *dest++ = *src;
            }
        }

        new_array.counts.total = dest - new_array.inodes;
        new_array.counts.deleted = 0;
        inode_array_allocator_free(array->inodes);
        *array = new_array;
    } else {
        inode_array_allocator_free(array->inodes);
        array->inodes = NULL;
        array->alloc = 0;
        array->counts.total = array->counts.deleted = 0;
    }

    return 0;
}

int inode_index_array_delete(FDIRStorageInodeIndexArray *array,
        const uint64_t inode)
{
    FDIRStorageInodeIndexInfo *node;

    if ((node=inode_index_array_get(array, inode)) == NULL) {
        return ENOENT;
    }

    node->status = FDIR_STORAGE_INODE_STATUS_DELETED;
    array->counts.deleted++;
    return 0;
}

int inode_index_array_find(FDIRStorageInodeIndexArray *array,
        FDIRStorageInodeIndexInfo *inode)
{
    FDIRStorageInodeIndexInfo *node;

    node = (FDIRStorageInodeIndexInfo *)bsearch(inode, array->inodes,
            array->counts.total, sizeof(FDIRStorageInodeIndexInfo),
            (int (*)(const void *, const void *))inode_index_array_compare);
    if (node == NULL || node->status != FDIR_STORAGE_INODE_STATUS_NORMAL) {
        return ENOENT;
    }

    *inode = *node;
    return 0;
}
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
#include "slice_array.h"

int slice_array_add(FDIRTrunkSliceArray *array,
        const FDIRTrunkSliceInfo *slice)
{
    FDIRTrunkSliceInfo *dest;

    if (array->alloc <= array->counts.total) {
        logError("file: "__FILE__", line: %d, "
                "too many slices exceeds allocated: %d",
                __LINE__, array->alloc);
        return EOVERFLOW;
    }

    dest = array->slices + array->counts.total++;
    *dest = *slice;
    dest->status = FDIR_STORAGE_INODE_STATUS_NORMAL;
    return 0;
}

static int slice_array_compare(const FDIRTrunkSliceInfo *slice1,
        const FDIRTrunkSliceInfo *slice2)
{
    int sub;
    sub = (int)slice1->offset - (int)slice2->offset;
    if (sub == 0) {
        return (int)slice1->size - (int)slice2->size;
    } else {
        return sub;
    }
}

static inline FDIRTrunkSliceInfo *slice_array_get(
        FDIRTrunkSliceArray *array, const int offset, const int size)
{
    FDIRTrunkSliceInfo target;
    FDIRTrunkSliceInfo *slice;

    target.offset = offset;
    target.size = size;
    slice = (FDIRTrunkSliceInfo *)bsearch(&target, array->slices,
            array->counts.total, sizeof(FDIRTrunkSliceInfo),
            (int (*)(const void *, const void *))slice_array_compare);
    if (slice == NULL || slice->status != FDIR_STORAGE_INODE_STATUS_NORMAL) {
        return NULL;
    }

    return slice;
}

int slice_array_check_shrink(FDIRTrunkSliceArray *array)
{
    int result;
    int count;
    FDIRTrunkSliceArray new_array;
    FDIRTrunkSliceInfo *src;
    FDIRTrunkSliceInfo *end;
    FDIRTrunkSliceInfo *dest;

    if ((count=array->counts.total - array->counts.deleted) > 0) {
        if (count > array->counts.deleted) {
            return 0;
        }
        if ((result=slice_array_alloc(&new_array, count)) != 0) {
            return result;
        }

        dest = new_array.slices;
        end = array->slices + array->counts.total;
        for (src=array->slices; src<end; src++) {
            if (src->status == FDIR_STORAGE_INODE_STATUS_NORMAL) {
                *dest++ = *src;
            }
        }

        new_array.counts.total = dest - new_array.slices;
        new_array.counts.deleted = 0;
        free(array->slices);
        *array = new_array;
    } else {
        slice_array_free(array);
    }

    return 0;
}

int slice_array_delete(FDIRTrunkSliceArray *array,
        const int offset, const int size)
{
    FDIRTrunkSliceInfo *slice;

    if ((slice=slice_array_get(array, offset, size)) == NULL) {
        return ENOENT;
    }

    slice->status = FDIR_STORAGE_INODE_STATUS_DELETED;
    array->counts.deleted++;
    return 0;
}

int slice_array_find(FDIRTrunkSliceArray *array,
        FDIRTrunkSliceInfo *slice)
{
    FDIRTrunkSliceInfo *node;

    node = (FDIRTrunkSliceInfo *)bsearch(slice, array->slices,
            array->counts.total, sizeof(FDIRTrunkSliceInfo),
            (int (*)(const void *, const void *))slice_array_compare);
    if (node == NULL || node->status != FDIR_STORAGE_INODE_STATUS_NORMAL) {
        return ENOENT;
    }

    *slice = *node;
    return 0;
}

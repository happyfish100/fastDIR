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

//slice_array.h

#ifndef _TRUNK_SLICE_ARRAY_H_
#define _TRUNK_SLICE_ARRAY_H_

#include "trunk_types.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline int slice_array_alloc(FDIRTrunkSliceArray *array,
        const int count)
{
    array->alloc = 64;
    while (array->alloc < count) {
        array->alloc *= 2;
    }
    if ((array->slices=fc_malloc(sizeof(FDIRTrunkSliceInfo) *
                    array->alloc)) == NULL)
    {
        return ENOMEM;
    }
    array->counts.total = array->counts.deleted = 0;

    return 0;
}

static inline void slice_array_free(FDIRTrunkSliceArray *array)
{
    if (array->slices != NULL) {
        free(array->slices);
        array->slices = NULL;
        array->alloc = 0;
        array->counts.total = array->counts.deleted = 0;
    }
}

int slice_array_add(FDIRTrunkSliceArray *array,
        const FDIRTrunkSliceInfo *slice);

int slice_array_delete(FDIRTrunkSliceArray *array,
        const int offset, const int size);

int slice_array_check_shrink(FDIRTrunkSliceArray *array);

int slice_array_find(FDIRTrunkSliceArray *array,
        FDIRTrunkSliceInfo *slice);

#ifdef __cplusplus
}
#endif

#endif

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

#ifndef _STORAGE_TRUNK_TYPES_H
#define _STORAGE_TRUNK_TYPES_H

#include "../storage_types.h"

#define FDIR_SLICE_BINLOG_RECORD_MAX_SIZE  64

typedef struct fdir_trunk_slice_info {
    int64_t version;
    int offset;
    int size;
    int status;
} FDIRTrunkSliceInfo;

typedef struct fdir_trunk_slice_array {
    FDIRTrunkSliceInfo *slices;
    int alloc;
    struct {
        int total;
        int deleted;
        int adding;
    } counts;
} FDIRTrunkSliceArray;

typedef struct fdir_trunk_info {
    DABinlogWriter writer;
    struct {
        FDIRTrunkSliceArray array;
        short status;
    } slices;

    int file_size;
    int used_bytes;
    int free_start;
} FDIRTrunkInfo;

#endif

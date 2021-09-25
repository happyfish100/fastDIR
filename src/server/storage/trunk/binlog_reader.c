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

#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_binlog_index.h"
#include "../storage_global.h"
#include "diskallocator/binlog/common/binlog_reader.h"
//#include "binlog_writer.h"
#include "slice_array.h"
#include "binlog_reader.h"

#define SLICE_BINLOG_FIELD_COUNT   4

#define SLICE_FIELD_INDEX_VERSION  0
#define SLICE_FIELD_INDEX_OP_TYPE  1
#define SLICE_FIELD_INDEX_OFFSET   2
#define SLICE_FIELD_INDEX_SIZE     3

static int binlog_parse(const string_t *line, DABinlogOpType *op_type,
        FDIRTrunkSliceInfo *slice, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[SLICE_BINLOG_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            SLICE_BINLOG_FIELD_COUNT, false);
    if (count != SLICE_BINLOG_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, SLICE_BINLOG_FIELD_COUNT);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(slice->version,
            "version", SLICE_FIELD_INDEX_VERSION, ' ', 0);
    *op_type = cols[SLICE_FIELD_INDEX_OP_TYPE].str[0];
    if (!((*op_type == da_binlog_op_type_create) ||
                (*op_type == da_binlog_op_type_remove)))
    {
        sprintf(error_info, "unkown op type: %d (0x%02x)",
                *op_type, (unsigned char)*op_type);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(slice->offset, "offset",
            SLICE_FIELD_INDEX_OFFSET, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(slice->size, "size",
            SLICE_FIELD_INDEX_SIZE, '\n', 0);
    return 0;
}

int slice_binlog_reader_unpack_record(const string_t *line,
        void *args, char *error_info)
{
    int result;
    FDIRTrunkInfo *trunk;
    FDIRTrunkSliceInfo *slice;
    DABinlogOpType op_type;

    trunk = (FDIRTrunkInfo *)args;
    slice = trunk->slices.array.slices +
        trunk->slices.array.counts.total;
    if ((result=binlog_parse(line, &op_type, slice, error_info)) != 0) {
        return result;
    }

    if (op_type == da_binlog_op_type_create) {
        if ((result=slice_array_add(&trunk->
                        slices.array, slice)) != 0)
        {
            *error_info = '\0';
        }
    } else {
        if ((result=slice_array_delete(&trunk->slices.array,
                        slice->offset, slice->size)) != 0)
        {
            if (result == ENOENT) {
                result = 0;
            } else {
                *error_info = '\0';
            }
        }
    }

    return result;
}

int slice_binlog_reader_load(FDIRTrunkInfo *trunk)
{
    int result;

    //TODO
    result = 0;
    /*
    if ((result=slice_array_alloc(&trunk->slices.array,
                    FDIR_STORAGE_BATCH_INODE_COUNT)) != 0)
    {
        return result;
    }

    if ((result=da_binlog_reader_load(&trunk->writer.key, trunk)) != 0) {
        return result;
    }
    */

    /*
    if (2 * trunk->slices.array.counts.deleted >=
            trunk->slices.array.counts.total)
    {
        return slice_binlog_writer_shrink(trunk);
    }
    */

    return 0;
}

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
#include "write_fd_cache.h"
#include "inode_index_array.h"
#include "binlog_writer.h"
#include "binlog_reader.h"

#define BINLOG_MIN_FIELD_COUNT         4
#define BINLOG_MAX_FIELD_COUNT        10

#define BINLOG_FIELD_INDEX_TIMESTAMP   0
#define BINLOG_FIELD_INDEX_VERSION     1
#define BINLOG_FIELD_INDEX_INODE       2
#define BINLOG_FIELD_INDEX_OP_TYPE     3
#define BINLOG_FIELD_INDEX_SOURCE      4
#define BINLOG_FIELD_INDEX_FINDEX      5
#define BINLOG_FIELD_INDEX_TRUNK_ID    6
#define BINLOG_FIELD_INDEX_LENGTH      7
#define BINLOG_FIELD_INDEX_OFFSET      8
#define BINLOG_FIELD_INDEX_SIZE        9

int inode_binlog_parse_record(const string_t *line,
        DAPieceFieldInfo *field, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_MAX_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (count < BINLOG_MIN_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, BINLOG_MIN_FIELD_COUNT);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(field->storage.version,
            "version", BINLOG_FIELD_INDEX_VERSION, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(field->oid, "inode",
            BINLOG_FIELD_INDEX_INODE, ' ', 0);
    field->op_type = cols[BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    if (field->op_type == da_binlog_op_type_create ||
            field->op_type == da_binlog_op_type_update)
    {
        if (count != BINLOG_MAX_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MAX_FIELD_COUNT);
            return EINVAL;
        }

        field->source = cols[BINLOG_FIELD_INDEX_SOURCE].str[0];
        if (!(field->source == DA_FIELD_UPDATE_SOURCE_NORMAL ||
                    field->source == DA_FIELD_UPDATE_SOURCE_RECLAIM))
        {
            sprintf(error_info, "unkown source: %d (0x%02x)",
                    field->source, field->source);
            return EINVAL;
        }

        SF_BINLOG_PARSE_INT_SILENCE(field->fid, "field index",
                BINLOG_FIELD_INDEX_FINDEX, ' ', 0);
        SF_BINLOG_PARSE_INT_SILENCE(field->storage.trunk_id,
                "trunk id", BINLOG_FIELD_INDEX_TRUNK_ID, ' ', 0);
        SF_BINLOG_PARSE_INT_SILENCE(field->storage.length, "length",
                BINLOG_FIELD_INDEX_LENGTH, ' ', 0);
        SF_BINLOG_PARSE_INT_SILENCE(field->storage.offset, "offset",
                BINLOG_FIELD_INDEX_OFFSET, ' ', 0);
        SF_BINLOG_PARSE_INT_SILENCE(field->storage.size, "size",
                BINLOG_FIELD_INDEX_SIZE, '\n', 0);
    } else if (field->op_type == da_binlog_op_type_remove) {
        if (count != BINLOG_MIN_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MIN_FIELD_COUNT);
            return EINVAL;
        }
    } else {
        sprintf(error_info, "unkown op type: %d (0x%02x)",
                field->op_type, (unsigned char)field->op_type);
        return EINVAL;
    }

    return 0;
}

int inode_binlog_reader_unpack_record(const string_t *line,
        void *args, char *error_info)
{
    const bool normal_update = true;
    int result;
    const char *caption = NULL;
    FDIRInodeSegmentIndexInfo *segment;
    FDIRStorageInodeIndexInfo inode;
    DAPieceFieldInfo field;
    DAPieceFieldStorage old;
    bool modified;

    segment = (FDIRInodeSegmentIndexInfo *)args;
    if ((result=inode_binlog_parse_record(line,
                    &field, error_info)) != 0)
    {
        return result;
    }

    if (field.op_type == da_binlog_op_type_create) {
        if ((result=inode_index_array_add(&segment->
                        inodes.array, &field)) != 0)
        {
            caption = "add";
        }
    } else if (field.op_type == da_binlog_op_type_update) {
        if ((result=inode_index_array_update(&segment->inodes.array,
                        &field, normal_update, &old, &modified)) != 0)
        {
            caption = "update";
        }
    } else {
        inode.inode = field.oid;
        if ((result=inode_index_array_delete(&segment->
                        inodes.array, &inode)) != 0)
        {
            if (result == ENOENT) {
                result = 0;
            } else {
                caption = "delete";
            }
        }
    }

    if (result != 0) {
        sprintf(error_info, "%s inode %"PRId64" fail, "
                "errno: %d, error info: %s", caption,
                field.oid, result, STRERROR(result));
    }
    return result;
}

int inode_binlog_reader_load_segment(FDIRInodeSegmentIndexInfo *segment)
{
    int result;
    DABinlogIdTypePair id_type_pair;

    if ((result=inode_index_array_alloc(&segment->inodes.array,
                    FDIR_STORAGE_BATCH_INODE_COUNT)) != 0)
    {
        return result;
    }

    DA_SET_BINLOG_ID_TYPE(id_type_pair, segment->binlog_id,
            FDIR_STORAGE_BINLOG_TYPE_INODE);
    if ((result=da_binlog_reader_load(&id_type_pair, segment)) != 0) {
        return result;
    }

    if (2 * segment->inodes.array.counts.deleted >=
            segment->inodes.array.counts.total)
    {
        return inode_binlog_writer_shrink(segment);
    }

    return 0;
}

static int load(const char *filename, const string_t *context,
        DAPieceFieldArray *array)
{
    int result;
    int line_count;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;
    DAPieceFieldInfo *field;
    char error_info[256];

    line_count = 0;
    result = 0;
    *error_info = '\0';
    field = array->records;
    line_start = context->str;
    buff_end = context->str + context->len;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        ++line_count;
        ++line_end;
        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=inode_binlog_parse_record(&line,
                        field, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "parse record fail, filename: %s, line no: %d%s%s",
                    __LINE__, filename, line_count, (*error_info != '\0' ?
                        ", error info: " : ""), error_info);
            break;
        }

        line_start = line_end;
        field++;
    }

    array->count = field - array->records;
    return result;
}

int inode_binlog_reader_load(const char *filename, DAPieceFieldArray *array)
{
    int result;
    int row_count;
    int64_t file_size;
    string_t context;

    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            array->records = NULL;
            array->count = 0;
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "access file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result,STRERROR(result));
            return result;
        }
    }

    if ((result=getFileContent(filename, &context.str, &file_size)) != 0) {
        return result;
    }
    context.len = file_size;

    row_count = getOccurCount(context.str, '\n');
    array->records = (DAPieceFieldInfo *)fc_malloc(
            sizeof(DAPieceFieldInfo) * row_count);
    if (array->records == NULL) {
        result = ENOMEM;
    } else {
        result = load(filename, &context, array);
    }

    free(context.str);
    return result;
}

int inode_binlog_reader_get_inode_range(const uint64_t binlog_id,
        uint64_t *first_inode, uint64_t *last_inode)
{
    int result;
    DAPieceFieldArray array;
    DAPieceFieldInfo *field;
    DAPieceFieldInfo *end;
    char filename[PATH_MAX];
    write_fd_cache_filename(binlog_id, filename, sizeof(filename));

    if ((result=inode_binlog_reader_load(filename, &array)) != 0) {
        return result;
    }

    if (array.count == 0) {
        *first_inode = *last_inode = 0;
        return ENOENT;
    }

    *first_inode = UINT64_MAX;
    *last_inode = 0;
    end = array.records + array.count;
    for (field=array.records; field<end; field++) {
        if (field->op_type == da_binlog_op_type_create) {
            if (field->oid < *first_inode) {
                *first_inode = field->oid;
            }

            if (field->oid > *last_inode) {
                *last_inode = field->oid;
            }
        }
    }

    free(array.records);
    return 0;
}

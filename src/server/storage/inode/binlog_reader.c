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
        sprintf(error_info, "%s inode fail, errno: %d, error info: %s",
                caption, result, STRERROR(result));
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

int inode_binlog_reader_get_first_inode(const uint64_t binlog_id,
        int64_t *inode)
{
    char filename[PATH_MAX];
    char buff[FDIR_INODE_BINLOG_RECORD_MAX_SIZE];
    char error_info[SF_ERROR_INFO_SIZE];
    char *line_end;
    int result;
    int64_t bytes;
    string_t line;
    DAPieceFieldInfo field;

    write_fd_cache_filename(binlog_id, filename, sizeof(filename));

    *error_info = '\0';
    bytes = sizeof(buff);
    if ((result=getFileContentEx(filename, buff, 0, &bytes)) != 0) {
        return result;
    }
    
    line.str = buff;
    line_end = memchr(buff, '\n', bytes);
    if (line_end == NULL) {
        result = EINVAL;
        sprintf(error_info, "expect new line char(\\n)");
    } else {
        line.len = (line_end - line.str) + 1;
        if ((result=inode_binlog_parse_record(&line,
                        &field, error_info)) == 0)
        {
            if (field.op_type != da_binlog_op_type_create) {
                result = EINVAL;
                sprintf(error_info, "unexpect op type: %c", field.op_type);
            }
        }
    }

    if (result == 0) {
        *inode = field.oid;
    } else {
        logError("file: "__FILE__", line: %d, "
                "get first inode fail, binlog id: %"PRId64", "
                "binlog file: %s, error info: %s",
                __LINE__, binlog_id, filename, error_info);
    }

    return result;
}

static inline int parse_created_inode(const uint64_t binlog_id,
        const char *filename, string_t *line, int64_t *inode)
{
    int result;
    char error_info[SF_ERROR_INFO_SIZE];
    DAPieceFieldInfo field;

    if ((result=inode_binlog_parse_record(line,
                    &field, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "parse last line fail, binlog id: %"PRId64", "
                "binlog file: %s, error info: %s",
                __LINE__, binlog_id, filename, error_info);
        return result;
    }

    if (field.op_type == da_binlog_op_type_create) {
        *inode = field.oid;
        return 0;
    } else {
        return EAGAIN;
    }
}

static int reverse_detect_created_inode(const uint64_t binlog_id,
        const char *filename, string_t *content, int64_t *inode)
{
    string_t line;
    char *line_end;
    int remain_len;
    int result;

    remain_len = content->len - 1;  //skip last \n
    line_end = content->str + remain_len;
    while (remain_len > 0) {
        line.str = (char *)fc_memrchr(content->str, '\n', remain_len);
        if (line.str == NULL) {
            line.str = content->str;
        }

        line.len = (line_end - line.str) + 1;
        result = parse_created_inode(binlog_id, filename, &line, inode);
        if (result != EAGAIN) {
            return result;
        }

        remain_len = line.str - content->str;
        line_end = line.str;
    }

    return EAGAIN;
}

static int detect_last_created_inode(const uint64_t binlog_id,
        const char *filename, const int64_t file_size, int64_t *inode)
{
    char buff[16 * 1024];
    string_t content;
    int64_t offset;
    int64_t read_bytes;
    int remain_len;
    int fd;
    int result;

    fd = open(filename, O_RDONLY);
    if (fd < 0) {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    result = EAGAIN;
    remain_len = file_size;
    while (remain_len > 0) {
        if (remain_len >= sizeof(buff)) {
            offset = (remain_len - sizeof(buff)) + 1;
        } else {
            offset = 0;
        }
        read_bytes = (remain_len - offset) + 1;
        if ((result=getFileContentEx1(fd, filename, buff,
                        offset, &read_bytes)) != 0)
        {
            break;
        }

        if (offset == 0) {
            content.str = buff;
            content.len = read_bytes;
        } else {
            content.str = memchr(buff, '\n', read_bytes);
            if (content.str == NULL) {
                logError("file: "__FILE__", line: %d, "
                        "binlog id: %"PRId64", binlog file: %s, "
                        "offset: %"PRId64", length: %"PRId64", "
                        "expect new line char (\\n)", __LINE__,
                        binlog_id, filename, offset, read_bytes);
                result = EINVAL;
                break;
            }
            content.str += 1;  //skip new line
            content.len = read_bytes - (content.str - buff);
        }

        if ((result=reverse_detect_created_inode(binlog_id,
                        filename, &content, inode)) != EAGAIN)
        {
            break;
        }

        remain_len -= content.len;
    }

    close(fd);
    return result;
}

int inode_binlog_reader_get_last_inode(const uint64_t binlog_id,
        int64_t *inode)
{
    char filename[PATH_MAX];
    char buff[FDIR_INODE_BINLOG_RECORD_MAX_SIZE];
    string_t line;
    int result;
    int64_t file_size;

    write_fd_cache_filename(binlog_id, filename, sizeof(filename));
    if ((result=fc_get_last_line(filename, buff, sizeof(buff),
                    &file_size, &line)) != 0)
    {
        return result;
    }

    result = parse_created_inode(binlog_id, filename, &line, inode);
    if (result != EAGAIN) {
        return result;
    }

    return detect_last_created_inode(binlog_id, filename, file_size, inode);
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

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
#include "../../server_global.h"
#include "diskallocator/binlog/space/binlog_reader.h"
#include "write_fd_cache.h"
#include "inode_index_array.h"
#include "binlog_writer.h"
#include "binlog_reader.h"

#define BINLOG_MIN_FIELD_COUNT   3
#define BINLOG_MAX_FIELD_COUNT   5

#define BINLOG_FIELD_INDEX_VERSION  0
#define BINLOG_FIELD_INDEX_INODE    1
#define BINLOG_FIELD_INDEX_OP_TYPE  2
#define BINLOG_FIELD_INDEX_FILE_ID  3
#define BINLOG_FIELD_INDEX_OFFSET   4

static int binlog_parse(const string_t *line, DABinlogOpType *op_type,
        FDIRStorageInodeIndexInfo *inode_index, char *error_info)
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

    SF_BINLOG_PARSE_INT_SILENCE(inode_index->version,
            "version", BINLOG_FIELD_INDEX_VERSION, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(inode_index->inode,
            "inode", BINLOG_FIELD_INDEX_INODE, ' ', 0);
    *op_type = cols[BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    if (*op_type == da_binlog_op_type_create) {
        if (count != BINLOG_MAX_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MAX_FIELD_COUNT);
            return EINVAL;
        }
        SF_BINLOG_PARSE_INT_SILENCE(inode_index->file_id, "file id",
                BINLOG_FIELD_INDEX_FILE_ID, ' ', 0);
        SF_BINLOG_PARSE_INT_SILENCE(inode_index->offset, "offset",
                BINLOG_FIELD_INDEX_OFFSET, '\n', 0);
    } else if (*op_type == da_binlog_op_type_remove) {
        if (count != BINLOG_MIN_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MIN_FIELD_COUNT);
            return EINVAL;
        }
    } else {
        sprintf(error_info, "unkown op type: %d (0x%02x)",
                *op_type, (unsigned char)*op_type);
        return EINVAL;
    }

    return 0;
}

int inode_binlog_reader_unpack_record(const string_t *line,
        void *args, char *error_info)
{
    int result;
    FDIRInodeSegmentIndexInfo *segment;
    FDIRStorageInodeIndexInfo *inode;
    DABinlogOpType op_type;

    segment = (FDIRInodeSegmentIndexInfo *)args;
    inode = segment->inodes.array.inodes +
        segment->inodes.array.counts.total;
    if ((result=binlog_parse(line, &op_type, inode, error_info)) != 0) {
        return result;
    }

    if (op_type == da_binlog_op_type_create) {
        if ((result=inode_index_array_add(&segment->
                        inodes.array, inode)) != 0)
        {
            *error_info = '\0';
        }
    } else {
        if ((result=inode_index_array_delete(&segment->
                        inodes.array, inode->inode)) != 0)
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

int inode_binlog_reader_load(FDIRInodeSegmentIndexInfo *segment)
{
    int result;

    if ((result=inode_index_array_alloc(&segment->inodes.array,
                    FDIR_STORAGE_BATCH_INODE_COUNT)) != 0)
    {
        return result;
    }

    if ((result=da_binlog_reader_load(&segment->writer.key, segment)) != 0) {
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
    DABinlogOpType op_type;
    FDIRStorageInodeIndexInfo inode_index;

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
        if ((result=binlog_parse(&line, &op_type,
                        &inode_index, error_info)) == 0)
        {
            if (op_type != da_binlog_op_type_create) {
                result = EINVAL;
                sprintf(error_info, "unexpect op type: %c", op_type);
            }
        }
    }

    if (result == 0) {
        *inode = inode_index.inode;
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
    DABinlogOpType op_type;
    FDIRStorageInodeIndexInfo inode_index;

    if ((result=binlog_parse(line, &op_type,
                    &inode_index, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "parse last line fail, binlog id: %"PRId64", "
                "binlog file: %s, error info: %s",
                __LINE__, binlog_id, filename, error_info);
        return result;
    }

    if (op_type == da_binlog_op_type_create) {
        *inode = inode_index.inode;
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

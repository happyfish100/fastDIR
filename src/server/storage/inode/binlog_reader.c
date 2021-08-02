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
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "../../server_global.h"
#include "binlog_fd_cache.h"
#include "binlog_reader.h"

#define BINLOG_MIN_FIELD_COUNT   2
#define BINLOG_MAX_FIELD_COUNT   4

#define BINLOG_FIELD_INDEX_INODE    0
#define BINLOG_FIELD_INDEX_OP_TYPE  1
#define BINLOG_FIELD_INDEX_FILE_ID  2
#define BINLOG_FIELD_INDEX_OFFSET   3

#define BINLOG_RECORD_MAX_SIZE     64

#define BINLOG_PARSE_INT_SILENCE(var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            sprintf(error_info, "invalid %s: %.*s",  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)

static int binlog_parse(const string_t *line,
        FDIRStorageInodeIndexOpType *op_type,
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

    BINLOG_PARSE_INT_SILENCE(inode_index->inode, "inode",
            BINLOG_FIELD_INDEX_INODE, ' ', 0);
    *op_type = cols[BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    if (*op_type == inode_index_op_type_create) {
        if (count != BINLOG_MAX_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MAX_FIELD_COUNT);
            return EINVAL;
        }
        BINLOG_PARSE_INT_SILENCE(inode_index->file_id, "file id",
                BINLOG_FIELD_INDEX_FILE_ID, ' ', 0);
        BINLOG_PARSE_INT_SILENCE(inode_index->offset, "offset",
                BINLOG_FIELD_INDEX_OFFSET, '\n', 0);
    } else if (*op_type == inode_index_op_type_remove) {
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

int binlog_reader_load(const int binlog_index,
        FDIRStorageInodeIndexArray *index_array)
{
    int result;
    char filename[PATH_MAX];

    return 0;
}

int binlog_reader_get_first_inode(const int binlog_index, int64_t *inode)
{
    char filename[PATH_MAX];
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[SF_ERROR_INFO_SIZE];
    int result;
    int64_t bytes;

    binlog_fd_cache_filename(binlog_index, filename, sizeof(filename));

    *error_info = '\0';
    bytes = sizeof(buff);
    if ((result=getFileContentEx(filename, buff, 0, &bytes)) != 0) {
    }
    
    /*
    if ((result=binlog_parse(const string_t *line,
        FDIRStorageInodeIndexOpType *op_type,
        FDIRStorageInodeIndexInfo *inode_index, char *error_info)


    if (result != 0) {
        if (*error_info != '\0') {
            logError("file: "__FILE__", line: %d, "
                    "get_first_record_version fail, "
                    "binlog file: %s, error info: %s",
                    __LINE__, filename, error_info);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "get_first_record_version fail, "
                    "binlog file: %s, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
        }
    }
    */

    return result;
}

int binlog_reader_get_last_inode(const int binlog_index, int64_t *inode)
{
    char filename[PATH_MAX];
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[SF_ERROR_INFO_SIZE];
    int result;
    int offset;
    int64_t file_size = 0;
    int64_t bytes;

    binlog_fd_cache_filename(binlog_index, filename, sizeof(filename));
    if (access(filename, F_OK) == 0) {
        result = getFileSize(filename, &file_size);
    } else {
        result = errno != 0 ? errno : EPERM;
    }
    if ((result == 0 && file_size == 0) || (result == ENOENT)) {
        return ENOENT;
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "access file: %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(errno));
        return result;
    }

    bytes = file_size < sizeof(buff) - 1 ? file_size : sizeof(buff) - 1;
    offset = file_size - bytes;
    bytes += 1;   //for last \0
    if ((result=getFileContentEx(filename, buff, offset, &bytes)) != 0) {
        return result;
    }

    /*
    *error_info = '\0';
    if ((result=binlog_detect_record_reverse(buff, bytes,
                    data_version, NULL, error_info,
                    sizeof(error_info))) != 0)
    {
        if (*error_info != '\0') {
            logError("file: "__FILE__", line: %d, "
                    "get_last_record_version fail, "
                    "binlog file: %s, error info: %s",
                    __LINE__, filename, error_info);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "get_last_record_version fail, "
                    "binlog file: %s, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
        }
    }
    */

    return result;
}

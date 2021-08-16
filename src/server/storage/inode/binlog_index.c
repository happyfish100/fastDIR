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
#include "binlog_index.h"

#define BINLOG_INDEX_FILENAME   "ib_index.dat"

#define BINLOG_HEADER_FIELD_COUNT   2
#define BINLOG_HEADER_FIELD_INDEX_RECORD_COUNT 0
#define BINLOG_HEADER_FIELD_INDEX_LAST_VERSION 1

#define BINLOG_RECORD_FIELD_COUNT   3
#define BINLOG_RECORD_FIELD_INDEX_ID           0
#define BINLOG_RECORD_FIELD_INDEX_FIRST_INODE  1
#define BINLOG_RECORD_FIELD_INDEX_LAST_INODE   2

#define BINLOG_INDEX_RECORD_MAX_SIZE   64

static int parse_header(const string_t *line, int *record_count,
        int64_t *last_version, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_HEADER_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_HEADER_FIELD_COUNT, false);
    if (count != BINLOG_HEADER_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, BINLOG_HEADER_FIELD_COUNT);
        return EINVAL;
    }

    FDIR_BINLOG_PARSE_INT_SILENCE(*record_count, "record count",
            BINLOG_HEADER_FIELD_INDEX_RECORD_COUNT, ' ', 0);
    FDIR_BINLOG_PARSE_INT_SILENCE(*last_version, "last version",
            BINLOG_HEADER_FIELD_INDEX_LAST_VERSION, '\n', 0);
    return 0;
}

static int parse_record(const string_t *line,
        FDIRInodeBinlogIndexInfo *index, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_RECORD_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_RECORD_FIELD_COUNT, false);
    if (count != BINLOG_RECORD_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, BINLOG_RECORD_FIELD_COUNT);
        return EINVAL;
    }

    FDIR_BINLOG_PARSE_INT_SILENCE(index->binlog_id, "binlog id",
            BINLOG_RECORD_FIELD_INDEX_ID, ' ', 0);
    FDIR_BINLOG_PARSE_INT_SILENCE(index->inodes.first, "first inode",
            BINLOG_RECORD_FIELD_INDEX_FIRST_INODE, ' ', 1);
    FDIR_BINLOG_PARSE_INT_SILENCE(index->inodes.last, "last inode",
            BINLOG_RECORD_FIELD_INDEX_LAST_INODE, '\n', 1);
    return 0;
}

static inline void binlog_index_get_filename(
        char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s", STORAGE_PATH_STR,
            BINLOG_INDEX_FILENAME);
}

static int parse(FDIRInodeBinlogIndexContext *ctx,
        const string_t *lines, const int row_count)
{
    int result;
    int record_count;
    char error_info[256];
    char filename[PATH_MAX];
    const string_t *line;
    const string_t *end;
    FDIRInodeBinlogIndexInfo *bindex;

    if (row_count < 1) {
        return EINVAL;
    }

    if ((result=parse_header(lines, &record_count, &ctx->
                    last_version, error_info)) != 0)
    {
        binlog_index_get_filename(filename, sizeof(filename));
        logError("file: "__FILE__", line: %d, "
                "inode binlog index file: %s, parse header fail, "
                "error info: %s", __LINE__, filename, error_info);
        return result;
    }

    if (row_count != record_count + 1) {
        binlog_index_get_filename(filename, sizeof(filename));
        logError("file: "__FILE__", line: %d, "
                "inode binlog index file: %s, line count: %d != "
                "record count: %d + 1", __LINE__, filename,
                row_count, record_count + 1);
        return EINVAL;
    }

    ctx->index_array.alloc = 64;
    while (ctx->index_array.alloc < record_count) {
        ctx->index_array.alloc *= 2;
    }
    ctx->index_array.indexes = (FDIRInodeBinlogIndexInfo *)fc_malloc(
            sizeof(FDIRInodeBinlogIndexInfo) * ctx->index_array.alloc);
    if (ctx->index_array.indexes == NULL) {
        return ENOMEM;
    }

    bindex = ctx->index_array.indexes;
    end = lines + row_count;
    for (line=lines+1; line<end; line++) {
        if ((result=parse_record(line, bindex, error_info)) != 0) {
            binlog_index_get_filename(filename, sizeof(filename));
            logError("file: "__FILE__", line: %d, "
                    "inode binlog index file: %s, parse line #%d fail, "
                    "error info: %s", __LINE__, filename,
                    (int)(line - lines) + 1, error_info);
            return result;
        }
        bindex++;
    }

    ctx->index_array.count = bindex - ctx->index_array.indexes;
    return 0;
}

static int load(FDIRInodeBinlogIndexContext *ctx, const char *filename)
{
    int result;
    int row_count;
    int64_t file_size;
    string_t context;
    string_t *lines;

    if ((result=getFileContent(filename, &context.str, &file_size)) != 0) {
        return result;
    }

    context.len = file_size;
    row_count = getOccurCount(context.str, '\n');
    lines = (string_t *)fc_malloc(sizeof(string_t) * row_count);
    if (lines == NULL) {
        free(context.str);
        return ENOMEM;
    }

    row_count = split_string_ex(&context, '\n', lines, row_count, true);
    result = parse(ctx, lines, row_count);
    free(lines);
    free(context.str);
    return result;
}

int binlog_index_load(FDIRInodeBinlogIndexContext *ctx)
{
    int result;
    char filename[PATH_MAX];

    binlog_index_get_filename(filename, sizeof(filename));
    if (access(filename, F_OK) == 0) {
        return load(ctx, filename);
    } else if (errno == ENOENT) {
        memset(ctx, 0, sizeof(*ctx));
        return 0;
    } else {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "access file %s fail, "
                "errno: %d, error info: %s", __LINE__,
                filename, result, STRERROR(result));
        return result;
    }
}

static int save(FDIRInodeBinlogIndexContext *ctx, const char *filename)
{
    char buff[16 * 1024];
    char *bend;
    FDIRInodeBinlogIndexInfo *index;
    FDIRInodeBinlogIndexInfo *end;
    char *p;
    int fd;
    int len;
    int result;

    fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    result = 0;
    p = buff;
    bend = buff + sizeof(buff);
    p += sprintf(p, "%d %"PRId64"\n", ctx->index_array.count,
            ctx->last_version);

    end = ctx->index_array.indexes + ctx->index_array.count;
    for (index=ctx->index_array.indexes; index<end; index++) {
        if (bend - p < BINLOG_INDEX_RECORD_MAX_SIZE) {
            len = p - buff;
            if (fc_safe_write(fd, buff, len) != len) {
                result = errno != 0 ? errno : EIO;
                logError("file: "__FILE__", line: %d, "
                        "write file %s fail, errno: %d, error info: %s",
                        __LINE__, filename, result, STRERROR(result));
                break;
            }
            p = buff;
        }

        p += sprintf(p, "%"PRId64" %"PRId64" %"PRId64"\n",
                index->binlog_id, index->inodes.first,
                index->inodes.last);
    }

    if (result == 0) {
        len = p - buff;
        if (len > 0 && fc_safe_write(fd, buff, len) != len) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "write file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
        }
    }

    close(fd);
    return result;
}

int binlog_index_save(FDIRInodeBinlogIndexContext *ctx)
{
    int result;
    char filename[PATH_MAX];
    char tmp_filename[PATH_MAX];

    binlog_index_get_filename(filename, sizeof(filename));
    snprintf(tmp_filename, sizeof(tmp_filename),
            "%s.tmp", filename);
    if ((result=save(ctx, tmp_filename)) != 0) {
        return result;
    }

    if (rename(tmp_filename, filename) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "rename file \"%s\" to \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, tmp_filename, filename,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

int binlog_index_expand(FDIRInodeBinlogIndexContext *ctx)
{
    int alloc;
    FDIRInodeBinlogIndexInfo *indexes;

    if (ctx->index_array.alloc == 0) {
        alloc = 64;
    } else {
        alloc = ctx->index_array.alloc * 2;
    }
    indexes = (FDIRInodeBinlogIndexInfo *)fc_malloc(
            sizeof(FDIRInodeBinlogIndexInfo) * alloc);
    if (indexes == NULL) {
        return ENOMEM;
    }

    if (ctx->index_array.count > 0) {
        memcpy(indexes, ctx->index_array.indexes,
                sizeof(FDIRInodeBinlogIndexInfo) *
                ctx->index_array.count);
        free(ctx->index_array.indexes);
    }

    ctx->index_array.indexes = indexes;
    ctx->index_array.alloc = alloc;
    return 0;
}

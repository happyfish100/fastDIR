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
#include "../../server_global.h"
#include "bid_journal.h"

#define BINLOG_ID_JOURNAL_FILENAME "binlog_id.journal"

#define BID_JOURNAL_RECORD_MAX_SIZE   48
#define BID_JOURNAL_FIELD_COUNT        3

#define BID_JOURNAL_FIELD_INDEX_VERSION    0
#define BID_JOURNAL_FIELD_INDEX_BINLOG_ID  1
#define BID_JOURNAL_FIELD_INDEX_OP_TYPE    2

typedef struct {
    int fd;
    volatile int64_t version;
} BidJournalContext;

static BidJournalContext journal_ctx = {-1, 0};

static int parse_line(const string_t *line, FDIRInodeBinlogIdJournal
        *journal, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BID_JOURNAL_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BID_JOURNAL_FIELD_COUNT, false);
    if (count != BID_JOURNAL_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, BID_JOURNAL_FIELD_COUNT);
        return EINVAL;
    }

    FDIR_BINLOG_PARSE_INT_SILENCE(journal->version,
            "version", BID_JOURNAL_FIELD_INDEX_VERSION, ' ', 1);
    FDIR_BINLOG_PARSE_INT_SILENCE(journal->binlog_id,
            "binlog_id", BID_JOURNAL_FIELD_INDEX_BINLOG_ID, ' ', 0);
    journal->op_type = cols[BID_JOURNAL_FIELD_INDEX_OP_TYPE].str[0];
    if (!((journal->op_type == inode_binlog_id_op_type_create) ||
                (journal->op_type == inode_binlog_id_op_type_remove)))
    {
        sprintf(error_info, "unkown op type: %d (0x%02x)",
                journal->op_type, (unsigned char)journal->op_type);
        return EINVAL;
    }

    return 0;
}


static inline void get_full_filename(char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s", STORAGE_PATH_STR,
            BINLOG_ID_JOURNAL_FILENAME);
}

static int get_last_version(int64_t *version)
{
    int result;
    int64_t file_size;
    char buff[BID_JOURNAL_RECORD_MAX_SIZE];
    char error_info[256];
    string_t line;
    char full_filename[PATH_MAX];
    FDIRInodeBinlogIdJournal journal;

    get_full_filename(full_filename, sizeof(full_filename));
    if ((result=fc_get_last_line(full_filename, buff,
                    sizeof(buff), &file_size, &line)) != 0)
    {
        if (result == ENOENT) {
            return 0;
        }
    }

    if ((result=parse_line(&line, &journal, error_info)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "parse last line fail, file: %s, line: %.*s, "
                "error info: %s", __LINE__, full_filename, line.len,
                line.str, error_info);
        return result;
    }

    *version = journal.version;
    return 0;
}

static int journal_open()
{
    int result;
    char full_filename[PATH_MAX];

    get_full_filename(full_filename, sizeof(full_filename));
    if ((journal_ctx.fd=open(full_filename, O_WRONLY |
                    O_CREAT | O_APPEND, 0755)) < 0)
    {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, result, strerror(result));
        return result;
    }

    return 0;
}

int bid_journal_init()
{
    int result;
    char full_filename[PATH_MAX];

    get_full_filename(full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno != ENOENT) {
            return errno != 0 ? errno : EPERM;
        }
    } else {
        if ((result=get_last_version((int64_t *)
                        &journal_ctx.version)) != 0)
        {
            return result;
        }
    }

    return journal_open();
}

int64_t bid_journal_current_version()
{
    return __sync_add_and_fetch(&journal_ctx.version, 0);
}

int bid_journal_log(const uint64_t binlog_id,
        const FDIRInodeBinlogIdOpType op_type)
{
    char buff[BID_JOURNAL_RECORD_MAX_SIZE];
    int len;

    len = sprintf(buff, "%"PRId64" %"PRId64" %c\n",
            __sync_add_and_fetch(&journal_ctx.version, 1),
            binlog_id, op_type);
    return fc_safe_write(journal_ctx.fd, buff, len);
}

static int parse_buffer(FDIRInodeBidJournalArray *jarray,
        const int64_t start_version, const string_t *content,
        const int estimated, char *error_info)
{
    char *buff_end;
    char *line_end;
    FDIRInodeBinlogIdJournal *journal;
    string_t line;
    bool skip;
    int alloc;
    int result;

    alloc = 4;
    while (alloc < estimated) {
        alloc *= 2;
    }

    jarray->records = (FDIRInodeBinlogIdJournal *)fc_malloc(
            sizeof(FDIRInodeBinlogIdJournal) * alloc);
    if (jarray->records == NULL) {
        return ENOMEM;
    }

    skip = true;
    journal = jarray->records;
    buff_end = content->str + content->len;
    line.str = content->str;
    while (line.str < buff_end) {
        line_end = memchr(line.str, '\n', buff_end - line.str);
        if (line_end == NULL) {
            sprintf(error_info, "expect new line char(\\n)");
            return EINVAL;
        }

        line_end++;
        line.len = line_end - line.str;
        if ((result=parse_line(&line, journal, error_info)) != 0) {
            return result;
        }

        if (skip && journal->version >= start_version) {
            skip = false;
        }
        if (!skip) {
            journal++;
            jarray->count++;
            if (jarray->count == alloc) {
                alloc *= 2;
                jarray->records = (FDIRInodeBinlogIdJournal *)fc_realloc(
                        jarray->records, sizeof(FDIRInodeBinlogIdJournal) *
                        alloc);
                if (jarray->records == NULL) {
                    return ENOMEM;
                }
                journal = jarray->records + jarray->count;
            }
        }

        line.str = line_end;
    }

    return 0;
}

int bid_journal_fetch(FDIRInodeBidJournalArray *jarray,
        const int64_t start_version)
{
    int result;
    int64_t distance;
    int64_t bytes;
    int64_t offset;
    int64_t file_size;
    int64_t buff_size;
    char fixed[16 * 1024];
    char *buff;
    string_t content;
    char full_filename[PATH_MAX];
    char error_info[256];

    jarray->count = 0;
    jarray->records = NULL;
    distance = journal_ctx.version - start_version + 1;
    if (distance <= 0 ) {
        return 0;
    }

    get_full_filename(full_filename, sizeof(full_filename));
    if ((result=getFileSize(full_filename, &file_size)) != 0) {
        return result;
    }

    bytes = BID_JOURNAL_RECORD_MAX_SIZE * distance;
    if (bytes <= file_size) {
        offset = file_size - bytes + 1;
        buff_size = bytes;
    } else {
        offset = 0;
        buff_size = file_size + 1;
    }

    if (buff_size <= sizeof(fixed)) {
        buff = fixed;
    } else {
        if ((buff=(char *)fc_malloc(buff_size)) == NULL) {
            return ENOMEM;
        }
    }

    if ((result=getFileContentEx(full_filename, buff,
                offset, &buff_size)) == 0)
    {
        if (offset == 0) {
            content.str = buff;
            content.len = buff_size;
        } else {
            content.str = (char *)memchr(buff, '\n', buff_size);
            if (content.str == NULL) {
                result = ENOENT;
            } else {
                content.str++;  //skip new line char
                content.len = (buff + buff_size) - content.str;
            }
        }

        if (result == 0) {
            *error_info = '\0';
            if ((result=parse_buffer(jarray, start_version,
                            &content, distance, error_info)) != 0)
            {
                if (*error_info != '\0') {
                    logError("file: "__FILE__", line: %d, "
                            "parse binlog fail, file: %s, error info: %s",
                            __LINE__, full_filename, error_info);
                }
            }
        }
    }

    if (buff != fixed) {
        free(buff);
    }
    return result;
}

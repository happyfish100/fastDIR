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
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../../server_global.h"
#include "binlog_writer.h"

#define BINLOG_INDEX_FILENAME "binlog_index.dat"

#define BINLOG_INDEX_ITEM_CURRENT_WRITE     "current_write"
#define BINLOG_INDEX_ITEM_CURRENT_COMPRESS  "current_compress"

typedef struct {
    int current_write;
    int current_compress;
} BinlogWriterContext;

static BinlogWriterContext binlog_writer_ctx;

static int write_to_binlog_index(const int current_write_index)
{
    char full_filename[MAX_PATH_SIZE];
    char buff[256];
    int fd;
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    if ((fd=open(full_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, full_filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : ENOENT;
    }

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE, current_write_index,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS,
            binlog_writer_ctx.current_compress);
    if (fc_safe_write(fd, buff, len) != len) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, full_filename,
                errno, STRERROR(errno));
        close(fd);
        return errno != 0 ? errno : EIO;
    }

    close(fd);
    return 0;
}

static int get_binlog_index_from_file()
{
    char full_filename[MAX_PATH_SIZE];
    IniContext iniContext;
    int result;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return write_to_binlog_index(binlog_writer_ctx.current_write);
        } else {
            return errno != 0 ? errno : EPERM;
        }
    }

    memset(&iniContext, 0, sizeof(IniContext));
    if ((result=iniLoadFromFile(full_filename, &iniContext)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, "
                "error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    binlog_writer_ctx.current_write = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_WRITE, &iniContext, 0);
    binlog_writer_ctx.current_compress = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS, &iniContext, 0);

    iniFreeContext(&iniContext);
    return 0;
}

int inode_binlog_writer_init()
{
    int result;

    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    return 0;
}

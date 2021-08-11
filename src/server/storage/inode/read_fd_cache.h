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

#ifndef _READ_FD_CACHE_H
#define _READ_FD_CACHE_H

#include <fcntl.h>
#include "fastcommon/logger.h"
#include "binlog_fd_cache.h"

#ifdef __cplusplus
extern "C" {
#endif

    extern BinlogFDCacheContext g_read_cache_ctx;

    int read_fd_cache_init(const int max_idle_time, const int capacity);

    //return fd, < 0 for error
    static inline int read_fd_cache_get(const uint64_t binlog_id)
    {
        int fd;
        int result;

        if ((fd=binlog_fd_cache_get(&g_read_cache_ctx, binlog_id)) < 0) {
            return fd;
        }

        if (lseek(fd, 0, SEEK_SET) < 0) {
            char full_filename[PATH_MAX];

            result = errno != 0 ? errno : EIO;
            binlog_fd_cache_filename(binlog_id, full_filename,
                    sizeof(full_filename));
            logError("file: "__FILE__", line: %d, "
                    "lseek file: %s fail, offset: 0, "
                    "errno: %d, error info: %s", __LINE__,
                    full_filename, result, STRERROR(result));
            return -1 * result;
        }

        return fd;
    }

#ifdef __cplusplus
}
#endif

#endif

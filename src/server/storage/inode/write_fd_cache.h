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

#ifndef _WRITE_FD_CACHE_H
#define _WRITE_FD_CACHE_H

#include "binlog_fd_cache.h"

#ifdef __cplusplus
extern "C" {
#endif

    extern BinlogFDCacheContext g_write_cache_ctx;

    int write_fd_cache_init(const int max_idle_time, const int capacity);

    //return fd, < 0 for error
    static inline int write_fd_cache_get(const uint64_t binlog_id)
    {
        return binlog_fd_cache_get(&g_write_cache_ctx, binlog_id);
    }

    static inline int write_fd_cache_remove(const uint64_t binlog_id)
    {
        return binlog_fd_cache_remove(&g_write_cache_ctx, binlog_id);
    }

#ifdef __cplusplus
}
#endif

#endif

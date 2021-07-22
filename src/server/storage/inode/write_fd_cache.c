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

#include "write_fd_cache.h"

BinlogFDCacheContext g_write_cache_ctx;

int write_fd_cache_init(const int max_idle_time, const int capacity)
{
    const int open_flags =  O_WRONLY | O_CREAT | O_APPEND;
    return binlog_fd_cache_init(&g_write_cache_ctx,
            open_flags, max_idle_time, capacity);
}

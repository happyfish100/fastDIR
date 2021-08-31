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

#ifndef _SLICE_WRITE_FD_CACHE_H
#define _SLICE_WRITE_FD_CACHE_H

#include "diskallocator/binlog/common/write_fd_cache.h"
#include "../storage_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    //return fd, < 0 for error
    static inline int write_fd_cache_get(const uint64_t trunk_id)
    {
        DA_DECLARE_BINLOG_ID_TYPE_VAR(key, trunk_id,
                FDIR_STORAGE_BINLOG_TYPE_TRUNK);
        return da_write_fd_cache_get(&key);
    }

    static inline int write_fd_cache_remove(const uint64_t trunk_id)
    {
        DA_DECLARE_BINLOG_ID_TYPE_VAR(key, trunk_id,
                FDIR_STORAGE_BINLOG_TYPE_TRUNK);
        return da_write_fd_cache_remove(&key);
    }

    static inline int write_fd_cache_filename(const uint64_t trunk_id,
            char *full_filename, const int size)
    {
        DA_DECLARE_BINLOG_ID_TYPE_VAR(key, trunk_id,
                FDIR_STORAGE_BINLOG_TYPE_TRUNK);
        return da_write_fd_cache_filename(&key, full_filename, size);
    }

#ifdef __cplusplus
}
#endif

#endif

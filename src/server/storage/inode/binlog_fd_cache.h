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

#ifndef _BINLOG_FD_CACHE_H
#define _BINLOG_FD_CACHE_H

#include "fastcommon/fc_list.h"
#include "fastcommon/fast_mblock.h"

typedef struct binlog_id_fd_pair {
    uint32_t binlog_id;
    int fd;
} BinlogIdFDPair;

typedef struct binlog_fd_cache_entry {
    BinlogIdFDPair pair;
    struct fc_list_head dlink;
    struct binlog_fd_cache_entry *next;  //for hashtable
} BinlogFDCacheEntry;

typedef struct {
    BinlogFDCacheEntry **buckets;
    unsigned int size;
} BinlogFDCacheHashtable;

typedef struct {
    BinlogFDCacheHashtable htable;
    struct {
        int capacity;
        int count;
        struct fc_list_head head;
    } lru;
    struct fast_mblock_man allocator;
} BinlogFDCacheContext;

#ifdef __cplusplus
extern "C" {
#endif

    int binlog_fd_cache_init(BinlogFDCacheContext *cache_ctx, const int capacity);

    //return fd, -1 for not exist
    int binlog_fd_cache_get(BinlogFDCacheContext *cache_ctx,
            const uint32_t binlog_id);

    int binlog_fd_cache_add(BinlogFDCacheContext *cache_ctx,
            const uint32_t binlog_id, const int fd);

    int binlog_fd_cache_delete(BinlogFDCacheContext *cache_ctx,
            const uint32_t binlog_id);

#ifdef __cplusplus
}
#endif

#endif

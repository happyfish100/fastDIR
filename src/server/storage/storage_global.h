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

#ifndef _STORAGE_GLOBAL_H
#define _STORAGE_GLOBAL_H

#include "sf/sf_ordered_writer.h"
#include "storage_types.h"

typedef struct {
    string_t path;   //data path
    int inode_binlog_subdirs;
    int inode_index_dump_interval;
    TimeInfo inode_index_dump_base_time;
    FDIRDataSyncThreadArray data_sync_thread_array;
    SFOrderedWriterContext ordered_writer_ctx;
    struct {
        int waitings;
        pthread_lock_cond_pair_t lcp;
    } data_sync_notify;
} FDIRStorageGlobalVars;

#define STORAGE_PATH            g_storage_global_vars.path
#define STORAGE_PATH_STR        STORAGE_PATH.str
#define STORAGE_PATH_LEN        STORAGE_PATH.len

#define INODE_BINLOG_SUBDIRS       g_storage_global_vars.inode_binlog_subdirs
#define INODE_INDEX_DUMP_INTERVAL  g_storage_global_vars.inode_index_dump_interval
#define INODE_INDEX_DUMP_BASE_TIME g_storage_global_vars.inode_index_dump_base_time
#define DATA_SYNC_THREAD_ARRAY     g_storage_global_vars.data_sync_thread_array
#define ORDERED_WRITER_CTX         g_storage_global_vars.ordered_writer_ctx

#define DATA_SYNC_NOTIFY_WAITINGS  g_storage_global_vars.data_sync_notify.waitings
#define DATA_SYNC_NOTIFY_LCP       g_storage_global_vars.data_sync_notify.lcp

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRStorageGlobalVars g_storage_global_vars;

    static inline void fdir_data_sync_finish(const int count)
    {
        PTHREAD_MUTEX_LOCK(&DATA_SYNC_NOTIFY_LCP.lock);
        DATA_SYNC_NOTIFY_WAITINGS -= count;
        if (DATA_SYNC_NOTIFY_WAITINGS == 0) {
            pthread_cond_signal(&DATA_SYNC_NOTIFY_LCP.cond);
        }
        PTHREAD_MUTEX_UNLOCK(&DATA_SYNC_NOTIFY_LCP.lock);
    }

#ifdef __cplusplus
}
#endif

#endif

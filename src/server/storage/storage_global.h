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

#include "sf/sf_func.h"
#include "sf/sf_ordered_writer.h"
#include "storage_types.h"

typedef struct {
    const FDIRStorageEngineConfig *db_cfg;
    FDIRDataSyncThreadArray data_sync_thread_array;
    FDIROrderdUpdateChain ordered_update_chain;
    struct fast_mblock_man update_record_allocator;
    FDIRBinlogWriteThreadContext binlog_write_thread_ctx;
    DABinlogWriter inode_binlog_writer;
    SFSynchronizeContext data_sync_notify;
} FDIRStorageGlobalVars;

#define STORAGE_PATH            g_storage_global_vars.db_cfg->path
#define STORAGE_PATH_STR        STORAGE_PATH.str
#define STORAGE_PATH_LEN        STORAGE_PATH.len

#define INODE_BINLOG_SUBDIRS    g_storage_global_vars.db_cfg->inode_binlog_subdirs
#define INDEX_DUMP_INTERVAL     g_storage_global_vars.db_cfg->index_dump_interval
#define INDEX_DUMP_BASE_TIME    g_storage_global_vars.db_cfg->index_dump_base_time
#define DATA_SYNC_THREAD_ARRAY  g_storage_global_vars.data_sync_thread_array
#define ORDERED_UPDATE_CHAIN    g_storage_global_vars.ordered_update_chain
#define UPDATE_RECORD_ALLOCATOR g_storage_global_vars.update_record_allocator
#define BINLOG_WRITE_THREAD_CTX g_storage_global_vars.binlog_write_thread_ctx
#define INODE_BINLOG_WRITER     g_storage_global_vars.inode_binlog_writer

#define DATA_SYNC_NOTIFY           g_storage_global_vars.data_sync_notify
#define DATA_SYNC_NOTIFY_WAITINGS  DATA_SYNC_NOTIFY.waiting_count
#define DATA_SYNC_NOTIFY_LCP       DATA_SYNC_NOTIFY.lcp

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRStorageGlobalVars g_storage_global_vars;

    static inline void fdir_data_sync_finish(const int count)
    {
        sf_synchronize_counter_notify(&DATA_SYNC_NOTIFY, count);
    }

#ifdef __cplusplus
}
#endif

#endif

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

#include "diskallocator/binlog/common/write_fd_cache.h"
#include "inode/binlog_reader.h"
#include "inode/binlog_writer.h"
#include "data_sync_thread.h"
#include "storage_engine.h"

static int init_write_fd_cache()
{
    const int max_idle_time = 3600;
    const int capacity = 1361;
    DABinlogTypeSubdirPair pairs[FDIR_STORAGE_BINLOG_TYPE_COUNT];
    DABinlogTypeSubdirArray type_subdir_array;

    DA_BINLOG_SET_TYPE_SUBDIR_PAIR(pairs[FDIR_STORAGE_BINLOG_TYPE_INODE],
            FDIR_STORAGE_BINLOG_TYPE_INODE, "inode",
            inode_binlog_pack_record_callback,
            inode_binlog_reader_unpack_record,
            inode_binlog_batch_update_callback,
            inode_binlog_shrink_callback);


    type_subdir_array.pairs = pairs;
    type_subdir_array.count = FDIR_STORAGE_BINLOG_TYPE_COUNT;
    return da_write_fd_cache_init(&type_subdir_array,
            max_idle_time, capacity);
}

int fdir_storage_engine_init(IniFullContext *ini_ctx)
{
    const char *subdir_name = "inode";
    const int buffer_size = 64 * FDIR_INODE_BINLOG_RECORD_MAX_SIZE;
    int result;

    if ((result=init_write_fd_cache()) != 0) {
        return result;
    }

    if ((result=data_sync_thread_init()) != 0) {
        return result;
    }

    if ((result=sf_ordered_writer_init(&ORDERED_WRITER_CTX,
                    STORAGE_PATH_STR, subdir_name, buffer_size,
                    FDIR_INODE_BINLOG_RECORD_MAX_SIZE)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(&DATA_SYNC_NOTIFY_LCP)) != 0) {
        return result;
    }

    return 0;
}

int fdir_storage_engine_start()
{
    int result;

    if ((result=data_sync_thread_start()) != 0) {
        return result;
    }

    return 0;
}

void fdir_storage_engine_terminate()
{
}

int fdir_storage_engine_store(FDIRDBUpdateDentryArray *array)
{
    int result;
    struct fc_queue_info chain;
    FDIRDBUpdateDentry *entry;
    FDIRDBUpdateDentry *end;
    SFWriterVersionEntry *ver;

    if ((result=sf_ordered_writer_alloc_versions(&ORDERED_WRITER_CTX,
                    array->count, &chain)) != 0)
    {
        return result;
    }

    PTHREAD_MUTEX_LOCK(&DATA_SYNC_NOTIFY_LCP.lock);
    DATA_SYNC_NOTIFY_WAITINGS = array->count;
    PTHREAD_MUTEX_UNLOCK(&DATA_SYNC_NOTIFY_LCP.lock);

    end = array->entries + array->count;
    for (entry=array->entries, ver=chain.head;
            entry<end; entry++, ver=ver->next)
    {
        ver->version = entry->version;
        data_sync_thread_push(entry);
    }
    sf_ordered_writer_push_versions(&ORDERED_WRITER_CTX, &chain);

    PTHREAD_MUTEX_LOCK(&DATA_SYNC_NOTIFY_LCP.lock);
    if (DATA_SYNC_NOTIFY_WAITINGS > 0) {
        pthread_cond_wait(&DATA_SYNC_NOTIFY_LCP.cond,
                &DATA_SYNC_NOTIFY_LCP.lock);
    }
    PTHREAD_MUTEX_UNLOCK(&DATA_SYNC_NOTIFY_LCP.lock);

    return 0;
}

int fdir_storage_engine_fetch(const int64_t inode,
        const int field_index, FastBuffer *buffer)
{
    return 0;
}

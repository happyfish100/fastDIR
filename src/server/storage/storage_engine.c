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
#include "inode/bid_journal.h"
#include "inode/segment_index.h"
#include "data_sync_thread.h"
#include "binlog_write_thread.h"
#include "storage_engine.h"

#define DATA_SYNC_THREAD_DEFAULT_COUNT  4
#define DATA_SYNC_THREAD_MIN_COUNT      1
#define DATA_SYNC_THREAD_MAX_COUNT     64

static int init_write_fd_cache()
{
    const int max_idle_time = 3600;
    const int capacity = 1361;
    DABinlogTypeSubdirPair pairs[FDIR_STORAGE_BINLOG_TYPE_COUNT];
    DABinlogTypeSubdirArray type_subdir_array;

    DA_BINLOG_SET_TYPE_SUBDIR_PAIR(pairs[FDIR_STORAGE_BINLOG_TYPE_INODE],
            FDIR_STORAGE_BINLOG_TYPE_INODE, "inode",
            inode_binlog_reader_unpack_record,
            inode_binlog_shrink_callback);

    type_subdir_array.pairs = pairs;
    type_subdir_array.count = FDIR_STORAGE_BINLOG_TYPE_COUNT;
    return da_write_fd_cache_init(&type_subdir_array,
            max_idle_time, capacity);
}

static int update_record_alloc_init(FDIRInodeUpdateRecord *record, void *args)
{
    record->inode.buffer.buff = (char *)(record + 1);
    record->inode.buffer.alloc_size = FDIR_INODE_BINLOG_RECORD_MAX_SIZE;
    return 0;
}

int fdir_storage_engine_init(IniFullContext *ini_ctx,
        const int my_server_id, const FDIRStorageEngineConfig *db_cfg,
        const DADataGlobalConfig *data_cfg)
{
    const int file_block_size = 4 * 1024 * 1024;
    char *storage_config_filename;
    char full_storage_filename[PATH_MAX];
    int result;

    g_storage_global_vars.db_cfg = db_cfg;
    if ((result=da_binlog_writer_global_init()) != 0) {
        return result;
    }

    if ((result=da_binlog_writer_init(&INODE_BINLOG_WRITER,
                    FDIR_STORAGE_BINLOG_TYPE_INODE,
                    FDIR_INODE_BINLOG_RECORD_MAX_SIZE)) != 0)
    {
        return result;
    }

    storage_config_filename = iniGetStrValue(ini_ctx->section_name,
            "storage_config_filename", ini_ctx->context);
    if (storage_config_filename == NULL || *storage_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item "
                "\"storage_config_filename\" not exist or empty",
                __LINE__, ini_ctx->filename, ini_ctx->section_name);
        return ENOENT;
    }
    resolve_path(ini_ctx->filename, storage_config_filename,
            full_storage_filename, sizeof(full_storage_filename));
    if ((result=da_load_config(my_server_id, file_block_size,
            data_cfg, full_storage_filename)) != 0)
    {
        return result;
    }

    if ((result=init_write_fd_cache()) != 0) {
        return result;
    }

    DATA_SYNC_THREAD_ARRAY.count = iniGetIntCorrectValue(ini_ctx,
            "data_sync_thread_count", DATA_SYNC_THREAD_DEFAULT_COUNT,
            DATA_SYNC_THREAD_MIN_COUNT, DATA_SYNC_THREAD_MAX_COUNT);
    if ((result=data_sync_thread_init()) != 0) {
        return result;
    }

    if ((result=init_pthread_lock(&ORDERED_UPDATE_CHAIN.lock)) != 0) {
        return result;
    }
    ORDERED_UPDATE_CHAIN.next_version = 1;
    ORDERED_UPDATE_CHAIN.head = ORDERED_UPDATE_CHAIN.tail = NULL;

    if ((result=fast_mblock_init_ex1(&UPDATE_RECORD_ALLOCATOR,
                    "update-record", sizeof(FDIRInodeUpdateRecord) +
                    FDIR_INODE_BINLOG_RECORD_MAX_SIZE,
                    8 * 1024, 0, (fast_mblock_alloc_init_func)
                    update_record_alloc_init, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(&DATA_SYNC_NOTIFY_LCP)) != 0) {
        return result;
    }

    if ((result=bid_journal_init()) != 0) {
        return result;
    }

    if ((result=inode_segment_index_init()) != 0) {
        return result;
    }

    if ((result=binlog_write_thread_init()) != 0) {
        return result;
    }

    return 0;
}

int fdir_storage_engine_start()
{
    int result;

    if ((result=da_binlog_writer_start()) != 0) {
        return result;
    }

    if ((result=data_sync_thread_start()) != 0) {
        return result;
    }

    if ((result=da_init_start(binlog_write_thread_push)) != 0) {
        return result;
    }

    if ((result=inode_segment_index_start()) != 0) {
        return result;
    }

    if ((result=binlog_write_thread_start()) != 0) {
        return result;
    }

    return 0;
}

void fdir_storage_engine_terminate()
{
}

int fdir_storage_engine_store(const FDIRDBUpdateFieldArray *array)
{
    FDIRDBUpdateFieldInfo *entry;
    FDIRDBUpdateFieldInfo *end;
    int result;

    PTHREAD_MUTEX_LOCK(&DATA_SYNC_NOTIFY_LCP.lock);
    DATA_SYNC_NOTIFY_WAITINGS = array->count;
    PTHREAD_MUTEX_UNLOCK(&DATA_SYNC_NOTIFY_LCP.lock);

    end = array->entries + array->count;
    for (entry=array->entries; entry<end; entry++) {
        if (entry->op_type == da_binlog_op_type_create &&
                entry->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            if ((result=inode_segment_index_pre_add(entry->inode)) != 0) {
                return result;
            }
        }

        data_sync_thread_push(entry);
    }

    PTHREAD_MUTEX_LOCK(&DATA_SYNC_NOTIFY_LCP.lock);
    if (DATA_SYNC_NOTIFY_WAITINGS > 0) {
        pthread_cond_wait(&DATA_SYNC_NOTIFY_LCP.cond,
                &DATA_SYNC_NOTIFY_LCP.lock);
    }
    PTHREAD_MUTEX_UNLOCK(&DATA_SYNC_NOTIFY_LCP.lock);

    return 0;
}

int fdir_storage_engine_redo(const FDIRDBUpdateFieldArray *array)
{
    int result;
    bool found;
    bool keep;
    FDIRStorageInodeIndexInfo index;
    FDIRDBUpdateFieldArray redo_array;
    FDIRDBUpdateFieldInfo *entry;
    FDIRDBUpdateFieldInfo *end;
    FDIRDBUpdateFieldInfo *dest;
    int remove_count = 0;
    int create_count = 0;

    redo_array.entries = (FDIRDBUpdateFieldInfo *)fc_malloc(
            sizeof(FDIRDBUpdateFieldInfo) * array->count);
    if (redo_array.entries == NULL) {
        return ENOMEM;
    }

    dest = redo_array.entries;
    end = array->entries + array->count;
    for (entry=array->entries; entry<end; entry++) {
        index.inode = entry->inode;
        found = (inode_segment_index_get(&index) == 0);
        if (entry->op_type == da_binlog_op_type_remove) {
            keep = found;
            if (keep) remove_count++;
        } else if (entry->op_type == da_binlog_op_type_create &&
                entry->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            keep = !found;
            if (keep) create_count++;
        } else {
            if (found) {
                keep = (entry->version > index.fields
                        [entry->field_index].version);
            } else {
                keep = false;
            }
        }

        if (keep) {
            *dest++ = *entry;
        }
    }

    redo_array.count = dest - redo_array.entries;

    logInfo("file: "__FILE__", line: %d, "
            "record count: %d, redo {total: %d, create: %d, remove: %d, update: %d}",
            __LINE__, array->count, redo_array.count, create_count, remove_count,
            redo_array.count - (create_count + remove_count));

    if (redo_array.count > 0) {
        result = fdir_storage_engine_store(&redo_array);
    } else {
        result = 0;
    }

    free(redo_array.entries);
    return result;
}

int fdir_storage_engine_fetch(const int64_t inode,
        const int field_index, FastBuffer *buffer)
{
    return 0;
}

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

#include "sf/sf_func.h"
#include "diskallocator/storage_allocator.h"
#include "diskallocator/binlog/trunk/trunk_space_log.h"
#include "diskallocator/dio/trunk_write_thread.h"
#include "inode/segment_index.h"
#include "data_sync_thread.h"

int data_sync_thread_init()
{
    int result;
    FDIRDataSyncThreadInfo *thread;
    FDIRDataSyncThreadInfo *end;

    DATA_SYNC_THREAD_ARRAY.threads = fc_malloc(
            sizeof(FDIRDataSyncThreadInfo) *
            DATA_SYNC_THREAD_ARRAY.count);
    if (DATA_SYNC_THREAD_ARRAY.threads == NULL) {
        return ENOMEM;
    }

    end = DATA_SYNC_THREAD_ARRAY.threads + DATA_SYNC_THREAD_ARRAY.count;
    for (thread=DATA_SYNC_THREAD_ARRAY.threads; thread<end; thread++) {
        thread->thread_index = thread - DATA_SYNC_THREAD_ARRAY.threads;
        if ((result=fc_queue_init(&thread->queue, (long)
                        (&((FDIRDBUpdateFieldInfo *)NULL)->next))) != 0)
        {
            return result;
        }

        if ((result=sf_synchronize_ctx_init(&thread->synchronize_ctx)) != 0) {
            return result;
        }
    }

    return 0;
}

#define ADD_TO_SPACE_CHAIN(space_chain, record) \
    do {  \
        if (space_chain->head == NULL) { \
            space_chain->head = record;  \
        } else { \
            FC_SET_CHAIN_TAIL_NEXT(*space_chain,    \
                    DATrunkSpaceLogRecord, record); \
        } \
        space_chain->tail = record; \
    } while (0)

static inline int add_to_space_log_chain_ex(struct fc_queue_info *space_chain,
        const FDIRDBUpdateFieldInfo *dentry, const int field_index,
        const char op_type, const uint32_t trunk_id, const uint32_t offset,
        const uint32_t size)
{
    DATrunkSpaceLogRecord *record;

    if ((record=da_trunk_space_log_alloc_fill_record_ex(dentry->version,
                    dentry->inode, field_index, op_type, trunk_id,
                    offset, size)) == NULL)
    {
        return ENOMEM;
    }

    ADD_TO_SPACE_CHAIN(space_chain, record);
    return 0;
}

static inline int add_to_space_log_chain(struct fc_queue_info *space_chain,
        const FDIRDBUpdateFieldInfo *entry, const int field_index,
        const char op_type, const DAPieceFieldStorage *storage)
{
    DATrunkSpaceLogRecord *record;

    if ((record=da_trunk_space_log_alloc_fill_record(entry->version,
                    entry->inode, field_index, op_type, storage)) == NULL)
    {
        return ENOMEM;
    }

    ADD_TO_SPACE_CHAIN(space_chain, record);
    return 0;
}

static int remove_field(const FDIRDBUpdateFieldInfo *entry,
        const int field_index, const DAPieceFieldStorage *storage,
        FDIRInodeUpdateRecord *record)
{
    int result;

    if (DA_PIECE_FIELD_IS_EMPTY(storage)) {
        return 0;
    }

    if ((result=add_to_space_log_chain(&record->space_chain, entry,
                    field_index, da_binlog_op_type_reclaim_space,
                    storage)) != 0)
    {
        return result;
    }

    return 0;
}

static int remove_dentry(const FDIRDBUpdateFieldInfo *entry,
        FDIRInodeUpdateRecord *record)
{
    int result;
    int i;
    FDIRStorageInodeIndexInfo index;
    FDIRInodeUpdateResult r;

    index.inode = entry->inode;
    if ((result=inode_segment_index_delete(&index, &r)) != 0) {
        return result;
    }

    for (i=0; i<FDIR_PIECE_FIELD_COUNT; i++) {
        if ((result=remove_field(entry, i, index.fields + i, record)) != 0) {
            return result;
        }
    }

    record->version = r.version;
    record->field.inode = entry->inode;
    record->field.index = FDIR_PIECE_FIELD_INDEX_BASIC;
    record->field.storage.version = entry->version;
    record->field.op_type = da_binlog_op_type_remove;
    return 0;
}

static int set_dentry_field(FDIRDataSyncThreadInfo *thread,
        const FDIRDBUpdateFieldInfo *entry,
        FDIRInodeUpdateRecord *record)
{
    const bool normal_update = true;
    DATrunkSpaceInfo space;
    FDIRInodeUpdateResult r;
    int count;
    int result;

    record->field.inode = entry->inode;
    record->field.index = entry->field_index;
    record->field.storage.version = entry->version;
    if (entry->buffer == NULL) {
        DA_PIECE_FIELD_DELETE(&record->field.storage);
    } else {
        count = 1;
        if ((result=storage_allocator_normal_alloc(entry->inode,
                        entry->buffer->length, &space, &count)) != 0)
        {
            return result;
        }

        if ((result=trunk_write_thread_by_buff_synchronize(&space,
                        entry->buffer->data, &thread->synchronize_ctx)) != 0)
        {
            return result;
        }

        record->field.storage.trunk_id = space.id_info.id;
        record->field.storage.offset = space.offset;
        record->field.storage.size = space.size;
    }

    if (entry->op_type == da_binlog_op_type_create &&
            entry->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
    {
        record->field.op_type = da_binlog_op_type_create;
        if ((result=inode_segment_index_add(&record->field, &r)) != 0) {
            return result;
        }
        DA_PIECE_FIELD_SET_EMPTY(&r.old);
    } else {
        record->field.op_type = da_binlog_op_type_update;
        if ((result=inode_segment_index_update(&record->field,
                        normal_update, &r)) != 0)
        {
            return result;
        }
    }

    record->version = r.version;
    if (r.version == 0) {
        return result;
    }

    if ((result=remove_field(entry, entry->field_index,
                    &r.old, record)) != 0)
    {
        return result;
    }

    if (entry->buffer != NULL) {
        if ((result=add_to_space_log_chain_ex(&record->space_chain, entry,
                        entry->field_index, da_binlog_op_type_consume_space,
                        space.id_info.id, space.offset, space.size)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static void push_record_to_update_chain(FDIRInodeUpdateRecord *record)
{
    FDIRInodeUpdateRecord *previous;
    struct fc_queue_info qinfo;

    PTHREAD_MUTEX_LOCK(&ORDERED_UPDATE_CHAIN.lock);
    if (record->version == ORDERED_UPDATE_CHAIN.next_version) {
        qinfo.head = qinfo.tail = record;
        ++ORDERED_UPDATE_CHAIN.next_version;
        while (ORDERED_UPDATE_CHAIN.head != NULL &&
                ORDERED_UPDATE_CHAIN.head->version ==
                ORDERED_UPDATE_CHAIN.next_version)
        {
            FC_SET_CHAIN_TAIL_NEXT(qinfo, FDIRInodeUpdateRecord,
                    ORDERED_UPDATE_CHAIN.head);
            qinfo.tail = ORDERED_UPDATE_CHAIN.head;

            ++ORDERED_UPDATE_CHAIN.next_version;
            ORDERED_UPDATE_CHAIN.head = ORDERED_UPDATE_CHAIN.head->next;
        }
        if (ORDERED_UPDATE_CHAIN.head == NULL) {
            ORDERED_UPDATE_CHAIN.tail = NULL;
        }

        FC_SET_CHAIN_TAIL_NEXT(qinfo, FDIRInodeUpdateRecord, NULL);
        //TODO  push qinfo to queue
    } else {
        if (ORDERED_UPDATE_CHAIN.head == NULL) {
            ORDERED_UPDATE_CHAIN.head = record;
            ORDERED_UPDATE_CHAIN.tail = record;
            record->next = NULL;
        } else {
            if (record->version < ORDERED_UPDATE_CHAIN.head->version) {
                record->next = ORDERED_UPDATE_CHAIN.head;
                ORDERED_UPDATE_CHAIN.head = record;
            } else if (record->version > ORDERED_UPDATE_CHAIN.tail->version) {
                ORDERED_UPDATE_CHAIN.tail->next = record;
                record->next = NULL;
                ORDERED_UPDATE_CHAIN.tail = record;
            } else {
                previous = ORDERED_UPDATE_CHAIN.head;
                while (record->version > previous->next->version) {
                    previous = previous->next;
                }

                record->next = previous->next;
                previous->next = record;
            }
        }
    }
    PTHREAD_MUTEX_UNLOCK(&ORDERED_UPDATE_CHAIN.lock);
}

static int data_sync_thread_deal(FDIRDataSyncThreadInfo *thread,
        FDIRDBUpdateFieldInfo *head)
{
    FDIRDBUpdateFieldInfo *entry;
    FDIRInodeUpdateRecord *record;
    int count;
    int result;

    entry = head;
    count = 0;
    do {
        ++count;

        if ((record=(FDIRInodeUpdateRecord *)fast_mblock_alloc_object(
                        &UPDATE_RECORD_ALLOCATOR)) == NULL)
        {
            return ENOMEM;
        }
        record->space_chain.head = record->space_chain.tail = NULL;
        if (entry->op_type == da_binlog_op_type_remove) {
            result = remove_dentry(entry, record);
        } else {
            result = set_dentry_field(thread, entry, record);
        }

        if (result != 0) {
            return result;
        }

        if (record->version > 0) {
            FC_SET_CHAIN_TAIL_NEXT(record->space_chain,
                    DATrunkSpaceLogRecord, NULL);
            push_record_to_update_chain(record);
        } else {
            fast_mblock_free_object(&UPDATE_RECORD_ALLOCATOR, record);
        }
    } while ((entry=entry->next) != NULL);

    fdir_data_sync_finish(count);
    return 0;
}

static void *data_sync_thread_func(void *arg)
{
    FDIRDataSyncThreadInfo *thread;
    FDIRDBUpdateFieldInfo *head;

    thread = arg;
#ifdef OS_LINUX
    {
        char thread_name[16];
        prctl(PR_SET_NAME, "data-sync%02d", (int)(thread -
                    DATA_SYNC_THREAD_ARRAY.threads));
    }
#endif

    while (1) {
        if ((head=fc_queue_pop_all(&thread->queue)) != NULL) {
            if (data_sync_thread_deal(thread, head) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "deal dentry fail, program exit!",
                        __LINE__);
                sf_terminate_myself();
            }
        }
    }

    return NULL;
}

int data_sync_thread_start()
{
    int result;
    pthread_t tid;
    FDIRDataSyncThreadInfo *thread;
    FDIRDataSyncThreadInfo *end;

    end = DATA_SYNC_THREAD_ARRAY.threads + DATA_SYNC_THREAD_ARRAY.count;
    for (thread=DATA_SYNC_THREAD_ARRAY.threads; thread<end; thread++) {
        if ((result=fc_create_thread(&tid, data_sync_thread_func,
                        thread, SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    return 0;
}

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
            ((DATrunkSpaceLogRecord *)(space_chain->tail))->next = record; \
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
        const FDIRDBUpdateFieldInfo *dentry, const int field_index,
        const char op_type, const DAPieceFieldStorage *storage)
{
    DATrunkSpaceLogRecord *record;

    if ((record=da_trunk_space_log_alloc_fill_record(dentry->version,
                    dentry->inode, field_index, op_type, storage)) == NULL)
    {
        return ENOMEM;
    }

    ADD_TO_SPACE_CHAIN(space_chain, record);
    return 0;
}

static int remove_field(FDIRDataSyncThreadInfo *thread,
        const FDIRDBUpdateFieldInfo *dentry, const int field_index,
        const DAPieceFieldStorage *storage)
{
    int result;

    if (DA_PIECE_FIELD_IS_EMPTY(storage)) {
        return 0;
    }

    if ((result=add_to_space_log_chain(&thread->space_chain, dentry,
                    field_index, da_binlog_op_type_reclaim_space,
                    storage)) != 0)
    {
        return result;
    }

    return 0;
}

static int remove_dentry(FDIRDataSyncThreadInfo *thread,
        const FDIRDBUpdateFieldInfo *dentry)
{
    int result;
    int i;
    FDIRStorageInodeIndexInfo index;
    FDIRStorageInodeFieldInfo field;

    index.inode = dentry->inode;
    if ((result=inode_segment_index_delete(&index)) != 0) {
        return result;
    }

    for (i=0; i<FDIR_PIECE_FIELD_COUNT; i++) {
        if ((result=remove_field(thread, dentry,
                        i, index.fields + i)) != 0)
        {
            return result;
        }
    }

    field.inode = dentry->inode;
    field.index = FDIR_PIECE_FIELD_INDEX_BASIC;
    field.storage.version = dentry->version;
    field.op_type = da_binlog_op_type_remove;

    //TODO
    return 0;
}

static int set_field(FDIRDataSyncThreadInfo *thread,
        const FDIRDBUpdateFieldInfo *dentry,
        const FDIRDBUpdateMessage *msg)
{
    const bool normal_update = true;
    DATrunkSpaceInfo space;
    FDIRStorageInodeFieldInfo field;
    DAPieceFieldStorage old;
    bool modified;
    int count;
    int result;

    field.inode = dentry->inode;
    field.index = msg->field_index;
    field.storage.version = dentry->version;
    if (msg->buffer == NULL) {
        DA_PIECE_FIELD_DELETE(&field.storage);
    } else {
        count = 1;
        if ((result=storage_allocator_normal_alloc(dentry->inode,
                        msg->buffer->length, &space, &count)) != 0)
        {
            return result;
        }

        if ((result=trunk_write_thread_by_buff_synchronize(&space,
                        msg->buffer->data, &thread->synchronize_ctx)) != 0)
        {
            return result;
        }

        field.storage.trunk_id = space.id_info.id;
        field.storage.offset = space.offset;
        field.storage.size = space.size;
    }

    if (dentry->op_type == da_binlog_op_type_create &&
            msg->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
    {
        if ((result=inode_segment_index_add(&field)) != 0) {
            return result;
        }
        field.op_type = da_binlog_op_type_create;
        modified = true;
        DA_PIECE_FIELD_SET_EMPTY(&old);
    } else {
        if ((result=inode_segment_index_update(&field, normal_update,
                        &old, &modified)) != 0)
        {
            return result;
        }
        field.op_type = da_binlog_op_type_update;
    }

    if (!modified) {
        return result;
    }

    if ((result=remove_field(thread, dentry,
                    msg->field_index, &old)) != 0)
    {
        return result;
    }

    if (msg->buffer != NULL) {
        if ((result=add_to_space_log_chain_ex(&thread->space_chain, dentry,
                        msg->field_index, da_binlog_op_type_consume_space,
                        space.id_info.id, space.offset, space.size)) != 0)
        {
            return result;
        }
    }

    //TODO
    return 0;
}

static int set_dentry_fields(FDIRDataSyncThreadInfo *thread,
        const FDIRDBUpdateFieldInfo *dentry)
{
    const FDIRDBUpdateMessage *msg;
    const FDIRDBUpdateMessage *end;
    int result;

    end = dentry->mms.messages + dentry->mms.msg_count;
    for (msg=dentry->mms.messages; msg<end; msg++) {
        if ((result=set_field(thread, dentry, msg)) != 0) {
            return result;
        }
    }

    return 0;
}

static int data_sync_thread_deal(FDIRDataSyncThreadInfo *thread,
        FDIRDBUpdateFieldInfo *head)
{
    FDIRDBUpdateFieldInfo *dentry;
    int count;
    int result;

    thread->space_chain.head = thread->space_chain.tail = NULL;
    dentry = head;
    count = 0;
    do {
        ++count;

        if (dentry->op_type == da_binlog_op_type_remove) {
            result = remove_dentry(thread, dentry);
        } else {
            result = set_dentry_fields(thread, dentry);
        }

        if (result != 0) {
            return result;
        }
    } while ((dentry=dentry->next) != NULL);

    //TODO
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

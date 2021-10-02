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
        if ((result=fc_queue_init(&thread->queue, (long)
                        (&((FDIRDBUpdateDentry *)NULL)->next))) != 0)
        {
            return result;
        }
    }

    return 0;
}

/*
    DATrunkSpaceLogRecord *record;

    record = (DATrunkSpaceLogRecord *)fast_mblock_alloc_object(
            &g_trunk_space_log_ctx.record_allocator);
    if (record == NULL) {
        return ENOMEM;
    }

    record->version = version;
    record->oid = oid;
    record->fid = fid;
    record->op_type = op_type;
    */

static int remove_field(FDIRStorageInodeIndexInfo *index,
        const int field_index)
{
    if (DA_PIECE_FIELD_IS_EMPTY(index->fields + field_index)) {
        return 0;
    }

    return 0;
}

static int remove_dentry(FDIRStorageInodeIndexInfo *index)
{
    int i;

    for (i=0; i<FDIR_PIECE_FIELD_COUNT; i++) {
        remove_field(index, i);
    }

    inode_segment_index_delete(index->inode);
    return 0;
}

static int set_dentry_fields(FDIRDBUpdateDentry *dentry,
        FDIRStorageInodeIndexInfo *index)
{
    FDIRDBUpdateMessage *msg;
    FDIRDBUpdateMessage *end;
    /*
    DATrunkSpaceWithVersion space;
    int count;
    int result;
    */

    end = dentry->mms.messages + dentry->mms.msg_count;
    for (msg=dentry->mms.messages; msg<end; msg++) {
        if (msg->buffer != NULL) {
        } else {
        }
    }
    //storage_allocator_normal_alloc(blk_hc, size, spaces, count)
    return 0;
}

static int sync_dentry(FDIRDBUpdateDentry *dentry)
{
    int result;
    FDIRStorageInodeIndexInfo index;

    index.inode = dentry->inode;
    if ((result=inode_segment_index_get(&index)) != 0) {
        if (result != ENOENT) {
            return result;
        }
    }

    if (dentry->op_type == da_binlog_op_type_remove) {
        if (result != 0) {
            return result;
        }

        if ((result=remove_dentry(&index)) != 0) {
            return result;
        }
    } else {
        if ((result=set_dentry_fields(dentry, &index)) != 0) {
            return result;
        }
    }

    //TODO
    return 0;
}

static int data_sync_thread_deal(FDIRDBUpdateDentry *head)
{
    FDIRDBUpdateDentry *dentry;
    int count;
    int result;

    dentry = head;
    count = 0;
    do {
        ++count;
        if ((result=sync_dentry(dentry)) != 0) {
            return result;
        }
    } while ((dentry=dentry->next) != NULL);

    fdir_data_sync_finish(count);
    return 0;
}

static void *data_sync_thread_func(void *arg)
{
    FDIRDataSyncThreadInfo *thread;
    FDIRDBUpdateDentry *head;

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
            if (data_sync_thread_deal(head) != 0) {
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

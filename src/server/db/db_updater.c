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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "../server_global.h"
#include "../dentry.h"
#include "dentry_serializer.h"
#include "db_updater.h"

#define BUFFER_BATCH_FREE_COUNT  1024

typedef struct fdir_dentry_merged_messages {
    FDIRChangeNotifyMessage *messages[FDIR_PIECE_FIELD_COUNT];
    int msg_count;
    int merge_count;
} FDIRDentryMergedMessages;

typedef struct fdir_dentry_merged_messages_array {
    FDIRDentryMergedMessages *entries;
    int count;
    int alloc;
} FDIRDentryMergedMessagesArray;

typedef struct fdir_db_updater_context {
    int64_t last_version;
    FDIRChangeNotifyMessagePtrArray msg_ptr_array;
    FDIRDentryMergedMessagesArray merged_msg_array;
    struct {
        FastBuffer *buffers[BUFFER_BATCH_FREE_COUNT];
        int count;
    } buffer_ptr_array;

    //pthread_lock_cond_pair_t lc_pair;
} FDIRDBUpdaterContext;

static FDIRDBUpdaterContext db_updater_ctx;

#define MSG_PTR_ARRAY     db_updater_ctx.msg_ptr_array
#define MERGED_MSG_ARRAY  db_updater_ctx.merged_msg_array
#define BUFFER_PTR_ARRAY  db_updater_ctx.buffer_ptr_array

int db_updater_init()
{
    /*
    int result;

    if ((result=init_pthread_lock_cond_pair(&db_updater_ctx.lc_pair)) != 0) {
        return result;
    }
    */

    return 0;
}

void db_updater_destroy()
{
}

static int realloc_msg_ptr_array(FDIRChangeNotifyMessagePtrArray *array)
{
    FDIRChangeNotifyMessage **messages;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    messages = (FDIRChangeNotifyMessage **)fc_malloc(
            sizeof(FDIRChangeNotifyMessage *) * array->alloc);
    if (messages == NULL) {
        return ENOMEM;
    }

    if (array->messages != NULL) {
        memcpy(messages, array->messages, sizeof(
                    FDIRChangeNotifyMessage *) * array->count);
        free(array->messages);
    }

    array->messages = messages;
    return 0;
}

static int realloc_merged_msg_array(FDIRDentryMergedMessagesArray *array)
{
    FDIRDentryMergedMessages *entries;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    entries = (FDIRDentryMergedMessages *)fc_malloc(
            sizeof(FDIRDentryMergedMessages) * array->alloc);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (array->entries != NULL) {
        memcpy(entries, array->entries, sizeof(
                    FDIRDentryMergedMessages) * array->count);
        free(array->entries);
    }

    array->entries = entries;
    return 0;
}

static int add_to_msg_ptr_array(FDIRChangeNotifyEvent *event)
{
    int result;
    FDIRChangeNotifyMessage *msg;
    FDIRChangeNotifyMessage *end;

    if (MSG_PTR_ARRAY.count + event->marray.count > MSG_PTR_ARRAY.alloc) {
        if ((result=realloc_msg_ptr_array(&MSG_PTR_ARRAY)) != 0) {
            return result;
        }
    }

    end = event->marray.messages + event->marray.count;
    for (msg=event->marray.messages; msg<end; msg++) {
        MSG_PTR_ARRAY.messages[MSG_PTR_ARRAY.count++] = msg;
    }

    return 0;
}

static int compare_msg_ptr_func(const FDIRChangeNotifyMessage **msg1,
        const FDIRChangeNotifyMessage **msg2)
{
    int sub;
    if ((sub=fc_compare_int64((*msg1)->dentry->inode,
                    (*msg2)->dentry->inode)) != 0)
    {
        return sub;
    }

    if ((sub=(int)(*msg1)->field_index - (int)
                (*msg2)->field_index) != 0)
    {
        return sub;
    }

    return fc_compare_int64((*msg1)->version, (*msg2)->version);
}

static inline void free_message_buffer(
        FDIRChangeNotifyMessage **start,
        FDIRChangeNotifyMessage **end)
{
    FDIRChangeNotifyMessage **msg;

    for (msg=start; msg<end; msg++) {
        if ((*msg)->buffer == NULL) {
            continue;
        }

        BUFFER_PTR_ARRAY.buffers[BUFFER_PTR_ARRAY.count++] = (*msg)->buffer;
        if (BUFFER_PTR_ARRAY.count == BUFFER_BATCH_FREE_COUNT) {
            dentry_serializer_batch_free_buffer(
                    BUFFER_PTR_ARRAY.buffers,
                    BUFFER_PTR_ARRAY.count);
            BUFFER_PTR_ARRAY.count = 0;
        }
    }
}

static int merge_one_field_messages(FDIRDentryMergedMessages *merged,
        FDIRChangeNotifyMessage **start, FDIRChangeNotifyMessage **end)
{
    FDIRChangeNotifyMessage **last;

    if ((*start)->field_index == FDIR_PIECE_FIELD_INDEX_CHILDREN) {
        //TODO
    } else {
        last = end - 1;
        merged->messages[merged->msg_count++] = *last;
        free_message_buffer(start, last);
    }

    return 0;
}

static int merge_one_dentry_messages(FDIRChangeNotifyMessage **start,
        FDIRChangeNotifyMessage **end)
{
    int result;
    FDIRChangeNotifyMessage **last;
    FDIRChangeNotifyMessage **msg;
    FDIRDentryMergedMessages *merged;

    if (MERGED_MSG_ARRAY.count >= MERGED_MSG_ARRAY.alloc) {
        if ((result=realloc_merged_msg_array(&MERGED_MSG_ARRAY)) != 0) {
            return result;
        }
    }

    merged = MERGED_MSG_ARRAY.entries + MERGED_MSG_ARRAY.count;
    merged->merge_count = end - start;

    last = end - 1;
    if ((*last)->op_type == da_binlog_op_type_remove && (*last)->
            field_index == FDIR_PIECE_FIELD_INDEX_FOR_REMOVE)
    {
        merged->messages[0] = *last;
        merged->msg_count = 1;
        free_message_buffer(start, last);
        return 0;
    }

    merged->msg_count = 0;
    for (msg=start + 1; msg<end; msg++) {
        if ((*msg)->field_index != (*start)->field_index) {
            if ((result=merge_one_field_messages(merged, start, msg)) != 0) {
                return result;
            }
            start = msg;
        }
    }

    if ((result=merge_one_field_messages(merged, start, msg)) != 0) {
        return result;
    }

    MERGED_MSG_ARRAY.count++;
    return 0;
}

static int merge_messages()
{
    int result;
    FDIRChangeNotifyMessage **msg;
    FDIRChangeNotifyMessage **start;
    FDIRChangeNotifyMessage **end;

    MERGED_MSG_ARRAY.count = 0;
    end = MSG_PTR_ARRAY.messages + MSG_PTR_ARRAY.count;
    start = MSG_PTR_ARRAY.messages;
    for (msg=start + 1; msg<end; msg++) {
        if ((*msg)->dentry != (*start)->dentry) {
            if ((result=merge_one_dentry_messages(start, msg)) != 0) {
                return result;
            }
            start = msg;
        }
    }

    return merge_one_dentry_messages(start, msg);
}

int db_updater_deal_events(FDIRChangeNotifyEvent *head, int *count)
{
    int result;
    FDIRChangeNotifyEvent *event;
    FDIRChangeNotifyEvent *last;

    MSG_PTR_ARRAY.count = 0;
    *count = 0;
    event = head;
    do {
        ++(*count);

        if ((result=add_to_msg_ptr_array(event)) != 0) {
            return result;
        }

        last = event;
        event = event->next;
    } while (event != NULL);

    db_updater_ctx.last_version = last->version;
    if (MSG_PTR_ARRAY.count > 1) {
        qsort(MSG_PTR_ARRAY.messages, MSG_PTR_ARRAY.count,
                sizeof(FDIRChangeNotifyMessage *),
                (int (*)(const void *, const void *))compare_msg_ptr_func);
    }

    if ((result=merge_messages()) != 0) {
        return result;
    }

    if (BUFFER_PTR_ARRAY.count > 0) {
        dentry_serializer_batch_free_buffer(
                BUFFER_PTR_ARRAY.buffers,
                BUFFER_PTR_ARRAY.count);
        BUFFER_PTR_ARRAY.count = 0;
    }

    return result;
}

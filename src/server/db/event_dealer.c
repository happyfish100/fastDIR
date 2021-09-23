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
#include "event_dealer.h"

#define BUFFER_BATCH_FREE_COUNT  1024

typedef struct fdir_event_dealer_context {
    FDIRChangeNotifyMessagePtrArray msg_ptr_array;
    FDIRDBUpdaterContext updater_ctx;
    struct {
        FastBuffer *buffers[BUFFER_BATCH_FREE_COUNT];
        int count;
    } buffer_ptr_array;
} FDIREventDealerContext;

static FDIREventDealerContext event_dealer_ctx;

#define MSG_PTR_ARRAY       event_dealer_ctx.msg_ptr_array
#define MERGED_DENTRY_ARRAY event_dealer_ctx.updater_ctx.array
#define BUFFER_PTR_ARRAY    event_dealer_ctx.buffer_ptr_array

int event_dealer_init()
{
    int result;

    if ((result=fast_buffer_init_ex(&event_dealer_ctx.
                    updater_ctx.buffer, 1024)) != 0)
    {
        return result;
    }

    return 0;
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

static int insert_children(FDIRServerDentry *dentry, const int64_t inode)
{
    if (dentry->db_args->children == NULL || dentry->db_args->
            children->alloc <= dentry->db_args->children->count)
    {
        dentry->db_args->children = i64_array_allocator_realloc(
                &I64_ARRAY_ALLOCATOR_CTX, dentry->db_args->children,
                dentry->db_args->children->count + 1);
        if (dentry->db_args->children == NULL) {
            return ENOMEM;
        }
    }

    return sorted_array_insert(&I64_SORTED_ARRAY_CTX, dentry->db_args->
            children->elts, &dentry->db_args->children->count, &inode);
}

static inline void copy_message(FDIRDBUpdateMessage *umsg,
        const FDIRChangeNotifyMessage *nmsg)
{
    umsg->buffer = nmsg->buffer;
    umsg->field_index = nmsg->field_index;
}

static int merge_children_messages(FDIRDBUpdateDentry *merged,
        FDIRChangeNotifyMessage **start, FDIRChangeNotifyMessage **end)
{
    int result;
    FDIRChangeNotifyMessage **msg;

    for (msg=start; msg<end; msg++) {
        if ((*msg)->op_type == da_binlog_op_type_create) {
            if ((result=insert_children((*msg)->dentry,
                            (*msg)->child)) != 0)
            {
                if (result == ENOMEM) {
                    return result;
                } else {
                    logWarning("file: "__FILE__", line: %d, "
                            "inode: %"PRId64", insert child %"PRId64" "
                            "fail, errno: %d, error info: %s", __LINE__,
                            (*msg)->dentry->inode, (*msg)->child,
                            result, STRERROR(result));
                }
            }
        } else {
            if ((*msg)->dentry->db_args->children == NULL) {
                logWarning("file: "__FILE__", line: %d, "
                        "inode: %"PRId64", child inode: %"PRId64", "
                        "the children array is NULL!", __LINE__,
                        (*msg)->dentry->inode, (*msg)->child);
                continue;
            }

            if ((result=sorted_array_delete(&I64_SORTED_ARRAY_CTX,
                            (*msg)->dentry->db_args->children->elts,
                            &(*msg)->dentry->db_args->children->count,
                            &(*msg)->child)) != 0)
            {
                logWarning("file: "__FILE__", line: %d, "
                        "inode: %"PRId64", delete child %"PRId64" fail, "
                        "errno: %d, error info: %s", __LINE__,
                        (*msg)->dentry->inode, (*msg)->child,
                        result, STRERROR(result));
            }
        }
    }

    if ((result=dentry_serializer_pack((*start)->dentry, (*start)->
                    field_index, &(*start)->buffer)) != 0)
    {
        return result;
    }

    copy_message(merged->mms.messages + merged->mms.msg_count++, *start);
    return 0;
}

static int merge_one_field_messages(FDIRDBUpdateDentry *merged,
        FDIRChangeNotifyMessage **start, FDIRChangeNotifyMessage **end)
{
    FDIRChangeNotifyMessage **last;

    if ((*start)->field_index == FDIR_PIECE_FIELD_INDEX_CHILDREN) {
        return merge_children_messages(merged, start, end);
    } else {
        last = end - 1;
        copy_message(merged->mms.messages + merged->mms.msg_count++, *last);
        free_message_buffer(start, last);
        return 0;
    }
}

static int merge_one_dentry_messages(FDIRChangeNotifyMessage **start,
        FDIRChangeNotifyMessage **end)
{
    int result;
    FDIRChangeNotifyMessage **last;
    FDIRChangeNotifyMessage **msg;
    FDIRDBUpdateDentry *merged;

    if (MERGED_DENTRY_ARRAY.count >= MERGED_DENTRY_ARRAY.alloc) {
        if ((result=db_updater_realloc_dentry_array(
                        &MERGED_DENTRY_ARRAY)) != 0)
        {
            return result;
        }
    }

    last = end - 1;
    merged = MERGED_DENTRY_ARRAY.entries + MERGED_DENTRY_ARRAY.count;
    merged->version = (*last)->version;
    merged->inode = (*start)->dentry->inode;
    merged->args = (*start)->dentry;
    merged->fields.ptr = (*start)->dentry->db_args->fields;
    merged->mms.merge_count = end - start;
    merged->mms.msg_count = 0;

    if ((*last)->op_type == da_binlog_op_type_remove && (*last)->
            field_index == FDIR_PIECE_FIELD_INDEX_FOR_REMOVE)
    {
        if ((*start)->op_type == da_binlog_op_type_create &&
                (*start)->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            dentry_release_ex((*start)->dentry, merged->mms.merge_count);
        } else {
            merged->op_type = da_binlog_op_type_remove;
            MERGED_DENTRY_ARRAY.count++;
        }
        free_message_buffer(start, last);
        return 0;
    }

    if ((*start)->op_type == da_binlog_op_type_create &&
            (*start)->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
    {
        merged->op_type = da_binlog_op_type_create;
    } else {
        merged->op_type = da_binlog_op_type_update;
    }

    for (msg=start + 1; msg<end; msg++) {
        if ((*msg)->field_index != (*start)->field_index) {
            if ((result=merge_one_field_messages(merged,
                            start, msg)) != 0)
            {
                return result;
            }
            start = msg;
        }
    }

    if ((result=merge_one_field_messages(merged,
                    start, msg)) != 0)
    {
        return result;
    }

    MERGED_DENTRY_ARRAY.count++;
    return 0;
}

static int merge_messages()
{
    int result;
    FDIRChangeNotifyMessage **msg;
    FDIRChangeNotifyMessage **start;
    FDIRChangeNotifyMessage **end;

    MERGED_DENTRY_ARRAY.count = 0;
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

    result = merge_one_dentry_messages(start, msg);

    if (BUFFER_PTR_ARRAY.count > 0) {
        dentry_serializer_batch_free_buffer(
                BUFFER_PTR_ARRAY.buffers,
                BUFFER_PTR_ARRAY.count);
        BUFFER_PTR_ARRAY.count = 0;
    }
    return result;
}

static int deal_merged_entries()
{
    int result;
    FDIRDBUpdateDentry *entry;
    FDIRDBUpdateDentry *end;

    result = db_updater_deal(&event_dealer_ctx.updater_ctx);
    end = MERGED_DENTRY_ARRAY.entries + MERGED_DENTRY_ARRAY.count;
    for (entry=MERGED_DENTRY_ARRAY.entries; entry<end; entry++) {
        dentry_release_ex(entry->args, entry->mms.merge_count);
    }

    return result;
}

int event_dealer_do(FDIRChangeNotifyEvent *head, int *count)
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

    if (MSG_PTR_ARRAY.count > 1) {
        qsort(MSG_PTR_ARRAY.messages, MSG_PTR_ARRAY.count,
                sizeof(FDIRChangeNotifyMessage *),
                (int (*)(const void *, const void *))compare_msg_ptr_func);
    }

    if ((result=merge_messages()) != 0) {
        return result;
    }

    event_dealer_ctx.updater_ctx.last_version = last->version;
    if ((result=deal_merged_entries()) != 0) {
        return result;
    }

    return result;
}

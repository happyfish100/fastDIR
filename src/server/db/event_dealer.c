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

#define UPDATER_CTX         event_dealer_ctx.updater_ctx
#define MSG_PTR_ARRAY       event_dealer_ctx.msg_ptr_array
#define MERGED_DENTRY_ARRAY event_dealer_ctx.updater_ctx.array
#define BUFFER_PTR_ARRAY    event_dealer_ctx.buffer_ptr_array

int event_dealer_init()
{
    int result;

    if ((result=fast_buffer_init_ex(&UPDATER_CTX.buffer, 1024)) != 0) {
        return result;
    }

    return db_updater_init(&UPDATER_CTX);
}

int64_t event_dealer_get_last_data_version()
{
    return UPDATER_CTX.last_versions.dentry.commit;
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
        memcpy(messages, array->messages,
                sizeof(FDIRChangeNotifyMessage *) * array->count);
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

    return fc_compare_int64((*msg1)->id, (*msg2)->id);
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

static int insert_children(FDIRServerDentry *dentry,
        const id_name_pair_t *child)
{
    int target_count;
    id_name_pair_t *elt;

    if (CHILDREN_CONTAINER == fdir_children_container_skiplist) {
        if ((elt=fast_mblock_alloc_object(&dentry->context->
                            db_args.child_allocator)) == NULL)
        {
            return ENOMEM;
        }

        *elt = *child;
        return uniq_skiplist_insert(dentry->db_args->children.sl, elt);
    } else {
        if (dentry->db_args->children.sa == NULL || dentry->db_args->
                children.sa->alloc <= dentry->db_args->children.sa->count)
        {
            target_count = dentry->db_args->children.sa != NULL ?
                dentry->db_args->children.sa->count + 1 : 1;
            dentry->db_args->children.sa = id_name_array_allocator_realloc(
                    &ID_NAME_ARRAY_ALLOCATOR_CTX, dentry->db_args->
                    children.sa, target_count);
            if (dentry->db_args->children.sa == NULL) {
                return ENOMEM;
            }
        }

        return sorted_array_insert(&ID_NAME_SORTED_ARRAY_CTX, dentry->db_args->
                children.sa->elts, &dentry->db_args->children.sa->count, child);
    }
}

static int remove_or_update_children(FDIRServerDentry *dentry,
        const DABinlogOpType op_type, const id_name_pair_t *child)
{
    id_name_pair_t *found;
    id_name_pair_t *elt;

    if (dentry->db_args->children.ptr == NULL) {
        logWarning("file: "__FILE__", line: %d, "
                "inode: %"PRId64", op_type: %c, array "
                "child inode: %"PRId64", the children "
                "is NULL!", __LINE__, dentry->inode,
                op_type, child->id);
        return EBUSY;
    }

    if (CHILDREN_CONTAINER == fdir_children_container_skiplist) {
        if (op_type == da_binlog_op_type_remove) {
            return uniq_skiplist_delete(dentry->db_args->
                    children.sl, (void *)child);
        } else {
            if ((elt=fast_mblock_alloc_object(&dentry->context->
                            db_args.child_allocator)) == NULL)
            {
                return ENOMEM;
            }

            *elt = *child;
            return uniq_skiplist_replace(dentry->db_args->children.sl, elt);
        }
    } else {
        if ((found=sorted_array_find(&ID_NAME_SORTED_ARRAY_CTX,
                        dentry->db_args->children.sa->elts,
                        dentry->db_args->children.sa->count,
                        child)) == NULL)
        {
            id_name_pair_t *pair;
            id_name_pair_t *end;

            end = dentry->db_args->children.sa->elts +
                dentry->db_args->children.sa->count;
            for (pair=dentry->db_args->children.sa->elts; pair<end; pair++) {
                logInfo("children[%d]: %"PRId64, (int)(pair - dentry->
                            db_args->children.sa->elts) + 1, pair->id);
            }

            return ENOENT;
        }

        server_immediate_free_str(dentry->context,
                found->name.str);

        if (op_type == da_binlog_op_type_remove) {
            sorted_array_delete_by_index(&ID_NAME_SORTED_ARRAY_CTX,
                    dentry->db_args->children.sa->elts,
                    &dentry->db_args->children.sa->count,
                    found - (id_name_pair_t *)dentry->
                    db_args->children.sa->elts);
        } else { //update
            found->name = child->name;
        }
    }

    return 0;
}

static int merge_children_messages(FDIRDBUpdateFieldInfo *merged,
        FDIRChangeNotifyMessage **start, FDIRChangeNotifyMessage **end)
{
    int result;
    FDIRChangeNotifyMessage **msg;

    for (msg=start; msg<end; msg++) {
        if ((int64_t)(*msg)->child.id < 0) {
            continue;
        }

        if ((*msg)->op_type == da_binlog_op_type_create) {
            if ((result=insert_children((*msg)->dentry,
                            &(*msg)->child)) != 0)
            {
                if (result == ENOMEM) {
                    return result;
                } else {
                    logWarning("file: "__FILE__", line: %d, "
                            "inode: %"PRId64", insert child {inode: %"PRId64
                            ", name: %.*s} fail, errno: %d, error info: %s",
                            __LINE__, (*msg)->dentry->inode, (*msg)->child.id,
                            (*msg)->child.name.len, (*msg)->child.name.str,
                            result, STRERROR(result));
                }
            }
        } else {
            if ((result=remove_or_update_children((*msg)->dentry, (*msg)->
                            op_type, &(*msg)->child)) == ENOMEM)
            {
                return result;
            }

            if (result == ENOENT) {
                logWarning("file: "__FILE__", line: %d, "
                        "parent inode: %"PRId64", op_type: %c, "
                        "child %"PRId64" not exist", __LINE__,
                        (*msg)->dentry->inode, (*msg)->op_type,
                        (*msg)->child.id);
            }
        }
    }

    if ((result=dentry_serializer_pack((*start)->dentry, (*start)->
                    field_index, &merged->buffer)) != 0)
    {
        return result;
    }

    return 0;
}

static int merge_one_field_messages(FDIRChangeNotifyMessage **start,
        FDIRChangeNotifyMessage **end)
{
    FDIRChangeNotifyMessage **last;
    FDIRChangeNotifyMessage **msg;
    FDIRDBUpdateFieldInfo *merged;

    last = end - 1;
    merged = MERGED_DENTRY_ARRAY.entries + MERGED_DENTRY_ARRAY.count++;
    merged->version = ++UPDATER_CTX.last_versions.field;
    merged->inode = (*start)->dentry->inode;
    merged->field_index = (*last)->field_index;
    merged->args = (*start)->dentry;
    merged->merge_count = end - start;
    merged->inc_alloc = 0;
    merged->mode = (*start)->dentry->stat.mode;
    merged->namespace_id = (*start)->dentry->ns_entry->id;
    merged->flags = 0;

    if ((*last)->field_index == FDIR_PIECE_FIELD_INDEX_CHILDREN) {
        merged->op_type = da_binlog_op_type_update;
        return merge_children_messages(merged, start, end);
    } else {
        if ((*last)->op_type == da_binlog_op_type_remove &&
                (*last)->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            merged->op_type = da_binlog_op_type_remove;
        }
        else if ((*start)->op_type == da_binlog_op_type_create &&
                (*start)->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            merged->op_type = da_binlog_op_type_create;
        } else {
            merged->op_type = da_binlog_op_type_update;
        }

        for (msg=start; msg<end; msg++) {
            merged->inc_alloc += (*msg)->inc_alloc;
        }

        merged->buffer = (*last)->buffer;
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

    if (MERGED_DENTRY_ARRAY.count + FDIR_PIECE_FIELD_COUNT >
            MERGED_DENTRY_ARRAY.alloc)
    {
        if ((result=db_updater_realloc_dentry_array(
                        &MERGED_DENTRY_ARRAY)) != 0)
        {
            return result;
        }
    }

    last = end - 1;
    if ((*last)->op_type == da_binlog_op_type_remove && (*last)->
            field_index == FDIR_PIECE_FIELD_INDEX_FOR_REMOVE)
    {
        if ((*start)->op_type == da_binlog_op_type_create &&
                (*start)->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            dentry_release_ex((*start)->dentry, end - start);
            free_message_buffer(start, last);
        } else {
            (*last)->field_index = FDIR_PIECE_FIELD_INDEX_BASIC;
            merge_one_field_messages(start, end);
        }
        return 0;
    }

    for (msg=start + 1; msg<end; msg++) {
        if ((*msg)->field_index != (*start)->field_index) {
            if ((result=merge_one_field_messages(start, msg)) != 0) {
                return result;
            }
            start = msg;
        }
    }

    return merge_one_field_messages(start, msg);
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
    FDIRDBUpdateFieldInfo *entry;
    FDIRDBUpdateFieldInfo *end;

    if ((result=db_updater_deal(&UPDATER_CTX)) != 0) {
        return result;
    }

    end = MERGED_DENTRY_ARRAY.entries + MERGED_DENTRY_ARRAY.count;
    for (entry=MERGED_DENTRY_ARRAY.entries; entry<end; entry++) {
        dentry_release_ex(entry->args, entry->merge_count);
    }

    event_dealer_free_buffers(&MERGED_DENTRY_ARRAY);
    return 0;
}

int event_dealer_do(struct fc_list_head *head, int *count)
{
    int result;
    FDIRChangeNotifyEvent *event;
    FDIRChangeNotifyEvent *last;

    last = fc_list_entry(head->prev, FDIRChangeNotifyEvent, dlink);
    UPDATER_CTX.last_versions.dentry.prepare = last->version;
    MSG_PTR_ARRAY.count = 0;
    *count = 0;
    fc_list_for_each_entry (event, head, dlink) {
        ++(*count);
        if ((result=add_to_msg_ptr_array(event)) != 0) {
            return result;
        }

        if (event->version > UPDATER_CTX.last_versions.dentry.prepare) {
            UPDATER_CTX.last_versions.dentry.prepare = event->version;
        }
    }

    if (MSG_PTR_ARRAY.count > 1) {
        qsort(MSG_PTR_ARRAY.messages, MSG_PTR_ARRAY.count,
                sizeof(FDIRChangeNotifyMessage *),
                (int (*)(const void *, const void *))compare_msg_ptr_func);
    }

    if ((result=merge_messages()) != 0) {
        return result;
    }

    if ((result=deal_merged_entries()) != 0) {
        return result;
    }
    UPDATER_CTX.last_versions.dentry.commit =
        UPDATER_CTX.last_versions.dentry.prepare;

    return result;
}

void event_dealer_free_buffers(FDIRDBUpdateFieldArray *array)
{
    FDIRDBUpdateFieldInfo *entry;
    FDIRDBUpdateFieldInfo *end;

    end = array->entries + array->count;
    for (entry=array->entries; entry<end; entry++) {
        if (entry->buffer == NULL) {
            continue;
        }

        BUFFER_PTR_ARRAY.buffers[BUFFER_PTR_ARRAY.count++] = entry->buffer;
        if (BUFFER_PTR_ARRAY.count == BUFFER_BATCH_FREE_COUNT) {
            dentry_serializer_batch_free_buffer(
                    BUFFER_PTR_ARRAY.buffers,
                    BUFFER_PTR_ARRAY.count);
            BUFFER_PTR_ARRAY.count = 0;
        }
    }

    if (BUFFER_PTR_ARRAY.count > 0) {
        dentry_serializer_batch_free_buffer(
                BUFFER_PTR_ARRAY.buffers,
                BUFFER_PTR_ARRAY.count);
        BUFFER_PTR_ARRAY.count = 0;
    }
}

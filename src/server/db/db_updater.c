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
#include "db_updater.h"

typedef struct fdir_db_updater_context {
    FDIRChangeNotifyMessagePtrArray msg_ptr_array;
    //pthread_lock_cond_pair_t lc_pair;
} FDIRDBUpdaterContext;

static FDIRDBUpdaterContext db_updater_ctx;

#define MSG_PTR_ARRAY  db_updater_ctx.msg_ptr_array

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

int db_updater_deal_events(FDIRChangeNotifyEvent *head, int *count)
{
    int result;
    FDIRChangeNotifyEvent *event;

    MSG_PTR_ARRAY.count = 0;
    *count = 0;
    event = head;
    do {
        ++(*count);

        if ((result=add_to_msg_ptr_array(event)) != 0) {
            return result;
        }
        event = event->next;
    } while (event != NULL);

    if (MSG_PTR_ARRAY.count > 1) {
        qsort(MSG_PTR_ARRAY.messages, MSG_PTR_ARRAY.count,
                sizeof(FDIRChangeNotifyMessage *),
                (int (*)(const void *, const void *))compare_msg_ptr_func);
    }

    return result;
}

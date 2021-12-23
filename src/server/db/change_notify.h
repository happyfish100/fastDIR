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


#ifndef _FDIR_CHANGE_NOTIFY_H
#define _FDIR_CHANGE_NOTIFY_H

#include "../server_types.h"

#define FDIR_CHANGE_NOTIFY_MAX_MSGS_PER_EVENT  8

struct fdir_data_thread_context;

typedef struct fdir_change_notify_message {
    int64_t id;  //for stable sort
    FDIRServerDentry *dentry;
    int field_index;
    DABinlogOpType op_type;
    FastBuffer *buffer;
    id_name_pair_t child; //child inode and name
    int64_t inc_alloc;    //inode allocate/deallocate space increment
} FDIRChangeNotifyMessage;

typedef struct fdir_change_notify_message_array {
    FDIRChangeNotifyMessage messages[FDIR_CHANGE_NOTIFY_MAX_MSGS_PER_EVENT];
    int count;
} FDIRChangeNotifyMessageArray;

typedef struct fdir_change_notify_message_ptr_array {
    FDIRChangeNotifyMessage **messages;
    int count;
    int alloc;
} FDIRChangeNotifyMessagePtrArray;

typedef struct fdir_change_notify_event {
    int64_t version;
    FDIRChangeNotifyMessageArray marray;
    struct fdir_data_thread_context *thread_ctx;
    struct fdir_change_notify_event *next; //for queue
} FDIRChangeNotifyEvent;

#define FDIR_CHANGE_NOTIFY_FILL_MESSAGE(message, ent, type, index, alloc) \
    (message)->dentry = ent;   \
    (message)->op_type = type; \
    (message)->field_index = index; \
    (message)->inc_alloc = alloc

#define FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(message, ent, type, index, alloc) \
    FDIR_CHANGE_NOTIFY_FILL_MESSAGE(message, ent, type, index, alloc); (message)++

#ifdef __cplusplus
extern "C" {
#endif

    int change_notify_init();
    void change_notify_destroy();

    void change_notify_push_to_queue(FDIRChangeNotifyEvent *event);

    void change_notify_load_done_signal();

#ifdef __cplusplus
}
#endif

#endif

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

//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "sf/sf_binlog_writer.h"
#include "../server_types.h"

#define BINLOG_OP_NONE_INT           0
#define BINLOG_OP_CREATE_DENTRY_INT  1
#define BINLOG_OP_REMOVE_DENTRY_INT  2
#define BINLOG_OP_RENAME_DENTRY_INT  3
#define BINLOG_OP_UPDATE_DENTRY_INT  4
#define BINLOG_OP_SET_XATTR_INT      5
#define BINLOG_OP_REMOVE_XATTR_INT   6

#define BINLOG_OP_NONE_STR           ""
#define BINLOG_OP_CREATE_DENTRY_STR  "cr"
#define BINLOG_OP_REMOVE_DENTRY_STR  "rm"
#define BINLOG_OP_RENAME_DENTRY_STR  "rn"
#define BINLOG_OP_UPDATE_DENTRY_STR  "up"
#define BINLOG_OP_SET_XATTR_STR      "sx"
#define BINLOG_OP_REMOVE_XATTR_STR   "rx"

#define BINLOG_OP_CREATE_DENTRY_LEN  (sizeof(BINLOG_OP_CREATE_DENTRY_STR) - 1)
#define BINLOG_OP_REMOVE_DENTRY_LEN  (sizeof(BINLOG_OP_REMOVE_DENTRY_STR) - 1)
#define BINLOG_OP_RENAME_DENTRY_LEN  (sizeof(BINLOG_OP_RENAME_DENTRY_STR) - 1)
#define BINLOG_OP_UPDATE_DENTRY_LEN  (sizeof(BINLOG_OP_UPDATE_DENTRY_STR) - 1)
#define BINLOG_OP_SET_XATTR_LEN      (sizeof(BINLOG_OP_SET_XATTR_STR) - 1)
#define BINLOG_OP_REMOVE_XATTR_LEN   (sizeof(BINLOG_OP_REMOVE_XATTR_STR) - 1)

#define BINLOG_OPTIONS_PATH_ENABLED  (1 | (1 << 1))

#define BINLOG_BUFFER_INIT_SIZE      4096
#define BINLOG_BUFFER_LENGTH(buffer) ((buffer).end - (buffer).buff)
#define BINLOG_BUFFER_REMAIN(buffer) ((buffer).end - (buffer).current)

struct server_binlog_record_buffer;

typedef void (*data_thread_notify_func)(struct fdir_binlog_record *record,
        const int result, const bool is_error);

typedef void (*release_binlog_rbuffer_func)(
        struct server_binlog_record_buffer *rbuffer);

typedef struct {
    FDIRDEntryPName pname;
    FDIRServerDentry *parent;
    FDIRServerDentry *dentry;
} FDIRRecordDEntry;

typedef enum {
    fdir_record_type_load = 'l',
    fdir_record_type_update = 'u'
} FDIRRecordType;

typedef struct fdir_binlog_record {
    uint64_t data_version;
    int64_t inode;
    string_t ns;   //namespace
    FDIRRecordType type;
    unsigned int hash_code;
    int operation;
    int timestamp;
    int flags;
    FDIRStatModifyFlags options;

    union {
        struct {
            FDIRRecordDEntry dest;  //must be the first
            FDIRRecordDEntry src;
            FDIRServerDentry *overwritten;
        } rename;

        struct {
            FDIRRecordDEntry dest;  //must be the first
            int64_t src_inode;
            FDIRServerDentry *src_dentry;
        } hdlink;

        FDIRRecordDEntry me;  //for create and remove
    };

    /* removed dentries for rename and remove operation */
    struct {
        FDIRServerDentry *dentries[2];
        int count;
    } removed;

    FDIRDEntryStat stat;

    union {
        string_t link;
        key_value_pair_t xattr;
    };

    //must be the last to avoid being overwritten by memset
    struct {
        data_thread_notify_func func;
        void *args;    //for thread continue deal
    } notify;

    struct {
        short data_thread_index;
        short arr_index;
    } extra;   //for data loader

    struct fdir_binlog_record *next; //for data thread queue
} FDIRBinlogRecord;

typedef struct server_binlog_record_buffer {
    SFVersionRange data_version; //for binlog writer and idempotency (slave only)
    volatile int reffer_count;
    void *args;  //for notify & release 
    release_binlog_rbuffer_func release_func;
    FastBuffer buffer;
    struct server_binlog_record_buffer *next;      //for producer
    struct server_binlog_record_buffer *nexts[0];  //for slave replications
} ServerBinlogRecordBuffer;

#ifdef __cplusplus
extern "C" {
#endif

static inline const char *get_operation_caption(const int operation)
{
    switch (operation) {
        case BINLOG_OP_CREATE_DENTRY_INT:
            return "CREATE";
        case BINLOG_OP_REMOVE_DENTRY_INT:
            return "REMOVE";
        case BINLOG_OP_RENAME_DENTRY_INT:
            return "RENAME";
        case BINLOG_OP_UPDATE_DENTRY_INT:
            return "UPDATE";
        case BINLOG_OP_SET_XATTR_INT:
            return "SET_XATTR";
        case BINLOG_OP_REMOVE_XATTR_INT:
            return "REMOVE_XATTR";
        default:
            return "UNKOWN";
    }
}

#ifdef __cplusplus
}
#endif

#endif

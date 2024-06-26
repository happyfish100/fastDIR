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
#include "../flock.h"

#define BINLOG_OP_NONE_INT           0
#define BINLOG_OP_CREATE_DENTRY_INT  1
#define BINLOG_OP_REMOVE_DENTRY_INT  2
#define BINLOG_OP_RENAME_DENTRY_INT  3
#define BINLOG_OP_UPDATE_DENTRY_INT  4
#define BINLOG_OP_SET_XATTR_INT      5
#define BINLOG_OP_REMOVE_XATTR_INT   6
#define BINLOG_OP_NO_OP_INT         10
#define BINLOG_OP_DUMP_DENTRY_INT   11

#define SERVER_OP_DUMP_DATA_INT         100
#define SERVICE_OP_SET_DSIZE_INT        101
#define SERVICE_OP_BATCH_SET_DSIZE_INT  102

#define SERVICE_OP_SYS_LOCK_APPLY_INT   111
#define SERVICE_OP_SYS_LOCK_RELEASE_INT 112
#define SERVICE_OP_FLOCK_APPLY_INT      113
#define SERVICE_OP_FLOCK_UNLOCK_INT     114
#define SERVICE_OP_FLOCK_GETLK_INT      115

#define SERVICE_OP_ACCESS_DENTRY_INT    120
#define SERVICE_OP_STAT_DENTRY_INT      121
#define SERVICE_OP_READ_LINK_INT        122
#define SERVICE_OP_LOOKUP_INODE_INT     123
#define SERVICE_OP_LIST_DENTRY_INT      124
#define SERVICE_OP_GET_XATTR_INT        125
#define SERVICE_OP_LIST_XATTR_INT       126
#define SERVICE_OP_GET_FULLNAME_INT     127

#define BINLOG_OP_NONE_STR           ""
#define BINLOG_OP_CREATE_DENTRY_STR  "cr"
#define BINLOG_OP_REMOVE_DENTRY_STR  "rm"
#define BINLOG_OP_RENAME_DENTRY_STR  "rn"
#define BINLOG_OP_UPDATE_DENTRY_STR  "up"
#define BINLOG_OP_SET_XATTR_STR      "sx"
#define BINLOG_OP_REMOVE_XATTR_STR   "rx"
#define BINLOG_OP_DUMP_DENTRY_STR    "dm"
#define BINLOG_OP_NO_OP_STR          "no"

#define BINLOG_OP_CREATE_DENTRY_LEN  (sizeof(BINLOG_OP_CREATE_DENTRY_STR) - 1)
#define BINLOG_OP_REMOVE_DENTRY_LEN  (sizeof(BINLOG_OP_REMOVE_DENTRY_STR) - 1)
#define BINLOG_OP_RENAME_DENTRY_LEN  (sizeof(BINLOG_OP_RENAME_DENTRY_STR) - 1)
#define BINLOG_OP_UPDATE_DENTRY_LEN  (sizeof(BINLOG_OP_UPDATE_DENTRY_STR) - 1)
#define BINLOG_OP_SET_XATTR_LEN      (sizeof(BINLOG_OP_SET_XATTR_STR) - 1)
#define BINLOG_OP_REMOVE_XATTR_LEN   (sizeof(BINLOG_OP_REMOVE_XATTR_STR) - 1)
#define BINLOG_OP_DUMP_DENTRY_LEN    (sizeof(BINLOG_OP_DUMP_DENTRY_STR) - 1)
#define BINLOG_OP_NO_OP_LEN          (sizeof(BINLOG_OP_NO_OP_STR) - 1)

#define BINLOG_OPTIONS_PATH_ENABLED  (1 | (1 << 1))

struct server_binlog_record_buffer;

typedef void (*data_thread_notify_func)(struct fdir_binlog_record *record,
        const int result, const bool is_error);

typedef void (*release_binlog_rbuffer_func)(
        struct server_binlog_record_buffer *rbuffer,
        const int reffer_count);

typedef struct {
    FDIRDEntryFullName fullname;
    FDIRDEntryPName pname;
    FDIRServerDentry *parent;
    FDIRServerDentry *dentry;
} FDIRRecordDEntry;

typedef enum {
    fdir_dentry_type_fullname = 'f',
    fdir_dentry_type_pname = 'p',
    fdir_dentry_type_inode = 'i'
} FDIRDEntryType;

typedef enum {
    fdir_record_type_update = 'u',
    fdir_record_type_query = 'q'
} FDIRRecordType;

typedef enum {
    fdir_record_source_binlog_replay = 0,
    fdir_record_source_slave_replay = 0,
    fdir_record_source_master_rpc = 1,
    fdir_record_source_binlog_dump = 2
} FDIRRecordSource;

typedef struct {
    FDIRServerDentry *dentry;
    DABinlogOpType op_type;
    bool remove_from_parent;
} FDIRAffectedDentry;

typedef struct fdir_binlog_record {
    uint64_t data_version;
    int64_t inode;
    string_t ns;   //namespace
    unsigned int hash_code;
    uint8_t operation;
    FDIRRecordType record_type;
    FDIRDEntryType dentry_type;
    int timestamp;
    int flags;
    FDIRStatModifyFlags options;
    FDIRDentryOperator oper;

    union {
        struct {
            FDIRRecordDEntry dest;  //must be the first
            FDIRRecordDEntry src;
            FDIRServerDentry *overwritten;
        } rename;

        struct {
            FDIRRecordDEntry dest;  //must be the first
            struct {
                int64_t inode;
                FDIRDEntryFullName fullname;
                FDIRServerDentry *dentry;
            } src;
        } hdlink;

        FDIRRecordDEntry me;  //for create and remove

        struct fdir_record_ptr_array *parray; //for batch set dsize
        FDIRFLockTaskInfo *flock;
    };

    /* affected dentries for rename and remove operation */
    struct {
        FDIRAffectedDentry entries[2];
        int count;
    } affected;

    union {
        FDIRDEntryStat stat;
        FDIRFlockParams flock_params;
        int mask;  //for access dentry
    };

    union {
        string_t link;
        BufferInfo fullname;   //for get fullname
        key_value_pair_t xattr;
    };

    SFKeyValueArray xattr_kvarray;  //for BINLOG_OP_DUMP_DENTRY_INT

    //must be the last to avoid being overwritten by memset
    struct {
        data_thread_notify_func func;
        void *args;    //for thread continue deal
    } notify;

    struct {
        short data_thread_index;
        short arr_index;
    } extra;   //for data loader

    FDIRRecordSource source;

    struct fdir_binlog_record *next; //for data thread queue
} FDIRBinlogRecord;

typedef struct fdir_record_ptr_array {
    FDIRBinlogRecord **records;
    int alloc;
    struct {
        int total;
        int success;
        int updated;
    } counts;
} FDIRRecordPtrArray;

typedef struct server_binlog_record_buffer {
    SFVersionRange data_version; //for binlog writer and idempotency (slave only)
    int64_t req_id;
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
            return "CREATE_DENTRY";
        case BINLOG_OP_REMOVE_DENTRY_INT:
            return "REMOVE_DENTRY";
        case BINLOG_OP_RENAME_DENTRY_INT:
            return "RENAME_DENTRY";
        case BINLOG_OP_UPDATE_DENTRY_INT:
            return "UPDATE_DENTRY";
        case BINLOG_OP_SET_XATTR_INT:
            return "SET_XATTR";
        case BINLOG_OP_REMOVE_XATTR_INT:
            return "REMOVE_XATTR";
        case BINLOG_OP_NO_OP_INT:
            return "NO_OP";
        case BINLOG_OP_DUMP_DENTRY_INT:
            return "DUMP_DENTRY";
        case SERVER_OP_DUMP_DATA_INT:
            return "DUMP_DATA";
        case SERVICE_OP_SET_DSIZE_INT:
            return "SET_DENTRY_SIZE";
        case SERVICE_OP_BATCH_SET_DSIZE_INT:
            return "BATCH_SET_DSIZE";
        case SERVICE_OP_SYS_LOCK_APPLY_INT:
            return "SYS_LOCK_APPLY";
        case SERVICE_OP_FLOCK_APPLY_INT:
            return "FLOCK_APPLY";
        case SERVICE_OP_SYS_LOCK_RELEASE_INT:
            return "SYS_LOCK_RELEASE";
        case SERVICE_OP_ACCESS_DENTRY_INT:
            return "ACCESS_DENTRY";
        case SERVICE_OP_STAT_DENTRY_INT:
            return "STAT_DENTRY";
        case SERVICE_OP_READ_LINK_INT:
            return "READ_LINK";
        case SERVICE_OP_LOOKUP_INODE_INT:
            return "LOOKUP_INODE";
        case SERVICE_OP_LIST_DENTRY_INT:
            return "LIST_DENTRY";
        case SERVICE_OP_GET_XATTR_INT:
            return "GET_XATTR";
        case SERVICE_OP_LIST_XATTR_INT:
            return "LIST_XATTR";
        case SERVICE_OP_GET_FULLNAME_INT:
            return "GET_FULLNAME";
        default:
            return "UNKOWN";
    }
}

#ifdef __cplusplus
}
#endif

#endif

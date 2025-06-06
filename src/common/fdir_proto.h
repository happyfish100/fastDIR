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

#ifndef _FDIR_PROTO_H
#define _FDIR_PROTO_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcfs/auth/auth_types.h"
#include "sf/sf_proto.h"
#include "fdir_types.h"

//service commands
#define FDIR_SERVICE_PROTO_CLIENT_JOIN_REQ             9
#define FDIR_SERVICE_PROTO_CLIENT_JOIN_RESP           10

#define FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ          11
#define FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP         12
#define FDIR_SERVICE_PROTO_CREATE_BY_PNAME_REQ        13 //by parent inode and name
#define FDIR_SERVICE_PROTO_CREATE_BY_PNAME_RESP       14
#define FDIR_SERVICE_PROTO_SYMLINK_DENTRY_REQ         15
#define FDIR_SERVICE_PROTO_SYMLINK_DENTRY_RESP        16
#define FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_REQ       17
#define FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_RESP      18
#define FDIR_SERVICE_PROTO_HDLINK_DENTRY_REQ          19
#define FDIR_SERVICE_PROTO_HDLINK_DENTRY_RESP         20
#define FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_REQ        21
#define FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_RESP       22
#define FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ          23
#define FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP         24
#define FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_REQ        25
#define FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_RESP       26
#define FDIR_SERVICE_PROTO_RENAME_DENTRY_REQ          27
#define FDIR_SERVICE_PROTO_RENAME_DENTRY_RESP         28
#define FDIR_SERVICE_PROTO_RENAME_BY_PNAME_REQ        29
#define FDIR_SERVICE_PROTO_RENAME_BY_PNAME_RESP       30
#define FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_REQ      31
#define FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_RESP     32
#define FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_REQ     33
#define FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_RESP    34
#define FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_REQ   35
#define FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_RESP  36
#define FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_REQ  37
#define FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_RESP 38

#define FDIR_SERVICE_PROTO_LIST_DENTRY_BY_PATH_REQ    39
#define FDIR_SERVICE_PROTO_LIST_DENTRY_BY_INODE_REQ   40
#define FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ       41
#define FDIR_SERVICE_PROTO_LIST_DENTRY_RESP           42
#define FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_REQ   43
#define FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_RESP  44
#define FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_REQ  45
#define FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_RESP 46

#define FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ         47
#define FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP        48
#define FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ        49
#define FDIR_SERVICE_PROTO_STAT_BY_INODE_RESP       50
#define FDIR_SERVICE_PROTO_STAT_BY_PNAME_REQ        51 //by parent inode and name
#define FDIR_SERVICE_PROTO_STAT_BY_PNAME_RESP       52
#define FDIR_SERVICE_PROTO_READLINK_BY_PATH_REQ     53
#define FDIR_SERVICE_PROTO_READLINK_BY_PATH_RESP    54
#define FDIR_SERVICE_PROTO_READLINK_BY_PNAME_REQ    55
#define FDIR_SERVICE_PROTO_READLINK_BY_PNAME_RESP   56
#define FDIR_SERVICE_PROTO_READLINK_BY_INODE_REQ    57
#define FDIR_SERVICE_PROTO_READLINK_BY_INODE_RESP   58

#define FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_REQ        59  //modified by inode
#define FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_RESP       60
#define FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_REQ  61  //modified by inode
#define FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_RESP 62
#define FDIR_SERVICE_PROTO_MODIFY_STAT_BY_INODE_REQ   63
#define FDIR_SERVICE_PROTO_MODIFY_STAT_BY_INODE_RESP  64
#define FDIR_SERVICE_PROTO_MODIFY_STAT_BY_PATH_REQ    65
#define FDIR_SERVICE_PROTO_MODIFY_STAT_BY_PATH_RESP   66

#define FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ           67  //file lock
#define FDIR_SERVICE_PROTO_FLOCK_DENTRY_RESP          68
#define FDIR_SERVICE_PROTO_GETLK_DENTRY_REQ           69
#define FDIR_SERVICE_PROTO_GETLK_DENTRY_RESP          70

/* system lock for apend and ftruncate */
#define FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_REQ      71
#define FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_RESP     72
#define FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_REQ    73
#define FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_RESP   74

#define FDIR_SERVICE_PROTO_SERVICE_STAT_REQ         75
#define FDIR_SERVICE_PROTO_SERVICE_STAT_RESP        76
#define FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ         77
#define FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP        78
#define FDIR_SERVICE_PROTO_NAMESPACE_STAT_REQ       79
#define FDIR_SERVICE_PROTO_NAMESPACE_STAT_RESP      80

#define FDIR_SERVICE_PROTO_GENERATE_NODE_ID_REQ     81  //for file lock
#define FDIR_SERVICE_PROTO_GENERATE_NODE_ID_RESP    82
#define FDIR_SERVICE_PROTO_GET_MASTER_REQ           83
#define FDIR_SERVICE_PROTO_GET_MASTER_RESP          84
#define FDIR_SERVICE_PROTO_GET_SLAVES_REQ           85
#define FDIR_SERVICE_PROTO_GET_SLAVES_RESP          86
#define FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ  87
#define FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP 88

#define FDIR_SERVICE_PROTO_ACCESS_BY_PATH_REQ       89
#define FDIR_SERVICE_PROTO_ACCESS_BY_PATH_RESP      90
#define FDIR_SERVICE_PROTO_ACCESS_BY_PNAME_REQ      91
#define FDIR_SERVICE_PROTO_ACCESS_BY_PNAME_RESP     92
#define FDIR_SERVICE_PROTO_ACCESS_BY_INODE_REQ      93
#define FDIR_SERVICE_PROTO_ACCESS_BY_INODE_RESP     94
#define FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_REQ    95
#define FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_RESP   96
#define FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_REQ   97
#define FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_RESP  98
#define FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_REQ   99
#define FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_RESP  100
#define FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_REQ  101
#define FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_RESP 102
#define FDIR_SERVICE_PROTO_GET_FULLNAME_BY_PNAME_REQ  103
#define FDIR_SERVICE_PROTO_GET_FULLNAME_BY_PNAME_RESP 104
#define FDIR_SERVICE_PROTO_GET_FULLNAME_BY_INODE_REQ  105
#define FDIR_SERVICE_PROTO_GET_FULLNAME_BY_INODE_RESP 106

#define FDIR_SERVICE_PROTO_NAMESPACE_LIST_REQ       141
#define FDIR_SERVICE_PROTO_NAMESPACE_LIST_RESP      142

//for namespace stat sync
#define FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_REQ        151
#define FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_RESP       152
#define FDIR_SERVICE_PROTO_NSS_FETCH_REQ            153
#define FDIR_SERVICE_PROTO_NSS_FETCH_RESP           154

//cluster commands
#define FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ    201
#define FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP   202
#define FDIR_CLUSTER_PROTO_JOIN_MASTER_REQ          203  //slave  -> master
#define FDIR_CLUSTER_PROTO_JOIN_MASTER_RESP         204
#define FDIR_CLUSTER_PROTO_PING_MASTER_REQ          205
#define FDIR_CLUSTER_PROTO_PING_MASTER_RESP         206
#define FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER      207  //notify next leader to other servers
#define FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER       208  //commit next leader to other servers

//replication commands, master -> slave
#define FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ           211
#define FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP          212
#define FDIR_REPLICA_PROTO_RPC_CALL_REQ             213
#define FDIR_REPLICA_PROTO_RPC_CALL_RESP            214
#define FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ          215
#define FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP         216
#define FDIR_REPLICA_PROTO_NOTIFY_SLAVE_QUIT        217  //when slave binlog not consistent

//replication commands, slave -> master
#define FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ    221
#define FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP   222
#define FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ    223
#define FDIR_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ     225
#define FDIR_REPLICA_PROTO_SYNC_BINLOG_RESP         226
#define FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_REQ       227
#define FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_RESP      228
#define FDIR_REPLICA_PROTO_SYNC_BINLOG_REPORT       229

#define FDIR_PROTO_FILE_TYPE_DUMP      'd'
#define FDIR_PROTO_FILE_TYPE_BINLOG    'b'

#define FDIR_PROTO_SYNC_BINLOG_STAGE_START 's'
#define FDIR_PROTO_SYNC_BINLOG_STAGE_END   'e'

typedef SFCommonProtoHeader  FDIRProtoHeader;

typedef struct fdir_proto_client_join_req {
    char flags[4];
    struct {
        char channel_id[4];
        char key[4];
    } idempotency;
    char auth_enabled;
    char trash_bin_enabled;
    char padding[2];
    char config_sign[SF_CLUSTER_CONFIG_SIGN_LEN];
} FDIRProtoClientJoinReq;

typedef struct fdir_proto_client_join_resp {
    char buffer_size[4];
    char padding[4];
} FDIRProtoClientJoinResp;

typedef struct fdir_proto_dentry_info {
    unsigned char ns_len;  //namespace length
    char path_len[2];
    char ns_str[0];      //namespace string
    //char *path_str;    //path_str = ns_str + ns_len
} FDIRProtoDEntryInfo;

typedef struct fdir_proto_dentry_by_pname {
    char parent_inode[8];
    unsigned char ns_len;   //namespace length
    unsigned char name_len; //dir name length
    char ns_str[0];         //namespace for hash code
    //char *name_str;       //name_str = ns_str + ns_len
} FDIRProtoDEntryByPName;

typedef struct fdir_proto_inode_info {
    char inode[8];
    unsigned char ns_len; //namespace length
    char ns_str[0];       //namespace for hash code
} FDIRProtoInodeInfo;

typedef struct fdir_proto_name_info {
    unsigned char len;
    char str[0];
} FDIRProtoNameInfo;

typedef struct fdir_proto_operator {
    char uid[4];
    char gid[4];
    struct {
        unsigned char count;
        char padding[3];
        char list[0];
    } additional_gids;
} FDIRProtoOperator;

typedef struct fdir_proto_create_dentry_front {
    union {
        char rdev[8]; /* for create dentry, device ID for special file */
        struct {
            char flags[4];  /* for hdlink */
            char padding1[4];
        };
    };
    char mode[4];
    char padding2[4];
    FDIRProtoOperator oper;
} FDIRProtoCreateDEntryFront;

typedef struct fdir_proto_create_dentry_req {
    FDIRProtoCreateDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoCreateDEntryReq;

typedef struct fdir_proto_create_dentry_by_pname_req {
    FDIRProtoCreateDEntryFront front;
    FDIRProtoDEntryByPName pname;
} FDIRProtoCreateDEntryByPNameReq;

typedef struct fdir_proto_symlink_dentry_front {
    char link_len[2];
    char padding[2];
    FDIRProtoCreateDEntryFront common;
    //char link_str[0];
} FDIRProtoSymlinkDEntryFront;

typedef struct fdir_proto_symlink_dentry_by_name_req {
    FDIRProtoSymlinkDEntryFront front;
    FDIRProtoDEntryByPName pname;
} FDIRProtoSymlinkDEntryByNameReq;

typedef struct fdir_proto_symlink_dentry_req {
    FDIRProtoSymlinkDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoSymlinkDEntryReq;

typedef struct fdir_proto_hdlink_dentry {
    FDIRProtoCreateDEntryFront front;
    FDIRProtoDEntryInfo src;
    FDIRProtoDEntryInfo dest;
} FDIRProtoHDLinkDEntry;

typedef struct fdir_proto_hdlink_by_pname_front {
    char src_inode[8];
    FDIRProtoCreateDEntryFront common;
} FDIRProtoHDlinkByPNameFront;

typedef struct fdir_proto_hdlink_dentry_by_pname {
    FDIRProtoHDlinkByPNameFront front;
    FDIRProtoDEntryByPName dest;
} FDIRProtoHDLinkDEntryByPName;

typedef struct fdir_proto_remove_dentry_front {
    char flags[4];
    char padding[4];
    FDIRProtoOperator oper;
} FDIRProtoRemoveDEntryFront;

typedef struct fdir_proto_remove_dentry {
    FDIRProtoRemoveDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoRemoveDEntry;

typedef struct fdir_proto_remove_dentry_by_pname {
    FDIRProtoRemoveDEntryFront front;
    FDIRProtoDEntryByPName pname;
} FDIRProtoRemoveDEntryByPName;

typedef struct fdir_proto_rename_dentry_front {
    char flags[4];
    char padding[4];
    FDIRProtoOperator oper;
} FDIRProtoRenameDEntryFront;

typedef struct fdir_proto_rename_dentry {
    FDIRProtoRenameDEntryFront front;
    FDIRProtoDEntryInfo src;
    FDIRProtoDEntryInfo dest;
} FDIRProtoRenameDEntry;

typedef struct fdir_proto_rename_dentry_by_pname {
    FDIRProtoRenameDEntryFront front;
    FDIRProtoDEntryByPName src;
    FDIRProtoDEntryByPName dest;
} FDIRProtoRenameDEntryByPName;

typedef struct fdir_proto_set_dentry_size_req {
    char inode[8];
    char file_size[8];   /* file size in bytes */
    char inc_alloc[8];   /* increase alloc size in bytes */
    char flags[4];
    unsigned char ns_len; //namespace length
    char ns_str[0];       //namespace for hash code
} FDIRProtoSetDentrySizeReq;

typedef struct fdir_proto_batch_set_dentry_size_req_header {
    char count[4];        //dentry count
    unsigned char ns_len; //namespace length
    char ns_str[0];       //namespace for hash code
} FDIRProtoBatchSetDentrySizeReqHeader;

typedef struct fdir_proto_batch_set_dentry_size_req_body {
    char inode[8];
    char file_size[8];   /* file size in bytes */
    char inc_alloc[8];   /* increase alloc size in bytes */
    char flags[4];
} FDIRProtoBatchSetDentrySizeReqBody;

typedef struct fdir_proto_dentry_stat {
    char mode[4];
    char uid[4];
    char gid[4];
    char btime[4];  /* create time */
    char atime[4];  /* access time */
    char ctime[4];  /* status change time */
    char mtime[4];  /* modify time */
    char nlink[4];  /* ref count for hard link */
    char rdev[8];   /* device ID */
    char size[8];   /* file size in bytes */
    char alloc[8];  /* alloc space in bytes */
    char space_end[8];  /* space end offset */
} FDIRProtoDEntryStat;

typedef struct fdir_proto_modify_stat_front {
    char mflags[8];  //modify flags
    char flags[4];   //follow syslink flags
    FDIRProtoDEntryStat stat;
    FDIRProtoOperator oper;
} FDIRProtoModifyStatFront;

typedef struct fdir_proto_modify_stat_by_inode_req {
    FDIRProtoModifyStatFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoModifyStatByInodeReq;

typedef struct fdir_proto_modify_stat_by_path_req {
    FDIRProtoModifyStatFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoModifyStatByPathReq;

typedef struct fdir_proto_query_dentry_req {
    FDIRProtoOperator oper;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoQueryDentryReq;

typedef struct fdir_proto_query_dentry_by_pname_req {
    FDIRProtoOperator oper;
    FDIRProtoDEntryByPName pname;
} FDIRProtoQueryDentryByPNameReq;

typedef struct fdir_proto_query_dentry_by_inode_req {
    FDIRProtoOperator oper;
    FDIRProtoInodeInfo ino;
} FDIRProtoQueryDentryByInodeReq;


typedef struct fdir_proto_lookup_inode_resp {
    char inode[8];
} FDIRProtoLookupInodeResp;


typedef struct fdir_proto_access_dentry_front {
    char flags[4];
    char mask;  //F_OK, R_OK, W_OK, X_OK
    char padding[3];
    FDIRProtoOperator oper;
} FDIRProtoAccessDEntryFront;

typedef struct fdir_proto_access_dentry_req {
    FDIRProtoAccessDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoAccessDEntryReq;

typedef struct fdir_proto_access_dentry_by_inode_req {
    FDIRProtoAccessDEntryFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoAccessDEntryByInodeReq;

typedef struct fdir_proto_access_dentry_by_pname_req {
    FDIRProtoAccessDEntryFront front;
    FDIRProtoDEntryByPName pname;
} FDIRProtoAccessDEntryByPNameReq;

typedef struct fdir_proto_get_fullname_front {
    char flags[4];
    char padding[4];
    FDIRProtoOperator oper;
} FDIRProtoGetFullnameFront;

typedef struct fdir_proto_get_fullname_by_inode_req {
    FDIRProtoGetFullnameFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoGetFullnameByInodeReq;

typedef struct fdir_proto_get_fullname_by_pname_req {
    FDIRProtoGetFullnameFront front;
    FDIRProtoDEntryByPName pname;
} FDIRProtoGetFullnameByPNameReq;

typedef struct fdir_proto_stat_dentry_front {
    char flags[4];
    char padding[4];
    FDIRProtoOperator oper;
} FDIRProtoStatDEntryFront;

typedef struct fdir_proto_stat_dentry_req {
    FDIRProtoStatDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoStatDEntryReq;

typedef struct fdir_proto_stat_dentry_by_inode_req {
    FDIRProtoStatDEntryFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoStatDEntryByInodeReq;

typedef struct fdir_proto_stat_dentry_by_pname_req {
    FDIRProtoStatDEntryFront front;
    FDIRProtoDEntryByPName pname;
} FDIRProtoStatDEntryByPNameReq;

typedef struct fdir_proto_stat_dentry_resp {
    char inode[8];
    FDIRProtoDEntryStat stat;
} FDIRProtoStatDEntryResp;

typedef struct fdir_proto_flock_dentry_front {
    char offset[8];  /* lock region offset */
    char length[8];  /* lock region length, 0 for until end of file */
    struct {
        char node[4]; // client node id
        char pid[4];
        char id[8];   //owner id
    } owner;
    char operation[4]; /* lock operation, LOCK_SH for read shared lock,
                         LOCK_EX for write exclusive lock,
                         LOCK_NB for non-block with LOCK_SH or LOCK_EX,
                         LOCK_UN for unlock */
    FDIRProtoOperator oper;
} FDIRProtoFlockDEntryFront;

typedef struct fdir_proto_flock_dentry_req {
    FDIRProtoFlockDEntryFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoFlockDEntryReq;

typedef struct fdir_proto_getlk_dentry_front {
    char offset[8];  /* lock region offset */
    char length[8];  /* lock region length, 0 for until end of file */
    struct {
        char node[4]; //client node id
        char pid[4];
        char id[8];   //owner id
    } owner;
    char operation[4];
    FDIRProtoOperator oper;
} FDIRProtoGetlkDEntryFront;

typedef struct fdir_proto_getlk_dentry_req {
    FDIRProtoGetlkDEntryFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoGetlkDEntryReq;

typedef struct fdir_proto_getlk_dentry_resp {
    char offset[8];  /* lock region offset */
    char length[8];  /* lock region length, 0 for until end of file */
    struct {
        char node[4]; //client node id
        char pid[4];
        char id[8];   //owner id
    } owner;
    char type[4];
    char padding[4];
} FDIRProtoGetlkDEntryResp;

typedef struct fdir_proto_sys_lock_dentry_req {
    char flags[4];      //LOCK_NB for non-block
    char padding[4];
    FDIRProtoInodeInfo ino;
} FDIRProtoSysLockDEntryReq;

typedef struct fdir_proto_sys_lock_dentry_resp {
    char size[8];       //file size
    char space_end[8];  //file data end offset
} FDIRProtoSysLockDEntryResp;

typedef struct fdir_proto_sys_unlock_dentry_req {
    char inode[8];
    char old_size[8];  //old file size for check
    char new_size[8];  //new file size to set
    char inc_alloc[8]; // increase alloc size in bytes
    char flags[4];     //if set file size or inc alloc space
    char force;        //force set file size
    unsigned char ns_len; //namespace length
    char ns_str[0];       //namespace for hash code
} FDIRProtoSysUnlockDEntryReq;

typedef struct fdir_proto_list_dentry_front {
    char flags[4];
    char padding[4];
    FDIRProtoOperator oper;
} FDIRProtoListDEntryFront;

typedef struct fdir_proto_list_dentry_by_path_req {
    FDIRProtoListDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoListDEntryByPathReq;

typedef struct fdir_proto_list_dentry_by_inode_req {
    FDIRProtoListDEntryFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoListDEntryByInodeReq;

typedef struct fdir_proto_list_dentry_next_body {
    char token[8];
    char offset[4];    //for check, must be same with server's
    char padding[4];
} FDIRProtoListDEntryNextBody;

typedef struct fdir_proto_list_dentry_resp_body_common_header {
    char token[8];
    char count[4];  //current dentry count
    char is_last;
    char padding[3];
} FDIRProtoListDEntryRespBodyCommonHeader;

typedef struct fdir_proto_list_dentry_resp_body_first_header {
    FDIRProtoListDEntryRespBodyCommonHeader common;
    char total_count[4];
} FDIRProtoListDEntryRespBodyFirstHeader;

typedef FDIRProtoListDEntryRespBodyCommonHeader
    FDIRProtoListDEntryRespBodyNextHeader;

typedef struct fdir_proto_list_dentry_resp_common_part {
    char inode[8];
    unsigned char name_len;
    char name_str[0];
} FDIRProtoListDEntryRespCommonPart;

typedef struct fdir_proto_list_dentry_resp_complete_part {
    FDIRProtoDEntryStat stat;
    FDIRProtoListDEntryRespCommonPart common;
} FDIRProtoListDEntryRespCompletePart;

typedef struct fdir_proto_list_dentry_resp_compact_part {
    char mode[4];
    FDIRProtoListDEntryRespCommonPart common;
} FDIRProtoListDEntryRespCompactPart;

typedef struct fdir_proto_set_xattr_fields {
    unsigned char name_len;
    char value_len[2];
    char padding[1];
    char flags[4];
    FDIRProtoOperator oper;
    //char name_str[0];
    //char value_str[0];
} FDIRProtoSetXAttrFields;

typedef struct fdir_proto_set_xattr_by_path_req {
    FDIRProtoSetXAttrFields fields;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoSetXAttrByPathReq;

typedef struct fdir_proto_set_xattr_by_inode_req {
    FDIRProtoSetXAttrFields fields;
    FDIRProtoInodeInfo ino;
} FDIRProtoSetXAttrByInodeReq;

typedef struct fdir_proto_xattr_front {
    char flags[4];
    char padding[4];
    FDIRProtoOperator oper;
} FDIRProtoXAttrFront;

typedef struct fdir_proto_get_xattr_by_path_req {
    FDIRProtoXAttrFront front;
    FDIRProtoNameInfo name;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoGetXAttrByPathReq;

typedef struct fdir_proto_get_xattr_by_node_req {
    FDIRProtoXAttrFront front;
    FDIRProtoNameInfo name;
    FDIRProtoInodeInfo ino;
} FDIRProtoGetXAttrByInodeReq;

typedef struct fdir_proto_list_xattr_by_inode_req {
    FDIRProtoXAttrFront front;
    FDIRProtoInodeInfo ino;
} FDIRProtoListXAttrByInodeReq;

typedef struct fdir_proto_list_xattr_by_path_req {
    FDIRProtoXAttrFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoListXAttrByPathReq;

typedef FDIRProtoGetXAttrByPathReq  FDIRProtoRemoveXAttrByPathReq;
typedef FDIRProtoGetXAttrByInodeReq FDIRProtoRemoveXAttrByInodeReq;

typedef struct fdir_proto_namespace_list_resp_header {
    char count[4];
    char server_id[4];
} FDIRProtoNamespaceListRespHeader;

typedef struct fdir_proto_namespace_list_resp_body {
    char dir_count[8];
    char file_count[8];
    char used_bytes[8];
    unsigned char name_len;
    char name_str[0];
} FDIRProtoNamespaceListRespBody;

typedef struct fdir_proto_service_stat_resp {
    char up_time[4];
    char server_id[4];
    char is_master;
    char status;
    char auth_enabled;
    struct {
        char enabled;
        char current_version[8];
        char version_delay[8];
        struct {
            char disk_avail[8];
            char inode_used_space[8];
            struct {
                char total[8];
                char used[8];
                char avail[8];
            } trunk;
        } space;

        struct {
            char total_count[8];
            char success_count[8];
            char reclaimed_count[8];
        } reclaim;

    } storage_engine;

    char padding[5];
    struct {
        char len;
        char str[10];
    } version;

    struct {
        char current_count[4];
        char max_count[4];
    } connection;

    struct {
        char current_version[8];
        char confirmed_version[8];
    } data;

    struct {
        char current_version[8];
        struct {
            char total_count[8];
            char next_version[8];
            char waiting_count[4];
            char max_waitings[4];
        } writer;
    } binlog;

    struct {
        char current_inode_sn[8];
        struct {
            char ns[8];
            char dir[8];
            char file[8];
        } counters;
    } dentry;
} FDIRProtoServiceStatResp;

typedef struct fdir_proto_cluster_stat_req {
    char filter_by;
    char op_type;  //including = and !
    char status;
    char is_master;
} FDIRProtoClusterStatReq;

typedef struct fdir_proto_cluster_stat_resp_body_header {
    char count[4];
    char padding[4];
} FDIRProtoClusterStatRespBodyHeader;

typedef struct fdir_proto_cluster_stat_resp_body_part {
    char server_id[4];
    char is_master;
    char status;
    char port[2];
    char confirmed_data_version[8];
    char ip_addr[IP_ADDRESS_SIZE];
} FDIRProtoClusterStatRespBodyPart;

typedef struct fdir_proto_namespace_stat_req {
    unsigned char ns_len; //namespace length
    char ns_str[0];       //namespace string
} FDIRProtoNamespaceStatReq;

typedef struct fdir_proto_namespace_stat_resp {
    struct {
        char total[8];
        char used[8];
        char avail[8];
    } inode_counters;

    char used_bytes[8];
} FDIRProtoNamespaceStatResp;

typedef struct fdir_proto_generate_node_id_req {
    char key[8];
    char node_id[4];
    char padding[4];
} FDIRProtoGenerateNodeIdReq;

typedef FDIRProtoGenerateNodeIdReq FDIRProtoGenerateNodeIdResp;

/* for FDIR_SERVICE_PROTO_GET_MASTER_RESP and
   FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP
   */
typedef struct fdir_proto_get_server_resp {
    char server_id[4];
    char port[2];
    char padding[2];
    char ip_addr[IP_ADDRESS_SIZE];
} FDIRProtoGetServerResp;

typedef struct fdir_proto_get_slaves_resp_body_header {
    char count[2];
    char padding[6];
} FDIRProtoGetSlavesRespBodyHeader;

typedef struct fdir_proto_get_slaves_resp_body_part {
    char server_id[4];
    char port[2];
    char status;
    char padding[1];
    char ip_addr[IP_ADDRESS_SIZE];
} FDIRProtoGetSlavesRespBodyPart;

typedef struct fdir_proto_get_server_status_req {
    char server_id[4];
    char auth_enabled;
    char padding[3];
    char config_sign[SF_CLUSTER_CONFIG_SIGN_LEN];
} FDIRProtoGetServerStatusReq;

typedef struct fdir_proto_get_server_status_resp {
    char is_master;
    char master_hint;
    char status;
    char force_election;
    char server_id[4];
    char up_time[4];
    char last_heartbeat_time[4];
    char last_shutdown_time[4];
    char data_version[8];
} FDIRProtoGetServerStatusResp;

typedef struct fdir_proto_cluster_server_identity {
    char cluster_id[4];    //the cluster id
    char server_id[4];     //the slave server id
    char auth_enabled;
    char padding[7];
    char config_sign[SF_CLUSTER_CONFIG_SIGN_LEN];
} FDIRProtoClusterServerIdentity;

typedef struct fdir_proto_join_master_req {
    FDIRProtoClusterServerIdentity si;
    char key[FDIR_REPLICA_KEY_SIZE];   //the slave key used on JOIN_SLAVE
} FDIRProtoJoinMasterReq;

typedef struct fdir_proto_join_slave_req {
    char cluster_id[4];  //the cluster id
    char server_id[4];   //the master server id
    char buffer_size[4]; //the master task size
    char key[FDIR_REPLICA_KEY_SIZE];  //the slave key passed / set by JOIN_MASTER
} FDIRProtoJoinSlaveReq;

typedef struct fdir_proto_join_slave_resp {
    //last N rows for consistency check
    char binlog_count[4];
    char binlog_length[4];

    struct {
        char index[4];   //binlog file index
        char offset[8];  //binlog file offset
    } binlog_pos_hint;
    char last_data_version[8];   //the slave's last data version
    char binlog[0];
} FDIRProtoJoinSlaveResp;

typedef struct fdir_proto_notify_slave_quit {
    char server_id[4];      //the master server id
    char error_code[4];
    char binlog_count[4];
    char padding[4];
    char first_unmatched_dv[8];   //the slave's first unmatched data version
} FDIRProtoNotifySlaveQuit;

typedef struct fdir_proto_ping_master_req {
    char confirmed_data_version[8];
} FDIRProtoPingMasterReq;

typedef struct fdir_proto_ping_master_resp_header {
    char inode_sn[8];  //current inode sn of master
    char server_count[4];
    char padding[4];
} FDIRProtoPingMasterRespHeader;

typedef struct fdir_proto_ping_master_resp_body_part {
    char server_id[4];
    char status;
    char padding[3];
    char data_version[8];
} FDIRProtoPingMasterRespBodyPart;

typedef struct fdir_proto_forward_requests_body_header {
    char binlog_length[4];
    char count[4];
    struct {
        char first[8];
        char last[8];
    } data_version;
} FDIRProtoForwardRequestsBodyHeader;

typedef struct fdir_proto_forward_request_metadata {
    char req_id[8];
    char data_version[8];
} FDIRProtoForwardRequestMetadata;

typedef struct fdir_proto_push_binlog_req_body_header {
    char binlog_length[4];
    char padding[4];
    struct {
        char first[8];
        char last[8];
    } data_version;
} FDIRProtoPushBinlogReqBodyHeader;

typedef struct fdir_proto_push_binlog_resp {
    char record_count[4];
    char padding[4];
} FDIRProtoPushBinlogResp;

typedef FDIRProtoPushBinlogResp FDIRProtoForwardRequestsResp;

typedef struct fdir_proto_replia_query_binlog_info_req {
    char server_id[4];
    char padding[4];
} FDIRProtoReplicaQueryBinlogInfoReq;

typedef struct fdir_proto_replia_query_binlog_info_resp {
    struct {
        char start_index[4];
        char last_index[4];
    } dump_data;
    struct {
        char start_index[4];
        char last_index[4];
    } binlog;
} FDIRProtoReplicaQueryBinlogInfoResp;

typedef struct fdir_proto_replia_sync_binlog_report_req {
    FDIRProtoClusterServerIdentity si;
    char stage;
    char padding[7];
} FDIRProtoReplicaSyncBinlogReportReq;

typedef struct fdir_proto_replia_sync_binlog_first_req {
    char file_type;  //dump data or normal binlog
    char padding[7];
    char server_id[4];
    char binlog_index[4];
} FDIRProtoReplicaSyncBinlogFirstReq;

typedef struct fdir_proto_replia_sync_dump_mark_req {
    char server_id[4];
    char padding[4];
} FDIRProtoReplicaSyncDumpMarkReq;

typedef struct fdir_proto_nss_fetch_resp_body_header {
    char count[4];
    char is_last;
    char padding[3];
} FDIRProtoNSSFetchRespBodyHeader;

typedef struct fdir_proto_nss_fetch_resp_body_part {
    char used_bytes[8];
    FDIRProtoNameInfo ns_name;
} FDIRProtoNSSFetchRespBodyPart;

static inline void fdir_log_network_error_ex1(SFResponseInfo *response,
        const ConnectionInfo *conn, const int result, const int log_level,
        const char *file, const int line)
{
    if (result == EPERM || result == EACCES) {
        if (log_level < LOG_WARNING) {
            sf_log_network_error_ex1(response, conn, "fdir",
                    result, LOG_WARNING, file, line);
            return;
        }
    }

    sf_log_network_error_ex1(response, conn, "fdir",
            result, log_level, file, line);
}

#define fdir_log_network_error_ex(response, conn, result, log_level) \
    fdir_log_network_error_ex1(response, conn, result, log_level, \
            __FILE__, __LINE__)

#define fdir_log_network_error(response, conn, result) \
    fdir_log_network_error_ex(response, conn, result, LOG_ERR)

static inline void fdir_log_network_error_for_update_ex(
        SFResponseInfo *response, const ConnectionInfo *conn,
        const int result, const int enoent_log_level,
        const char *file, const int line)
{
    if (result == EPERM || result == EACCES) {
        fdir_log_network_error_ex1(response, conn,
                result, LOG_ERR, file, line);
        return;
    }

    sf_log_network_error_for_update_ex(response, conn,
            "fdir", result, enoent_log_level, file, line);
}

#define fdir_log_network_error_for_update(response, \
        conn, result, enoent_log_level) \
    fdir_log_network_error_for_update_ex(response, conn, \
            result, enoent_log_level, __FILE__, __LINE__)


static inline void fdir_log_network_error_for_delete_ex(
        SFResponseInfo *response, const ConnectionInfo *conn,
        const int result, const int enoent_log_level,
        const char *file, const int line)
{
    if (result == EPERM || result == EACCES) {
        fdir_log_network_error_ex1(response, conn,
                result, LOG_ERR, file, line);
        return;
    }

    sf_log_network_error_for_delete_ex(response, conn, "fdir",
            result, enoent_log_level, file, line);
}

#define fdir_log_network_error_for_delete(response, \
        conn, result, enoent_log_level) \
    fdir_log_network_error_for_delete_ex(response, conn, \
            result, enoent_log_level, __FILE__, __LINE__)

#ifdef __cplusplus
extern "C" {
#endif

void fdir_proto_init();

static inline void fdir_proto_pack_dentry_stat_ex(const FDIRDEntryStat *stat,
        FDIRProtoDEntryStat *proto, const bool server_side)
{
    if (server_side) {
        int2buff(FDIR_UNSET_DENTRY_HARD_LINK(stat->mode), proto->mode);
    } else {
        int2buff(stat->mode, proto->mode);
    }
    int2buff(stat->uid, proto->uid);
    int2buff(stat->gid, proto->gid);
    int2buff(stat->atime, proto->atime);
    int2buff(stat->btime, proto->btime);
    int2buff(stat->ctime, proto->ctime);
    int2buff(stat->mtime, proto->mtime);
    int2buff(stat->nlink, proto->nlink);
    long2buff(stat->rdev, proto->rdev);
    long2buff(stat->size, proto->size);
    long2buff(stat->alloc, proto->alloc);
    long2buff(stat->space_end, proto->space_end);
}

#define fdir_proto_pack_dentry_stat(stat, proto) \
    fdir_proto_pack_dentry_stat_ex(stat, proto, false)

static inline void fdir_proto_unpack_dentry_stat(
        const FDIRProtoDEntryStat *proto,
        FDIRDEntryStat *stat)
{
    stat->mode = buff2int(proto->mode);
    stat->uid = buff2int(proto->uid);
    stat->gid = buff2int(proto->gid);
    stat->atime = buff2int(proto->atime);
    stat->btime = buff2int(proto->btime);
    stat->ctime = buff2int(proto->ctime);
    stat->mtime = buff2int(proto->mtime);
    stat->nlink = buff2int(proto->nlink);
    stat->rdev = buff2long(proto->rdev);
    stat->size = buff2long(proto->size);
    stat->alloc = buff2long(proto->alloc);
    stat->space_end = buff2long(proto->space_end);
}

int fdir_proto_get_master(ConnectionInfo *conn,
        const int network_timeout,
        FDIRClientServerEntry *master);

const char *fdir_get_server_status_caption(const int status);

const char *fdir_get_cmd_caption(const int cmd);

#ifdef __cplusplus
}
#endif

#endif

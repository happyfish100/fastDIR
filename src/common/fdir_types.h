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

#ifndef _FDIR_TYPES_H
#define _FDIR_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/server_id_func.h"
#include "sf/sf_types.h"

#define FDIR_REPLICA_KEY_SIZE    8

#define FDIR_DEFAULT_BINLOG_BUFFER_SIZE (256 * 1024)

#define FDIR_SERVER_DEFAULT_CLUSTER_PORT  11011
#define FDIR_SERVER_DEFAULT_SERVICE_PORT  11012

#define FDIR_MAX_PATH_COUNT             128
#define FDIR_BATCH_SET_MAX_DENTRY_COUNT 256

#define FDIR_XATTR_KVARRAY_ALLOCATOR_COUNT  6
#define FDIR_XATTR_KVARRAY_MAX_ELEMENTS (1 << FDIR_XATTR_KVARRAY_ALLOCATOR_COUNT)
#define FDIR_XATTR_MAX_VALUE_SIZE       256

#define FDIR_SERVER_STATUS_INIT       0
#define FDIR_SERVER_STATUS_BUILDING  10
#define FDIR_SERVER_STATUS_OFFLINE   21
#define FDIR_SERVER_STATUS_SYNCING   22
#define FDIR_SERVER_STATUS_ACTIVE    23

#define FDIR_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST  1

#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE   1  //file size
#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC   2  //increase alloc space
#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_SPACE_END   4  //space end offset for deallocate
#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME       8  //file modify time
#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_FORCE      16  //force update

#define FDIR_DENTRY_MODE_FLAGS_HARD_LINK    (1 << 22)  //hard link flags in 32 bits mode

#define FDIR_SET_DENTRY_HARD_LINK(mode) \
    ((mode) | FDIR_DENTRY_MODE_FLAGS_HARD_LINK)

#define FDIR_UNSET_DENTRY_HARD_LINK(mode) \
    ((mode) & (~FDIR_DENTRY_MODE_FLAGS_HARD_LINK))

#define FDIR_IS_DENTRY_HARD_LINK(mode)  \
    (((mode) & FDIR_DENTRY_MODE_FLAGS_HARD_LINK) != 0)

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE	(1 << 0)
#endif

#ifndef RENAME_EXCHANGE
#define RENAME_EXCHANGE     (1 << 1)
#endif

#define FDIR_FOLLOW_SYMLINK_MAX      10
#define FDIR_FLAGS_FOLLOW_SYMLINK    (1 << 0)
#define FDIR_FLAGS_XATTR_CREATE      (1 << 1)  //for setxattr
#define FDIR_FLAGS_XATTR_REPLACE     (1 << 2)  //for setxattr
#define FDIR_FLAGS_XATTR_GET_SIZE    (1 << 3)  //for getxattr and listxattr

#define FDIR_UNLINK_FLAGS_MATCH_ENABLED (1 << 8)
#define FDIR_UNLINK_FLAGS_MATCH_DIR  ((1 << 1) | FDIR_UNLINK_FLAGS_MATCH_ENABLED)
#define FDIR_UNLINK_FLAGS_MATCH_FILE ((1 << 2) | FDIR_UNLINK_FLAGS_MATCH_ENABLED)

#define FDIR_LIST_DENTRY_FLAGS_COMPACT_OUTPUT  (1 << 1) //for POSIX readdir

#define FDIR_IS_ROOT_PATH(path) \
    ((path).len == 1 && (path).str[0] == '/')

#define FDIR_SET_DENTRY_PNAME_PTR(pname, inode, nm_ptr) \
    do {  \
        (pname)->parent_inode = inode; \
        (pname)->name = *nm_ptr;       \
    } while (0)

typedef struct fdir_dentry_full_name {
    string_t ns;    //namespace
    string_t path;  //full path
} FDIRDEntryFullName;

typedef struct fdir_dentry_pname {
    int64_t parent_inode;
    string_t name;
} FDIRDEntryPName;

typedef struct fdir_namespace_stat {
    int64_t used_inodes;
    int64_t used_bytes;
} FDIRNamespaceStat;

typedef struct fdir_dentry_stat {
    int mode;
    uid_t uid;
    gid_t gid;
    int btime;  /* birth / create time */
    int atime;  /* access time */
    int ctime;  /* status change time */
    int mtime;  /* modify time */
    int nlink;  /* ref count for hard link and directory */
    dev_t rdev; /* device ID */
    int64_t size;   /* file size in bytes */
    int64_t alloc;  /* alloc space in bytes */
    int64_t space_end;   /* for remove disk space when deallocate */
} FDIRDEntryStat;

typedef struct fdir_dentry_info {
    int64_t inode;
    FDIRDEntryStat stat;
} FDIRDEntryInfo;

typedef union {
    int64_t flags;
    struct {
        union {
            int flags: 4;
            struct {
                bool ns: 1;      //namespace
                bool subname: 1;
            };
        } path_info;
        bool hash_code : 1;  //required field, for unpack check
        bool link : 1;       //for symlink
        bool mode : 1;
        bool atime: 1;
        bool btime: 1;
        bool ctime: 1;
        bool mtime: 1;
        bool gid:   1;
        bool uid:   1;
        bool rdev:  1;
        bool size:  1;
        bool inc_alloc: 1;  //for increaase space alloc
        bool space_end: 1;  //for space end offset
        bool src_inode: 1;  //for create hard link only
        bool blocked: 1;    //for set flock and sys-lock only
    };
} FDIRStatModifyFlags;

typedef struct fdir_set_dentry_size_info {
    uint64_t inode;
    int64_t file_size;
    int64_t inc_alloc;
    bool force;
    int flags;
} FDIRSetDEntrySizeInfo;

typedef SFBinlogWriterStat FDIRBinlogWriterStat;

#endif

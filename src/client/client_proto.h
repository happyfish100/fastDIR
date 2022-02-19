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


#ifndef _FDIR_CLIENT_PROTO_H
#define _FDIR_CLIENT_PROTO_H

#include <dirent.h>
#include "fastcommon/fast_mpool.h"
#include "sf/sf_proto.h"
#include "fdir_types.h"
#include "client_types.h"

typedef struct dirent FDIRDirent;

typedef struct fdir_client_owner_mode_pair {
    uid_t uid;
    gid_t gid;
    mode_t mode;
} FDIRClientOwnerModePair;

typedef struct fdir_client_dentry {
    FDIRDEntryInfo dentry;
    string_t name;
} FDIRClientDentry;

typedef struct fdir_client_buffer {
    int size;
    char fixed[16 * 1024]; //fixed buffer
    char *buff;            //recv buffer
} FDIRClientBuffer;

#define DENTRY_ARRAY_COMMON_FIELDS(type) \
    int alloc;  \
    int count;  \
    type *entries; \
    FDIRClientBuffer buffer

typedef struct fdir_client_common_dentry_array {
    DENTRY_ARRAY_COMMON_FIELDS(void);
} FDIRClientCommonDentryArray;

typedef struct fdir_client_dentry_array {
    DENTRY_ARRAY_COMMON_FIELDS(FDIRClientDentry);
    struct {
        struct {
            struct fast_mpool_man holder;
            struct fast_mpool_man *ptr;
        } mpool;
        bool inited;
        bool used;
        bool cloned;
    } name_allocator;
} FDIRClientDentryArray;

typedef struct fdir_client_compact_dentry_array {
    DENTRY_ARRAY_COMMON_FIELDS(FDIRDirent);
} FDIRClientCompactDentryArray;

typedef struct fdir_client_service_stat {
    int server_id;
    bool is_master;
    char status;

    struct {
        int current_count;
        int max_count;
    } connection;

    struct {
        int64_t current_version;
        FDIRBinlogWriterStat writer;
    } binlog;

    struct {
        int64_t current_inode_sn;
        struct {
            int64_t ns;
            int64_t dir;
            int64_t file;
        } counters;
    } dentry;
} FDIRClientServiceStat;

typedef struct fdir_client_namespace_stat {
    SFSpaceStat inode;
    struct {
        int64_t used;
    } space;
} FDIRClientNamespaceStat;

typedef struct fdir_client_cluster_stat_entry {
    int server_id;
    bool is_master;
    char status;
    char ip_addr[IP_ADDRESS_SIZE];
    uint16_t port;
} FDIRClientClusterStatEntry;

typedef struct fdir_client_namespace_stat_entry {
    string_t ns_name;
    int64_t used_bytes;
} FDIRClientNamespaceStatEntry;

typedef struct fdir_client_namespace_stat_array {
    int alloc;
    int count;
    FDIRClientNamespaceStatEntry *entries;
    SFProtoRecvBuffer buffer;
} FDIRClientNamespaceStatArray;

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_init_session(FDIRClientContext *client_ctx,
    FDIRClientSession *session);

void fdir_client_close_session(FDIRClientSession *session,
        const bool force_close);

int fdir_client_proto_join_server(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, SFConnectionParameters *conn_params);

int fdir_client_proto_create_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        const dev_t rdev, FDIRDEntryInfo *dentry);

int fdir_client_proto_create_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp,
        const dev_t rdev, FDIRDEntryInfo *dentry);

int fdir_client_proto_symlink_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_proto_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_link_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRClientOwnerModePair *omp, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_link_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const int64_t src_inode,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_remove_dentry_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_proto_rename_dentry_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags, FDIRDEntryInfo **dentry);

int fdir_client_proto_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const int flags, FDIRDEntryInfo **dentry);

int fdir_client_proto_lookup_inode_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int enoent_log_level, int64_t *inode);

int fdir_client_proto_lookup_inode_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const FDIRDEntryPName *pname,
        const int enoent_log_level, int64_t *inode);

int fdir_client_proto_stat_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_proto_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_proto_stat_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_proto_readlink_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        string_t *link, const int size);

int fdir_client_proto_readlink_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const FDIRDEntryPName *pname,
        string_t *link, const int size);

int fdir_client_proto_readlink_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        string_t *link, const int size);

int fdir_client_proto_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_batch_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const FDIRSetDEntrySizeInfo *dsizes, const int count);

int fdir_client_proto_modify_stat_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const int64_t inode, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_proto_modify_stat_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_flock_dentry_ex2(FDIRClientSession *session, const string_t *ns,
        const int64_t inode, const int operation, const int64_t offset,
        const int64_t length, const int64_t owner_id, const pid_t pid);

static inline int fdir_client_flock_dentry_ex(FDIRClientSession *session,
        const string_t *ns, const int64_t inode, const int operation,
        const int64_t offset, const int64_t length)
{
    return fdir_client_flock_dentry_ex2(session, ns, inode, operation,
            offset, length, (long)pthread_self(), getpid());
}

static inline int fdir_client_flock_dentry(FDIRClientSession *session,
        const string_t *ns, const int64_t inode, const int operation)
{
    return fdir_client_flock_dentry_ex(session, ns, inode, operation, 0, 0);
}

int fdir_client_proto_getlk_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        int *operation, int64_t *offset, int64_t *length,
        int64_t *owner_id, pid_t *pid);

int fdir_client_dentry_sys_lock(FDIRClientSession *session,
        const string_t *ns, const int64_t inode, const int flags,
        int64_t *file_size, int64_t *space_end);

int fdir_client_dentry_sys_unlock_ex(FDIRClientSession *session,
        const string_t *ns, const int64_t old_size,
        const FDIRSetDEntrySizeInfo *dsize);

static inline int fdir_client_dentry_sys_unlock(
        FDIRClientSession *session, const int64_t inode)
{
    FDIRSetDEntrySizeInfo dsize;
    dsize.inode = inode;
    dsize.file_size = 0;
    dsize.inc_alloc = 0;
    dsize.flags = 0;
    dsize.force = false;
    return fdir_client_dentry_sys_unlock_ex(session, NULL, 0, &dsize);
}

int fdir_client_proto_set_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const
        FDIRDEntryFullName *fullname, const key_value_pair_t *xattr,
        const int flags);

int fdir_client_proto_set_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const int64_t inode, const key_value_pair_t *xattr,
        const int flags);

int fdir_client_proto_remove_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const
        FDIRDEntryFullName *fullname, const string_t *name,
        const int flags, const int enoattr_log_level);

int fdir_client_proto_remove_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const int64_t inode, const string_t *name, const int flags,
        const int enoattr_log_level);

int fdir_client_proto_get_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const string_t *name, const int enoattr_log_level,
        string_t *value, const int size, const int flags);

int fdir_client_proto_get_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        const string_t *name, const int enoattr_log_level,
        string_t *value, const int size, const int flags);

int fdir_client_proto_list_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        string_t *list, const int size, const int flags);

int fdir_client_proto_list_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        string_t *list, const int size, const int flags);

int fdir_client_proto_list_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        FDIRClientDentryArray *array);

int fdir_client_proto_list_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        FDIRClientDentryArray *array);

int fdir_client_proto_list_compact_dentry_by_path(FDIRClientContext
        *client_ctx, ConnectionInfo *conn, const FDIRDEntryFullName
        *fullname, FDIRClientCompactDentryArray *array);

int fdir_client_proto_list_compact_dentry_by_inode(FDIRClientContext
        *client_ctx, ConnectionInfo *conn, const string_t *ns,
        const int64_t inode, FDIRClientCompactDentryArray *array);

int fdir_client_dentry_array_init_ex(FDIRClientDentryArray *array,
        struct fast_mpool_man *mpool);

#define fdir_client_dentry_array_init(array) \
    fdir_client_dentry_array_init_ex(array, NULL)

void fdir_client_dentry_array_free(FDIRClientDentryArray *array);

void fdir_client_compact_dentry_array_init(
        FDIRClientCompactDentryArray *array);

void fdir_client_compact_dentry_array_free(
        FDIRClientCompactDentryArray *array);

int fdir_client_proto_nss_subscribe(FDIRClientContext *client_ctx,
        ConnectionInfo *conn);

int fdir_client_proto_nss_fetch(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, FDIRClientNamespaceStatArray *array,
        bool *is_last);

int fdir_client_namespace_stat_array_init(FDIRClientNamespaceStatArray *array);
void fdir_client_namespace_stat_array_free(FDIRClientNamespaceStatArray *array);

int fdir_client_service_stat(FDIRClientContext *client_ctx,
        const ConnectionInfo *spec_conn, FDIRClientServiceStat *stat);

int fdir_client_cluster_stat(FDIRClientContext *client_ctx,
        FDIRClientClusterStatEntry *stats, const int size, int *count);

int fdir_client_proto_namespace_stat(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns,
        FDIRClientNamespaceStat *stat);

int fdir_client_get_master(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *master);

int fdir_client_get_slaves(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *slaves, const int size, int *count);

int fdir_client_get_readable_server(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *server);

#ifdef __cplusplus
}
#endif

#endif

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

#include "fastcommon/fast_mpool.h"
#include "fdir_types.h"
#include "client_types.h"

#define FDIR_CLIENT_BATCH_SET_DENTRY_MAX_COUNT  256

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

typedef struct fdir_client_dentry_array {
    int alloc;
    int count;
    FDIRClientDentry *entries;
    FDIRClientBuffer buffer;
    struct {
        struct fast_mpool_man mpool;
        bool inited;
        bool used;
    } name_allocator;
} FDIRClientDentryArray;

typedef struct fdir_client_service_stat {
    int server_id;
    bool is_master;
    char status;

    struct {
        int current_count;
        int max_count;
    } connection;

    struct {
        int64_t current_data_version;
        int64_t current_inode_sn;
        struct {
            int64_t ns;
            int64_t dir;
            int64_t file;
        } counters;
    } dentry;
} FDIRClientServiceStat;

typedef struct fdir_client_cluster_stat_entry {
    int server_id;
    bool is_master;
    char status;
    char ip_addr[IP_ADDRESS_SIZE];
    uint16_t port;
} FDIRClientClusterStatEntry;

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_init_session(FDIRClientContext *client_ctx,
    FDIRClientSession *session);

void fdir_client_close_session(FDIRClientSession *session,
        const bool force_close);

int fdir_client_proto_join_server(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, FDIRConnectionParameters *conn_params);

int fdir_client_proto_create_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_create_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

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
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_proto_link_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const int64_t src_inode,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_proto_remove_dentry_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

int fdir_client_proto_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_rename_dentry_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags, FDIRDEntryInfo **dentry);

int fdir_client_proto_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const int flags, FDIRDEntryInfo **dentry);

int fdir_client_proto_lookup_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int enoent_log_level, int64_t *inode);

int fdir_client_proto_stat_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_proto_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode, FDIRDEntryInfo *dentry);

int fdir_client_proto_stat_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryPName *pname,
        const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_proto_readlink_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        string_t *link, const int size);

int fdir_client_proto_readlink_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryPName *pname,
        string_t *link, const int size);

int fdir_client_proto_readlink_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode, string_t *link,
        const int size);

int fdir_client_proto_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry);

int fdir_client_proto_batch_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const FDIRSetDEntrySizeInfo *dsizes, const int count);

int fdir_client_proto_modify_dentry_stat(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const int64_t inode, const int64_t flags,
        const FDIRDEntryStatus *stat, FDIRDEntryInfo *dentry);

int fdir_client_flock_dentry_ex2(FDIRClientSession *session,
        const int64_t inode, const int operation, const int64_t offset,
        const int64_t length, const int64_t owner_id, const pid_t pid);

static inline int fdir_client_flock_dentry_ex(FDIRClientSession *session,
        const int64_t inode, const int operation, const int64_t offset,
        const int64_t length)
{
    return fdir_client_flock_dentry_ex2(session, inode, operation,
            offset, length, (long)pthread_self(), getpid());
}

static inline int fdir_client_flock_dentry(FDIRClientSession *session,
        const int64_t inode, const int operation)
{
    return fdir_client_flock_dentry_ex(session, inode, operation, 0, 0);
}

int fdir_client_proto_getlk_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode, int *operation,
        int64_t *offset, int64_t *length, int64_t *owner_id, pid_t *pid);

int fdir_client_dentry_sys_lock(FDIRClientSession *session,
        const int64_t inode, const int flags, int64_t *file_size,
        int64_t *space_end);

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

int fdir_client_proto_list_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        FDIRClientDentryArray *array);

int fdir_client_proto_list_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode,
        FDIRClientDentryArray *array);

int fdir_client_dentry_array_init(FDIRClientDentryArray *array);

void fdir_client_dentry_array_free(FDIRClientDentryArray *array);

int fdir_client_service_stat(FDIRClientContext *client_ctx,
        const char *ip_addr, const int port, FDIRClientServiceStat *stat);

int fdir_client_cluster_stat(FDIRClientContext *client_ctx,
        FDIRClientClusterStatEntry *stats, const int size, int *count);

int fdir_client_proto_namespace_stat(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, FDIRInodeStat *stat);

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

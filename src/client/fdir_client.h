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


#ifndef _FDIR_CLIENT_H
#define _FDIR_CLIENT_H

#include "fdir_proto.h"
#include "client_types.h"
#include "client_func.h"
#include "client_global.h"
#include "client_proto.h"

#define fdir_client_create_dentry(client_ctx, fullname, omp, dentry) \
        fdir_client_create_dentry_ex(client_ctx, fullname, omp, 0, dentry)

#define fdir_client_create_dentry_by_pname(client_ctx, ns, pname, omp, dentry) \
        fdir_client_create_dentry_by_pname_ex(client_ctx, \
                ns, pname, omp, 0, dentry)

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_create_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        const dev_t rdev, FDIRDEntryInfo *dentry);

int fdir_client_create_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, const dev_t rdev,
        FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry(FDIRClientContext *client_ctx,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry);

int fdir_client_link_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRClientOwnerModePair *omp, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_link_dentry_by_pname(FDIRClientContext *client_ctx,
        const int64_t src_inode, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, FDIRDEntryInfo *dentry);

static inline int fdir_client_remove_dentry_by_pname(
        FDIRClientContext *client_ctx, const string_t *ns,
        const FDIRDEntryPName *pname, const int flags)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_by_pname_ex(client_ctx,
            ns, pname, flags, &dentry);
}

static inline int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int flags)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_ex(client_ctx,
            fullname, flags, &dentry);
}

int fdir_client_rename_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags, FDIRDEntryInfo **dentry);

static inline int fdir_client_rename_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags)
{
    FDIRDEntryInfo dentry;
    FDIRDEntryInfo *p;
    p = &dentry;
    return fdir_client_rename_dentry_ex(client_ctx, src, dest, flags, &p);
}

int fdir_client_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const int flags, FDIRDEntryInfo **dentry);

static inline int fdir_client_rename_dentry_by_pname(
        FDIRClientContext *client_ctx, const string_t *src_ns,
        const FDIRDEntryPName *src_pname, const string_t *dest_ns,
        const FDIRDEntryPName *dest_pname, const int flags)
{
    FDIRDEntryInfo dentry;
    FDIRDEntryInfo *p;
    p = &dentry;
    return fdir_client_rename_dentry_by_pname_ex(client_ctx, src_ns,
            src_pname, dest_ns, dest_pname, flags, &p);
}

int fdir_client_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry);

int fdir_client_batch_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsizes,
        const int count);

int fdir_client_modify_stat_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_modify_stat_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, int *operation,
        int64_t *offset, int64_t *length, int64_t *owner_id, pid_t *pid);

int fdir_client_set_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const key_value_pair_t *xattr,
        const int flags);

int fdir_client_set_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const key_value_pair_t
        *xattr, const int flags);

int fdir_client_remove_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const string_t *name,
        const int flags, const int enoattr_log_level);

int fdir_client_remove_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const string_t *name,
        const int flags, const int enoattr_log_level);

int fdir_client_get_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const string_t *name,
        const int enoattr_log_level, string_t *value, const int size,
        const int flags);

int fdir_client_get_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const string_t *name,
        const int enoattr_log_level, string_t *value, const int size,
        const int flags);

int fdir_client_list_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, string_t *list,
        const int size, const int flags);

int fdir_client_list_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, string_t *list,
        const int size, const int flags);

#define fdir_client_lookup_inode_by_path(client_ctx, fullname, inode) \
    fdir_client_lookup_inode_by_path_ex(client_ctx, fullname, LOG_ERR, inode)

#define fdir_client_lookup_inode_by_pname(client_ctx, ns, pname, inode) \
    fdir_client_lookup_inode_by_pname_ex(client_ctx, ns, pname, LOG_ERR, inode)

#define fdir_client_stat_dentry_by_path(client_ctx, fullname, flags, dentry) \
    fdir_client_stat_dentry_by_path_ex(client_ctx, \
            fullname, LOG_ERR, flags, dentry)

#define fdir_client_stat_dentry_by_pname(client_ctx, ns, pname, flags, dentry) \
    fdir_client_stat_dentry_by_pname_ex(client_ctx, \
            ns, pname, LOG_ERR, flags, dentry)

#define fdir_client_get_xattr_by_path(client_ctx, \
        fullname, name, value, size, flags) \
    fdir_client_get_xattr_by_path_ex(client_ctx, fullname, \
            name, LOG_ERR, value, size, flags)

#define fdir_client_get_xattr_by_inode(client_ctx, ns, \
        inode, name, value, size, flags) \
    fdir_client_get_xattr_by_inode_ex(client_ctx, ns, \
            inode, name, LOG_ERR, value, size, flags)

#define fdir_client_remove_xattr_by_path(client_ctx, \
        fullname, name, flags) \
    fdir_client_remove_xattr_by_path_ex(client_ctx, \
            fullname, name, flags, LOG_ERR)

#define fdir_client_remove_xattr_by_inode(client_ctx, \
        ns, inode, name, flags) \
    fdir_client_remove_xattr_by_inode_ex(client_ctx, \
            ns, inode, name, flags, LOG_ERR)

int fdir_client_lookup_inode_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int enoent_log_level,
        int64_t *inode);

int fdir_client_lookup_inode_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int enoent_log_level, int64_t *inode);

int fdir_client_stat_dentry_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int flags,
        const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_readlink_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, string_t *link, const int size);

int fdir_client_readlink_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        string_t *link, const int size);

int fdir_client_readlink_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, string_t *link,
        const int size);

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array);

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode,
        FDIRClientDentryArray *array);

int fdir_client_list_compact_dentry_by_path(
        FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        FDIRClientCompactDentryArray *array);

int fdir_client_list_compact_dentry_by_inode(
        FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode,
        FDIRClientCompactDentryArray *array);

int fdir_client_namespace_stat(FDIRClientContext *client_ctx,
        const string_t *ns, FDIRClientNamespaceStat *stat);

#ifdef __cplusplus
}
#endif

#endif

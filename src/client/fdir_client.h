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

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_create_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry);

int fdir_client_create_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry(FDIRClientContext *client_ctx,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry);

int fdir_client_link_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry);

int fdir_client_link_dentry_by_pname(FDIRClientContext *client_ctx,
        const int64_t src_inode, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        FDIRDEntryInfo *dentry);

static inline int fdir_client_remove_dentry_by_pname(
        FDIRClientContext *client_ctx, const string_t *ns,
        const FDIRDEntryPName *pname)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_by_pname_ex(client_ctx,
            ns, pname, &dentry);
}

static inline int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_ex(client_ctx,
            fullname, &dentry);
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
        const string_t *ns, const int64_t inode, const int64_t size,
        const int64_t inc_alloc, const bool force, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_modify_dentry_stat(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t flags,
        const FDIRDEntryStatus *stat, FDIRDEntryInfo *dentry);

int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const int64_t inode, int *operation, int64_t *offset,
        int64_t *length, int64_t *owner_id, pid_t *pid);


#define fdir_client_lookup_inode(client_ctx, fullname, inode) \
    fdir_client_lookup_inode_ex(client_ctx, fullname, LOG_ERR, inode)

#define fdir_client_stat_dentry_by_path(client_ctx, fullname, dentry) \
    fdir_client_stat_dentry_by_path_ex(client_ctx, fullname, LOG_ERR, dentry)

#define fdir_client_stat_dentry_by_pname(client_ctx, pname, dentry) \
    fdir_client_stat_dentry_by_pname_ex(client_ctx, pname, LOG_ERR, dentry)

int fdir_client_lookup_inode_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int enoent_log_level,
        int64_t *inode);

int fdir_client_stat_dentry_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int enoent_log_level,
        FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryPName *pname, const int enoent_log_level,
        FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, FDIRDEntryInfo *dentry);

int fdir_client_readlink_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, string_t *link, const int size);

int fdir_client_readlink_by_pname(FDIRClientContext *client_ctx,
        const FDIRDEntryPName *pname, string_t *link, const int size);

int fdir_client_readlink_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, string_t *link, const int size);

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array);

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, FDIRClientDentryArray *array);

int fdir_client_namespace_stat(FDIRClientContext *client_ctx,
        const string_t *ns, FDIRInodeStat *stat);

#ifdef __cplusplus
}
#endif

#endif

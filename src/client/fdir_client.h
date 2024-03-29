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

#define fdir_client_create_dentry(client_ctx, path, mode, dentry) \
        fdir_client_create_dentry_ex(client_ctx, path, mode, 0, dentry)

#define fdir_client_create_dentry_by_pname(   \
        client_ctx, ns, opname, mode, dentry) \
        fdir_client_create_dentry_by_pname_ex(client_ctx, \
                ns, opname, mode, 0, dentry)

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_generate_node_id(FDIRClientContext *client_ctx,
        uint32_t *node_id, int64_t *key);

int fdir_client_create_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const mode_t mode,
        const dev_t rdev, FDIRDEntryInfo *dentry);

int fdir_client_create_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const mode_t mode, const dev_t rdev, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry(FDIRClientContext *client_ctx,
        const string_t *link, const FDIRClientOperFnamePair *path,
        const mode_t mode, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *link, const string_t *ns,
        const FDIRClientOperPnamePair *opname,
        const mode_t mode, FDIRDEntryInfo *dentry);

int fdir_client_link_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRDentryOperator *oper, const mode_t mode,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_link_dentry_by_pname(FDIRClientContext *client_ctx,
        const int64_t src_inode, const string_t *ns,
        const FDIRClientOperPnamePair *opname, const mode_t mode,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int flags,
        FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int flags, FDIRDEntryInfo *dentry);

static inline int fdir_client_remove_dentry_by_pname(
        FDIRClientContext *client_ctx, const string_t *ns,
        const FDIRClientOperPnamePair *opname, const int flags)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_by_pname_ex(client_ctx,
            ns, opname, flags, &dentry);
}

static inline int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int flags)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_ex(client_ctx, path, flags, &dentry);
}

int fdir_client_rename_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRDentryOperator *oper, const int flags,
        FDIRDEntryInfo **dentry);

static inline int fdir_client_rename_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRDentryOperator *oper, const int flags)
{
    FDIRDEntryInfo dentry;
    FDIRDEntryInfo *p;
    p = &dentry;
    return fdir_client_rename_dentry_ex(client_ctx,
            src, dest, oper, flags, &p);
}

int fdir_client_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const FDIRDentryOperator *oper, const int flags,
        FDIRDEntryInfo **dentry);

static inline int fdir_client_rename_dentry_by_pname(
        FDIRClientContext *client_ctx, const string_t *src_ns,
        const FDIRDEntryPName *src_pname, const string_t *dest_ns,
        const FDIRDEntryPName *dest_pname,
        const FDIRDentryOperator *oper, const int flags)
{
    FDIRDEntryInfo dentry;
    FDIRDEntryInfo *p;
    p = &dentry;
    return fdir_client_rename_dentry_by_pname_ex(client_ctx, src_ns,
            src_pname, dest_ns, dest_pname, oper, flags, &p);
}

int fdir_client_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry);

int fdir_client_batch_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsizes,
        const int count);

int fdir_client_modify_stat_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const int64_t mflags, const FDIRDEntryStat *stat,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_modify_stat_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        int *operation, int64_t *offset, int64_t *length,
        FDIRFlockOwner *owner);

int fdir_client_set_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const key_value_pair_t *xattr,
        const int flags);

int fdir_client_set_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const key_value_pair_t *xattr, const int flags);

int fdir_client_remove_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const string_t *name,
        const int flags, const int enoattr_log_level);

int fdir_client_remove_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const string_t *name, const int flags, const int enoattr_log_level);

int fdir_client_get_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const string_t *name,
        const int enoattr_log_level, string_t *value, const int size,
        const int flags);

int fdir_client_get_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const string_t *name, const int enoattr_log_level,
        string_t *value, const int size, const int flags);

int fdir_client_list_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, string_t *list,
        const int size, const int flags);

int fdir_client_list_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        string_t *list, const int size, const int flags);

#define fdir_client_lookup_inode_by_path(client_ctx, path, inode) \
    fdir_client_lookup_inode_by_path_ex(client_ctx, path, LOG_ERR, inode)

#define fdir_client_lookup_inode_by_pname(client_ctx, ns, opname, inode) \
    fdir_client_lookup_inode_by_pname_ex(client_ctx, ns, opname, LOG_ERR, inode)

#define fdir_client_stat_dentry_by_path(client_ctx, path, flags, dentry) \
    fdir_client_stat_dentry_by_path_ex(client_ctx, \
            path, LOG_ERR, flags, dentry)

#define fdir_client_stat_dentry_by_pname(client_ctx, ns, opname, flags, dentry) \
    fdir_client_stat_dentry_by_pname_ex(client_ctx, \
            ns, opname, LOG_ERR, flags, dentry)

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
        const FDIRClientOperFnamePair *path, const int enoent_log_level,
        int64_t *inode);

int fdir_client_lookup_inode_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int enoent_log_level, int64_t *inode);

int fdir_client_stat_dentry_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int flags,
        const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_access_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const char mask,
        const int flags, FDIRDEntryInfo *dentry);

int fdir_client_access_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const char mask, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_access_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const char mask, const int flags, FDIRDEntryInfo *dentry);

int fdir_client_readlink_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, string_t *link, const int size);

int fdir_client_readlink_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        string_t *link, const int size);

int fdir_client_readlink_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        string_t *link, const int size);

int fdir_client_get_fullname_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const int flags, string_t *fullname, const int size);

int fdir_client_get_fullname_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int flags, string_t *fullname, const int size);

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, FDIRClientDentryArray *array,
        const int flags);

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        FDIRClientDentryArray *array, const int flags);

int fdir_client_list_compact_dentry_by_path(
        FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path,
        FDIRClientCompactDentryArray *array);

int fdir_client_list_compact_dentry_by_inode(
        FDIRClientContext *client_ctx, const string_t *ns,
        const FDIRClientOperInodePair *oino,
        FDIRClientCompactDentryArray *array);

int fdir_client_namespace_stat(FDIRClientContext *client_ctx,
        const string_t *ns, FDIRClientNamespaceStat *stat);

int fdir_client_namespace_list(FDIRClientContext *client_ctx,
        int *server_id, FDIRClientNamespaceArray *array);


//init node id for flock
int fdir_client_init_node_id(FDIRClientContext *client_ctx);

#ifdef __cplusplus
}
#endif

#endif

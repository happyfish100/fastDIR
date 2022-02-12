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

#include "sf/idempotency/client/client_channel.h"
#include "sf/idempotency/client/rpc_wrapper.h"
#include "client_global.h"
#include "fdir_client.h"

#define GET_MASTER_CONNECTION(cm, arg1, result)   \
    (cm)->ops.get_master_connection(cm, arg1, result)

#define GET_READABLE_CONNECTION(cm, arg1, result) \
    (cm)->ops.get_readable_connection(cm, arg1, result)

int fdir_client_create_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        const dev_t rdev, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_create_dentry,
            fullname, omp, rdev, dentry);
}

int fdir_client_create_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, const dev_t rdev,
        FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_create_dentry_by_pname,
            ns, pname, omp, rdev, dentry);
}

int fdir_client_symlink_dentry(FDIRClientContext *client_ctx,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_symlink_dentry,
            link, fullname, omp, dentry);
}

int fdir_client_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_symlink_dentry_by_pname,
            link, ns, pname, omp, dentry);
}

int fdir_client_link_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRClientOwnerModePair *omp, const int flags,
        FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_link_dentry,
            src, dest, omp, flags, dentry);
}

int fdir_client_link_dentry_by_pname(FDIRClientContext *client_ctx,
        const int64_t src_inode, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_link_dentry_by_pname,
            src_inode, ns, pname, omp, flags, dentry);
}

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int flags,
        FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_dentry_ex,
            fullname, flags, dentry);
}

int fdir_client_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_dentry_by_pname_ex,
            ns, pname, flags, dentry);
}

int fdir_client_rename_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags, FDIRDEntryInfo **dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_rename_dentry_ex,
            src, dest, flags, dentry);
}

int fdir_client_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const int flags, FDIRDEntryInfo **dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_rename_dentry_by_pname_ex,
            src_ns, src_pname, dest_ns, dest_pname, flags, dentry);
}

int fdir_client_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_set_dentry_size,
            ns, dsize, dentry);
}

int fdir_client_batch_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsizes,
        const int count)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_batch_set_dentry_size,
            ns, dsizes, count);
}

int fdir_client_modify_stat_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_modify_stat_by_inode,
            ns, inode, mflags, stat, flags, dentry);
}

int fdir_client_modify_stat_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_modify_stat_by_path,
            fullname, mflags, stat, flags, dentry);
}

int fdir_client_set_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const key_value_pair_t *xattr,
        const int flags)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_set_xattr_by_path,
            fullname, xattr, flags);
}

int fdir_client_set_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const key_value_pair_t
        *xattr, const int flags)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_set_xattr_by_inode,
            ns, inode, xattr, flags);
}

int fdir_client_remove_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const string_t *name,
        const int flags, const int enoattr_log_level)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_xattr_by_path,
            fullname, name, flags, enoattr_log_level);
}

int fdir_client_remove_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const string_t *name,
        const int flags, const int enoattr_log_level)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_xattr_by_inode,
            ns, inode, name, flags, enoattr_log_level);
}

int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, int *operation,
        int64_t *offset, int64_t *length, int64_t *owner_id, pid_t *pid)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_getlk_dentry,
            ns, inode, operation, offset, length, owner_id, pid);
}

int fdir_client_lookup_inode_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int enoent_log_level,
        int64_t *inode)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_lookup_inode_by_path,
            fullname, enoent_log_level, inode);
}

int fdir_client_lookup_inode_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int enoent_log_level, int64_t *inode)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_lookup_inode_by_pname,
            ns, pname, enoent_log_level, inode);
}

int fdir_client_stat_dentry_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int flags,
        const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_stat_dentry_by_path,
            fullname, flags, enoent_log_level, dentry);
}

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode,
        const int flags, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_stat_dentry_by_inode,
            ns, inode, flags, dentry);
}

int fdir_client_stat_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_stat_dentry_by_pname,
            ns, pname, flags, enoent_log_level, dentry);
}

int fdir_client_readlink_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_readlink_by_path,
            fullname, link, size);
}

int fdir_client_readlink_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_readlink_by_pname,
            ns, pname, link, size);
}

int fdir_client_readlink_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, string_t *link,
        const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_readlink_by_inode,
            ns, inode, link, size);
}

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_dentry_by_path,
            fullname, array);
}

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, FDIRClientDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_dentry_by_inode,
            ns, inode, array);
}

int fdir_client_list_compact_dentry_by_path(
        FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        FDIRClientCompactDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx,
            &client_ctx->cm, GET_READABLE_CONNECTION, 0,
            fdir_client_proto_list_compact_dentry_by_path,
            fullname, array);
}

int fdir_client_list_compact_dentry_by_inode(
        FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode,
        FDIRClientCompactDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx,
            &client_ctx->cm, GET_READABLE_CONNECTION, 0,
            fdir_client_proto_list_compact_dentry_by_inode,
            ns, inode, array);
}

int fdir_client_namespace_stat(FDIRClientContext *client_ctx,
        const string_t *ns, FDIRClientNamespaceStat *stat)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_namespace_stat,
            ns, stat);
}

int fdir_client_get_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const string_t *name,
        const int enoattr_log_level, string_t *value,
        const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_get_xattr_by_path,
            fullname, name, enoattr_log_level, value, size, flags);
}

int fdir_client_get_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const string_t *name,
        const int enoattr_log_level, string_t *value,
        const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_get_xattr_by_inode,
            ns, inode, name, enoattr_log_level, value, size, flags);
}

int fdir_client_list_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, string_t *list,
        const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_xattr_by_path,
            fullname, list, size, flags);
}

int fdir_client_list_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, string_t *list,
        const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_xattr_by_inode,
            ns, inode, list, size, flags);
}

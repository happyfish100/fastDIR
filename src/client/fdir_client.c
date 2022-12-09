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

int fdir_client_generate_node_id(FDIRClientContext *client_ctx,
        uint32_t *node_id, int64_t *key)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_generate_node_id,
            node_id, key);
}

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
        const FDIRClientOperFnamePair *path, const int flags,
        FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_dentry_ex,
            path, flags, dentry);
}

int fdir_client_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_dentry_by_pname_ex,
            ns, opname, flags, dentry);
}

int fdir_client_rename_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRDentryOperator *oper, const int flags,
        FDIRDEntryInfo **dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_rename_dentry_ex,
            src, dest, oper, flags, dentry);
}

int fdir_client_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const FDIRDentryOperator *oper, const int flags,
        FDIRDEntryInfo **dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_rename_dentry_by_pname_ex,
            src_ns, src_pname, dest_ns, dest_pname, oper, flags, dentry);
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
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const int64_t mflags, const FDIRDEntryStat *stat,
        const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_modify_stat_by_inode,
            ns, oino, mflags, stat, flags, dentry);
}

int fdir_client_modify_stat_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_modify_stat_by_path,
            path, mflags, stat, flags, dentry);
}

int fdir_client_set_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const key_value_pair_t *xattr,
        const int flags)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_set_xattr_by_path,
            path, xattr, flags);
}

int fdir_client_set_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const key_value_pair_t *xattr, const int flags)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_set_xattr_by_inode,
            ns, oino, xattr, flags);
}

int fdir_client_remove_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const string_t *name,
        const int flags, const int enoattr_log_level)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_xattr_by_path,
            path, name, flags, enoattr_log_level);
}

int fdir_client_remove_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const string_t *name, const int flags, const int enoattr_log_level)
{
    const SFConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_remove_xattr_by_inode,
            ns, oino, name, flags, enoattr_log_level);
}

int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        int *operation, int64_t *offset, int64_t *length,
        FDIRFlockOwner *owner)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_getlk_dentry, ns,
            oino, operation, offset, length, owner);
}

int fdir_client_lookup_inode_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int enoent_log_level,
        int64_t *inode)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_lookup_inode_by_path,
            path, enoent_log_level, inode);
}

int fdir_client_lookup_inode_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int enoent_log_level, int64_t *inode)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_lookup_inode_by_pname,
            ns, opname, enoent_log_level, inode);
}

int fdir_client_stat_dentry_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const int flags,
        const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_stat_dentry_by_path,
            path, flags, enoent_log_level, dentry);
}

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const int flags, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_stat_dentry_by_inode,
            ns, oino, flags, dentry);
}

int fdir_client_stat_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_stat_dentry_by_pname,
            ns, opname, flags, enoent_log_level, dentry);
}

int fdir_client_access_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const char mask,
        const int flags, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_access_dentry_by_path,
            path, mask, flags, dentry);
}

int fdir_client_access_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const char mask, const int flags, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_access_dentry_by_inode,
            ns, oino, mask, flags, dentry);
}

int fdir_client_access_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        const char mask, const int flags, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_access_dentry_by_pname,
            ns, opname, mask, flags, dentry);
}

int fdir_client_readlink_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_readlink_by_path,
            path, link, size);
}

int fdir_client_readlink_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperPnamePair *opname,
        string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_readlink_by_pname,
            ns, opname, link, size);
}

int fdir_client_readlink_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_readlink_by_inode,
            ns, oino, link, size);
}

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, FDIRClientDentryArray *array,
        const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_dentry_by_path,
            path, array, flags);
}

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        FDIRClientDentryArray *array, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_dentry_by_inode,
            ns, oino, array, flags);
}

int fdir_client_list_compact_dentry_by_path(
        FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path,
        FDIRClientCompactDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx,
            &client_ctx->cm, GET_READABLE_CONNECTION, 0,
            fdir_client_proto_list_compact_dentry_by_path,
            path, array);
}

int fdir_client_list_compact_dentry_by_inode(
        FDIRClientContext *client_ctx, const string_t *ns,
        const FDIRClientOperInodePair *oino,
        FDIRClientCompactDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx,
            &client_ctx->cm, GET_READABLE_CONNECTION, 0,
            fdir_client_proto_list_compact_dentry_by_inode,
            ns, oino, array);
}

int fdir_client_namespace_stat(FDIRClientContext *client_ctx,
        const string_t *ns, FDIRClientNamespaceStat *stat)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_MASTER_CONNECTION, 0, fdir_client_proto_namespace_stat,
            ns, stat);
}

int fdir_client_get_xattr_by_path_ex(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, const string_t *name,
        const int enoattr_log_level, string_t *value,
        const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_get_xattr_by_path,
            path, name, enoattr_log_level, value, size, flags);
}

int fdir_client_get_xattr_by_inode_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        const string_t *name, const int enoattr_log_level,
        string_t *value, const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_get_xattr_by_inode,
            ns, oino, name, enoattr_log_level, value, size, flags);
}

int fdir_client_list_xattr_by_path(FDIRClientContext *client_ctx,
        const FDIRClientOperFnamePair *path, string_t *list,
        const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_xattr_by_path,
            path, list, size, flags);
}

int fdir_client_list_xattr_by_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRClientOperInodePair *oino,
        string_t *list, const int size, const int flags)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, &client_ctx->cm,
            GET_READABLE_CONNECTION, 0, fdir_client_proto_list_xattr_by_inode,
            ns, oino, list, size, flags);
}

#define FDIR_CLIENT_NODE_ITEM_ID   "id"
#define FDIR_CLIENT_NODE_ITEM_KEY  "key"

static inline void fdir_client_get_node_info_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/.fdir-client-node",
            g_fdir_client_vars.base_path);
}

static int fdir_client_save_node_info(const int64_t key)
{
    char buff[256];
    char filename[PATH_MAX];
    int len;

    len = sprintf(buff, "%s=%u\n"
            "%s=%"PRId64"\n",
            FDIR_CLIENT_NODE_ITEM_ID, FDIR_CLIENT_NODE_ID,
            FDIR_CLIENT_NODE_ITEM_KEY, key);
    fdir_client_get_node_info_filename(filename, sizeof(filename));
    return safeWriteToFile(filename, buff, len);
}

static int fdir_client_load_node_info(int64_t *key)
{
    IniContext ini_context;
    char filename[PATH_MAX];
    int result;

    fdir_client_get_node_info_filename(filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            FDIR_CLIENT_NODE_ID = 0;
            *key = 0;
            return 0;
        }

        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, filename, result);
        return result;
    }

    FDIR_CLIENT_NODE_ID = iniGetIntValue(NULL,
            FDIR_CLIENT_NODE_ITEM_ID, &ini_context, 0);
    *key = iniGetInt64Value(NULL, FDIR_CLIENT_NODE_ITEM_KEY,
            &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

int fdir_client_init_node_id(FDIRClientContext *client_ctx)
{
    int result;
    uint32_t old_node_id;
    int64_t old_key;
    int64_t new_key;

    if ((result=fdir_client_load_node_info(&old_key)) != 0) {
        return result;
    }

    old_node_id = FDIR_CLIENT_NODE_ID;
    new_key = old_key;
    if ((result=fdir_client_generate_node_id(client_ctx,
                    &FDIR_CLIENT_NODE_ID, &new_key)) != 0)
    {
        return result;
    }

    if (FDIR_CLIENT_NODE_ID != old_node_id || new_key != old_key) {
        return fdir_client_save_node_info(new_key);
    }

    return 0;
}

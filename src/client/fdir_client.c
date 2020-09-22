#include "sf/idempotency/client/client_channel.h"
#include "sf/idempotency/client/rpc_wrapper.h"
#include "client_global.h"
#include "fdir_client.h"

#define GET_MASTER_CONNECTION(client_ctx, arg1, result)        \
    client_ctx->conn_manager.get_master_connection(client_ctx, \
            result)

#define GET_READABLE_CONNECTION(client_ctx, arg1, result)        \
    client_ctx->conn_manager.get_readable_connection(client_ctx, \
            result)

int fdir_client_create_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const mode_t mode,
        FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_create_dentry, fullname, mode, dentry);
}

int fdir_client_create_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const mode_t mode, FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_create_dentry_by_pname, ns, pname,
            mode, dentry);
}

int fdir_client_symlink_dentry(FDIRClientContext *client_ctx,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const mode_t mode, FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_symlink_dentry, link, fullname,
            mode, dentry);
}

int fdir_client_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const mode_t mode,
        FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_symlink_dentry_by_pname, link, ns, pname,
            mode, dentry);
}

int fdir_client_link_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const mode_t mode, FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_link_dentry, src, dest, mode, dentry);
}

int fdir_client_link_dentry_by_pname(FDIRClientContext *client_ctx,
        const int64_t src_inode, const string_t *ns,
        const FDIRDEntryPName *pname, const mode_t mode,
        FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_link_dentry_by_pname, src_inode, ns,
            pname, mode, dentry);
}

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_remove_dentry_ex, fullname, dentry);
}

int fdir_client_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_remove_dentry_by_pname_ex, ns,
            pname, dentry);
}

int fdir_client_rename_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags, FDIRDEntryInfo **dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_rename_dentry_ex, src, dest,
            flags, dentry);
}

int fdir_client_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const int flags, FDIRDEntryInfo **dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_rename_dentry_by_pname_ex, src_ns,
            src_pname, dest_ns, dest_pname, flags, dentry);
}

int fdir_client_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t size,
        const int64_t inc_alloc, const bool force, const int flags,
        FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_set_dentry_size, ns, inode, size,
            inc_alloc, force, flags, dentry);
}

int fdir_client_modify_dentry_stat(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t flags,
        const FDIRDEntryStatus *stat, FDIRDEntryInfo *dentry)
{
    const FDIRConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_modify_dentry_stat, ns, inode, flags,
            stat, dentry);
}


int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const int64_t inode, int *operation, int64_t *offset,
        int64_t *length, int64_t *owner_id, pid_t *pid)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            NULL, fdir_client_proto_getlk_dentry, inode, operation,
            offset, length, owner_id, pid);
}

int fdir_client_lookup_inode(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        const int enoent_log_level, int64_t *inode)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_lookup_inode, fullname,
            enoent_log_level, inode);
}

int fdir_client_stat_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname,
        const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_stat_dentry_by_path, fullname,
            enoent_log_level, dentry);
}

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_stat_dentry_by_inode, inode, dentry);
}

int fdir_client_stat_dentry_by_pname(FDIRClientContext *client_ctx,
        const FDIRDEntryPName *pname,
        const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_stat_dentry_by_pname, pname,
            enoent_log_level, dentry);
}

int fdir_client_readlink_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_readlink_by_path, fullname, link, size);
}

int fdir_client_readlink_by_pname(FDIRClientContext *client_ctx,
        const FDIRDEntryPName *pname, string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_readlink_by_pname, pname, link, size);
}

int fdir_client_readlink_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, string_t *link, const int size)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_readlink_by_inode, inode, link, size);
}

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_list_dentry_by_path, fullname, array);
}

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, FDIRClientDentryArray *array)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_READABLE_CONNECTION,
            NULL, fdir_client_proto_list_dentry_by_inode, inode, array);
}

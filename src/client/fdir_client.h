
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
        const FDIRDEntryFullName *fullname, const mode_t mode,
        FDIRDEntryInfo *dentry);

int fdir_client_create_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const mode_t mode, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry(FDIRClientContext *client_ctx,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const mode_t mode, FDIRDEntryInfo *dentry);

int fdir_client_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const mode_t mode,
        FDIRDEntryInfo *dentry);

int fdir_client_link_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const mode_t mode, FDIRDEntryInfo *dentry);

int fdir_client_link_dentry_by_pname(FDIRClientContext *client_ctx,
        const int64_t src_inode, const string_t *ns,
        const FDIRDEntryPName *pname, const mode_t mode,
        FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

static inline int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_ex(client_ctx,
            fullname, &dentry);
}

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
        const int64_t inc_alloc, const bool force, FDIRDEntryInfo *dentry,
        const int flags);

int fdir_client_modify_dentry_stat(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t flags,
        const FDIRDEntryStatus *stat, FDIRDEntryInfo *dentry);


#ifdef __cplusplus
}
#endif

#endif

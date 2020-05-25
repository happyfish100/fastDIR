
#ifndef _FDIR_CLIENT_PROTO_H
#define _FDIR_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fdir_types.h"

typedef struct fdir_client_dentry {
    int64_t inode;
    string_t name;
    //FDIRDStatus stat;
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
    short port;
} FDIRClientClusterStatEntry;

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_init_session(FDIRClientContext *client_ctx,
    FDIRClientSession *session);

void fdir_client_close_session(FDIRClientSession *session,
        const bool force_close);

int fdir_client_create_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const mode_t mode,
        FDIRDEntryInfo *dentry);

int fdir_client_create_dentry_by_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const mode_t mode, FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry_ex(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

static inline int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname)
{
    FDIRDEntryInfo dentry;
    return fdir_client_remove_dentry_ex(client_ctx,
            fullname, &dentry);
}

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

int fdir_client_lookup_inode(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, int64_t *inode);

int fdir_client_stat_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry_by_pname(FDIRClientContext *client_ctx,
        const FDIRDEntryPName *pname, FDIRDEntryInfo *dentry);

int fdir_client_set_dentry_size(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int64_t size,
        const int64_t inc_alloc, const bool force, FDIRDEntryInfo *dentry);

int fdir_client_modify_dentry_stat(FDIRClientContext *client_ctx,
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

int fdir_client_getlk_dentry(FDIRClientContext *client_ctx,
        const int64_t inode, int *operation, int64_t *offset,
        int64_t *length, int64_t *owner_id, pid_t *pid);

int fdir_client_dentry_sys_lock(FDIRClientSession *session,
        const int64_t inode, const int flags, int64_t *file_size);

int fdir_client_dentry_sys_unlock_ex(FDIRClientSession *session,
        const string_t *ns, const int64_t inode, const bool force,
        const int64_t old_size, const int64_t new_size,
        const int64_t inc_alloc);

static inline int fdir_client_dentry_sys_unlock(
        FDIRClientSession *session, const int64_t inode)
{
    return fdir_client_dentry_sys_unlock_ex(session, NULL, inode,
            false, 0, 0, 0);
}

int fdir_client_list_dentry_by_path(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array);

int fdir_client_list_dentry_by_inode(FDIRClientContext *client_ctx,
        const int64_t inode, FDIRClientDentryArray *array);

int fdir_client_dentry_array_init(FDIRClientDentryArray *array);

void fdir_client_dentry_array_free(FDIRClientDentryArray *array);

int fdir_client_service_stat(FDIRClientContext *client_ctx,
        const char *ip_addr, const int port, FDIRClientServiceStat *stat);

int fdir_client_cluster_stat(FDIRClientContext *client_ctx,
        FDIRClientClusterStatEntry *stats, const int size, int *count);

int fdir_client_get_master(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *master);

int fdir_client_get_slaves(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *slaves, const int size, int *count);

int fdir_client_get_readable_server(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *server);


static inline void fdir_log_network_error_ex(FDIRResponseInfo *response,
        const ConnectionInfo *conn, const int result, const int line)
{
    if (response->error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%d, sock fd: %d, %s", line,
                conn->ip_addr, conn->port, conn->sock,
                response->error.message);
    } else {
        logError("file: "__FILE__", line: %d, "
                "communicate with dir server %s:%d fail, "
                "sock fd: %d, errno: %d, error info: %s", line,
                conn->ip_addr, conn->port, conn->sock,
                result, STRERROR(result));
    }
}

#define fdir_log_network_error(response, conn, result) \
        fdir_log_network_error_ex(response, conn, result, __LINE__)


#ifdef __cplusplus
}
#endif

#endif

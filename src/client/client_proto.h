
#ifndef _FDIR_CLIENT_PROTO_H
#define _FDIR_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fdir_types.h"

typedef struct fdir_client_dentry {
    string_t name;
    FDIRDStatus stat;
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

int fdir_client_create_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const mode_t mode,
        FDIRDEntryInfo *dentry);

int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

int fdir_client_stat_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry);

int fdir_client_set_dentry_size(FDIRClientContext *client_ctx,
        const int64_t inode, const int64_t size, const bool force,
        FDIRDEntryInfo *dentry);

int fdir_client_list_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array);

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

#ifdef __cplusplus
}
#endif

#endif

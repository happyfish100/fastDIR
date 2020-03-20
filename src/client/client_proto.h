
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

int fdir_client_create_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryFullName *entry_info, const int flags,
        const mode_t mode);

int fdir_client_remove_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryFullName *entry_info);

int fdir_client_list_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryFullName *entry_info, FDIRClientDentryArray *array);

int fdir_client_dentry_array_init(FDIRClientDentryArray *array);

void fdir_client_dentry_array_free(FDIRClientDentryArray *array);

int fdir_client_service_stat(ConnectionInfo *conn, FDIRClientServiceStat *stat);

int fdir_client_cluster_stat(FDIRServerCluster *server_cluster,
        FDIRClientClusterStatEntry *stats, const int size, int *count);

#ifdef __cplusplus
}
#endif

#endif

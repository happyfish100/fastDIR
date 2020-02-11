
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
    //char fixed[16 * 1024]; //fixed buffer
    char fixed[16]; //fixed buffer
    char *buff;            //recv buffer
    char *current;
} FDIRClientBuffer;

typedef struct fdir_client_dentry_array {
    int alloc;
    int count;
    FDIRClientDentry *entries;
    FDIRClientBuffer buffer;
    struct fast_mpool_man mpool;
} FDIRClientDentryArray;

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_create_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryInfo *entry_info, const int flags,
        const mode_t mode);

int fdir_client_list_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryInfo *entry_info, FDIRClientDentryArray *array);

int fdir_client_dentry_array_init(FDIRClientDentryArray *array);

void fdir_client_dentry_array_free(FDIRClientDentryArray *array);

#ifdef __cplusplus
}
#endif

#endif

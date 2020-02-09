
#ifndef _FDIR_CLIENT_PROTO_H
#define _FDIR_CLIENT_PROTO_H

#include "fdir_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdir_client_create_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryInfo *entry_info, const int flags,
        const mode_t mode);

#ifdef __cplusplus
}
#endif

#endif

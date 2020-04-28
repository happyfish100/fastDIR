
#ifndef _FDIR_CLIENT_GLOBAL_H
#define _FDIR_CLIENT_GLOBAL_H

#include "fdir_global.h"
#include "client_types.h"

typedef struct fdir_client_global_vars {
    int connect_timeout;
    int network_timeout;
    char base_path[MAX_PATH_SIZE];

    FDIRClientContext client_ctx;
} FDIRClientGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRClientGlobalVars g_fdir_client_vars;

#ifdef __cplusplus
}
#endif

#endif

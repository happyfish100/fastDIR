
#ifndef _CLIENT_GLOBAL_H
#define _CLIENT_GLOBAL_H

#include "fdir_global.h"

typedef struct client_global_vars {
    int connect_timeout;
    int network_timeout;
    char base_path[MAX_PATH_SIZE];
} FDIRClientGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRClientGlobalVars g_client_global_vars;

#ifdef __cplusplus
}
#endif

#endif

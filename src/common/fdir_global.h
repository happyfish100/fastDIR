
#ifndef _FDIR_GLOBAL_H
#define _FDIR_GLOBAL_H

#include "fdir_types.h"

typedef struct fdir_global_vars {
    Version version;
    FDIRServerCluster server_cluster;
} FDIRGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRGlobalVars g_fdir_global_vars;

#ifdef __cplusplus
}
#endif

#endif


#ifndef _FDIR_CLIENT_FUNC_H
#define _FDIR_CLIENT_FUNC_H

#include "fdir_global.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fdir_client_init(filename) \
fdir_client_init_ex((&g_fdir_global_vars.server_cluster), filename)

#define fdir_client_destroy() \
    fdir_client_destroy_ex((&g_fdir_global_vars.server_cluster))

/**
* client initial from config file
* params:
*       server_cluster: tracker group
*   conf_filename: client config filename
* return: 0 success, !=0 fail, return the error code
**/
int fdir_client_init_ex(FDIRServerCluster *server_cluster,
        const char *conf_filename);

/**
* client destroy function
* params:
*       server_cluster: tracker group
* return: none
**/
void fdir_client_destroy_ex(FDIRServerCluster *server_cluster);

#ifdef __cplusplus
}
#endif

#endif

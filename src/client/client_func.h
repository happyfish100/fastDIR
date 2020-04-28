
#ifndef _FDIR_CLIENT_FUNC_H
#define _FDIR_CLIENT_FUNC_H

#include "fdir_global.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fdir_client_load_from_file(filename) \
    fdir_client_load_from_file_ex((&g_fdir_client_vars.client_ctx), filename)

#define fdir_client_init(filename) \
    fdir_client_init_ex((&g_fdir_client_vars.client_ctx), filename, NULL)

#define fdir_client_destroy() \
    fdir_client_destroy_ex((&g_fdir_client_vars.client_ctx))

/**
* client initial from config file
* params:
*       client_ctx: the client context
*       conf_filename: the client config filename
* return: 0 success, !=0 fail, return the error code
**/
int fdir_client_load_from_file_ex(FDIRClientContext *client_ctx,
        const char *conf_filename);

int fdir_client_init_ex(FDIRClientContext *client_ctx,
        const char *conf_filename, const FDIRConnectionManager *conn_manager);

/**
* client destroy function
* params:
*       client_ctx: tracker group
* return: none
**/
void fdir_client_destroy_ex(FDIRClientContext *client_ctx);


int fdir_alloc_group_servers(FDIRServerGroup *server_group,
        const int alloc_size);

#ifdef __cplusplus
}
#endif

#endif

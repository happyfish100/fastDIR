
#ifndef _FDIR_SERVER_FUNC_H
#define _FDIR_SERVER_FUNC_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int server_load_config(const char *filename);

    FDIRClusterServerInfo *fdir_get_server_by_id(const int server_id);

#ifdef __cplusplus
}
#endif

#endif


#ifndef _FDIR_SIMPLE_CONNECTION_MANAGER_H
#define _FDIR_SIMPLE_CONNECTION_MANAGER_H

#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdir_simple_connection_manager_init(FDIRConnectionManager *conn_manager);

void fdir_simple_connection_manager_destroy(FDIRConnectionManager *conn_manager);

#ifdef __cplusplus
}
#endif

#endif

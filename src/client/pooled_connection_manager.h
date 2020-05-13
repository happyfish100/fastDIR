#ifndef _FDIR_POOLED_CONNECTION_MANAGER_H
#define _FDIR_POOLED_CONNECTION_MANAGER_H

#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdir_pooled_connection_manager_init(FDIRConnectionManager *conn_manager,
        const int max_count_per_entry, const int max_idle_time);

void fdir_pooled_connection_manager_destroy(FDIRConnectionManager *conn_manager);

#ifdef __cplusplus
}
#endif

#endif

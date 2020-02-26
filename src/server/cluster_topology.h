//cluster_topology.h

#ifndef _CLUSTER_TOPOLOGY_H_
#define _CLUSTER_TOPOLOGY_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_top_init();
int cluster_top_destroy();

int ct_init_slave_array(FDIRServerSlaveArray *slaves);

static inline void ct_add_to_slave_array(FDIRServerSlaveArray *slaves,
        FDIRClusterServerInfo *server)
{
    slaves->servers[slaves->count++] = server;
}

static inline void ct_clear_slave_array(FDIRServerSlaveArray *slaves)
{
    slaves->count = 0;
}

void ct_reset_slave_arrays();

void ct_slave_server_online(FDIRClusterServerInfo *server);

void ct_slave_server_offline(FDIRClusterServerInfo *server);

#ifdef __cplusplus
}
#endif

#endif

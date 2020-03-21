//cluster_relationship.h

#ifndef _CLUSTER_RELATIONSHIP_H_
#define _CLUSTER_RELATIONSHIP_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"
#include "cluster_info.h"

#ifdef __cplusplus
extern "C" {
#endif

extern FDIRClusterServerInfo *g_next_master;

int cluster_relationship_init();
int cluster_relationship_destroy();

int cluster_relationship_pre_set_master(FDIRClusterServerInfo *master);

int cluster_relationship_commit_master(FDIRClusterServerInfo *master);

void cluster_relationship_trigger_reselect_master();

#ifdef __cplusplus
}
#endif

#endif

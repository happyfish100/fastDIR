//cluster_relationship.h

#ifndef _CLUSTER_RELATIONSHIP_H_
#define _CLUSTER_RELATIONSHIP_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_relationship_init();
int cluster_relationship_destroy();

int cluster_relationship_pre_set_master(FDIRClusterServerInfo *master);

int cluster_relationship_commit_master(FDIRClusterServerInfo *master,
        const bool master_self);

#ifdef __cplusplus
}
#endif

#endif

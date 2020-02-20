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

void cluster_relationship_set_master(FCServerInfo *master,
        const bool master_self);

#ifdef __cplusplus
}
#endif

#endif

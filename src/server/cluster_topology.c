#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "server_global.h"
#include "cluster_topology.h"

static pthread_mutex_t g_ct_lock;

int ct_init_slave_array(FDIRServerSlaveArray *slaves)
{
    int bytes;

    slaves->count = 0;
    bytes = sizeof(FDIRClusterServerInfo *) * CLUSTER_SERVER_ARRAY.count;
    slaves->servers = (FDIRClusterServerInfo **)malloc(bytes);
    if (slaves->servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    return 0;
}

int ct_remove_from_slave_array(FDIRServerSlaveArray *slaves, FDIRClusterServerInfo *server)
{
    int i;
    for (i=0; i<slaves->count; i++) {
        if (slaves->servers[i] == server) {
            break;
        }
    }

    if (i == slaves->count) {
        return ENOENT;
    }

    while (++i < slaves->count) {
        slaves->servers[i - 1] = slaves->servers[i];
    }

    slaves->count--;
    return 0;
}

void ct_reset_slave_arrays()
{
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;

    pthread_mutex_lock(&g_ct_lock);
    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++) {
        if (cs != CLUSTER_MYSELF_PTR) {
            ct_add_to_slave_array(&CLUSTER_INACTIVE_SLAVES, cs);
        }
    }

    ct_clear_slave_array(&CLUSTER_ACTIVE_SLAVES);

    logInfo("slave active count: %d, inactive count: %d",
            CLUSTER_ACTIVE_SLAVES.count, CLUSTER_INACTIVE_SLAVES.count);

    pthread_mutex_unlock(&g_ct_lock);
}

void ct_slave_server_online(FDIRClusterServerInfo *cs)
{
    pthread_mutex_lock(&g_ct_lock);
    if (ct_remove_from_slave_array(&CLUSTER_INACTIVE_SLAVES, cs) == 0) {
        ct_add_to_slave_array(&CLUSTER_ACTIVE_SLAVES, cs);

        logInfo("slave server [%d] %s:%d online, slave active count: %d, "
                "inactive count: %d", cs->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs->server),
                CLUSTER_ACTIVE_SLAVES.count,
                CLUSTER_INACTIVE_SLAVES.count);
    } else {
        logError("file: "__FILE__", line: %d, "
                "slave server [%d] %s:%d NOT in inactive server list", __LINE__,
                cs->server->id, CLUSTER_GROUP_ADDRESS_FIRST_IP(cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs->server));
    }
    pthread_mutex_unlock(&g_ct_lock);
}

void ct_slave_server_offline(FDIRClusterServerInfo *cs)
{
    pthread_mutex_lock(&g_ct_lock);
    if (ct_remove_from_slave_array(&CLUSTER_ACTIVE_SLAVES, cs) == 0) {
        ct_add_to_slave_array(&CLUSTER_INACTIVE_SLAVES, cs);

        logInfo("slave server [%d] %s:%d offline, slave active count: %d, "
                "inactive count: %d", cs->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs->server),
                CLUSTER_ACTIVE_SLAVES.count,
                CLUSTER_INACTIVE_SLAVES.count);
    } else {
        logError("file: "__FILE__", line: %d, "
                "slave server [%d] %s:%d NOT in active server list", __LINE__,
                cs->server->id, CLUSTER_GROUP_ADDRESS_FIRST_IP(cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs->server));
    }
    pthread_mutex_unlock(&g_ct_lock);
}

int cluster_top_init()
{
    int result;

    if ((result=init_pthread_lock(&g_ct_lock)) != 0) {
        return result;
    }

	return 0;
}

int cluster_top_destroy()
{
	return 0;
}

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
    bytes = sizeof(FCServerInfo *) * FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX);
    slaves->servers = (FCServerInfo **)malloc(bytes);
    if (slaves->servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    return 0;
}

int ct_remove_from_slave_array(FDIRServerSlaveArray *slaves, FCServerInfo *server)
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
    FCServerInfo *server;
    FCServerInfo *send;

    pthread_mutex_lock(&g_ct_lock);
    send = FC_SID_SERVERS(CLUSTER_CONFIG_CTX) +
        FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX);
    for (server=FC_SID_SERVERS(CLUSTER_CONFIG_CTX); server<send; server++) {
        if (server != CLUSTER_MYSELF_PTR) {
            ct_add_to_slave_array(&CLUSTER_INACTIVE_SLAVES, server);
        }
    }

    ct_clear_slave_array(&CLUSTER_ACTIVE_SLAVES);

    logInfo("slave active count: %d, inactive count: %d",
            CLUSTER_ACTIVE_SLAVES.count, CLUSTER_INACTIVE_SLAVES.count);

    pthread_mutex_unlock(&g_ct_lock);
}

void ct_slave_server_online(FCServerInfo *server)
{
    pthread_mutex_lock(&g_ct_lock);
    if (ct_remove_from_slave_array(&CLUSTER_INACTIVE_SLAVES, server) == 0) {
        ct_add_to_slave_array(&CLUSTER_ACTIVE_SLAVES, server);

        logInfo("slave server [%d] %s:%d online, slave active count: %d, "
                "inactive count: %d", server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(server),
                CLUSTER_ACTIVE_SLAVES.count,
                CLUSTER_INACTIVE_SLAVES.count);
    } else {
        logError("file: "__FILE__", line: %d, "
                "slave server [%d] %s:%d NOT in inactive server list",
                __LINE__, server->id, CLUSTER_GROUP_ADDRESS_FIRST_IP(server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(server));
    }
    pthread_mutex_unlock(&g_ct_lock);
}

void ct_slave_server_offline(FCServerInfo *server)
{
    pthread_mutex_lock(&g_ct_lock);
    if (ct_remove_from_slave_array(&CLUSTER_ACTIVE_SLAVES, server) == 0) {
        ct_add_to_slave_array(&CLUSTER_INACTIVE_SLAVES, server);

        logInfo("slave server [%d] %s:%d offline, slave active count: %d, "
                "inactive count: %d", server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(server),
                CLUSTER_ACTIVE_SLAVES.count,
                CLUSTER_INACTIVE_SLAVES.count);
    } else {
        logError("file: "__FILE__", line: %d, "
                "slave server [%d] %s:%d NOT in active server list",
                __LINE__, server->id, CLUSTER_GROUP_ADDRESS_FIRST_IP(server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(server));
    }
    pthread_mutex_unlock(&g_ct_lock);
}

static int ct_master_check_brain_split()
{
    return 0;
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

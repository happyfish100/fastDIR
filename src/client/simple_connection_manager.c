#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

static inline int make_connection(ConnectionInfo *conn)
{
    if (conn->sock >= 0) {
        return 0;
    }

    return conn_pool_connect_server(conn, g_client_global_vars.
            network_timeout);
}

static ConnectionInfo *get_connection(FDIRClientContext *client_ctx,
        int *err_no)
{
    int index;
    int i;
    ConnectionInfo *conn;

    index = rand() % client_ctx->server_group.count;
    conn = client_ctx->server_group.servers + index;
    if ((*err_no=make_connection(conn)) == 0) {
        return conn;
    }

    i = (index + 1) % client_ctx->server_group.count;
    while (i != index) {
        conn = client_ctx->server_group.servers + i;
        if ((*err_no=make_connection(conn)) == 0) {
            return conn;
        }

        i = (i + 1) % client_ctx->server_group.count;
    }

    logError("file: "__FILE__", line: %d, "
            "get_connection fail", __LINE__);
    return NULL;
}

static int check_realloc_group_servers(FDIRServerGroup *server_group)
{
    int bytes;
    int alloc_size;
    ConnectionInfo *servers;

    if (server_group->alloc_size > server_group->count) {
        return 0;
    }

    if (server_group->alloc_size > 0) {
        alloc_size = server_group->alloc_size * 2;
    } else {
        alloc_size = 4;
    }
    bytes = sizeof(ConnectionInfo) * alloc_size;
    servers = (ConnectionInfo *)malloc(bytes);
    if (servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return errno != 0 ? errno : ENOMEM;
    }
    memset(servers, 0, bytes);

    if (server_group->count > 0) {
        memcpy(servers, server_group->servers,
                sizeof(ConnectionInfo) * server_group->count);
    }

    server_group->servers = servers;
    server_group->alloc_size = alloc_size;
    return 0;
}

static ConnectionInfo *get_cluster_connection(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *server)
{
    FDIRServerGroup *cluster_sarray;
    ConnectionInfo *conn;
    ConnectionInfo *end;

    cluster_sarray = (FDIRServerGroup *)client_ctx->conn_manager.args;
    end = cluster_sarray->servers + cluster_sarray->count;
    for (conn=cluster_sarray->servers; conn<end; conn++) {
        if (strcmp(conn->ip_addr, server->ip_addr) == 0 &&
                conn->port == server->port)
        {
            return conn;
        }
    }

    if (check_realloc_group_servers(cluster_sarray) != 0) {
        return NULL;
    }

    conn = cluster_sarray->servers + cluster_sarray->count++;
    conn_pool_set_server_info(conn, server->ip_addr, server->port);
    return conn;
}

static ConnectionInfo *get_master_connection(FDIRClientContext *client_ctx,
        int *err_no)
{
    ConnectionInfo *conn; 
    ConnectionInfo *mconn; 
    FDIRClientServerEntry master;

    do {
        if ((conn=get_connection(client_ctx, err_no)) == NULL) {
            break;
        }

        if ((*err_no=fdir_client_get_master(client_ctx, &master)) != 0) {
            break;
        }

        if ((mconn=get_cluster_connection(client_ctx, &master)) == NULL) {
            *err_no = ENOMEM;
            break;
        }
        if (mconn == conn) {
            return mconn;
        }

        if ((*err_no=make_connection(mconn)) != 0) {
            break;
        }

        return mconn;
    } while (0);

    logError("file: "__FILE__", line: %d, "
            "get_master_connection fail, errno: %d",
            __LINE__, *err_no);
    return NULL;
}

static ConnectionInfo *get_readable_connection(
        FDIRClientContext *client_ctx, int *err_no)
{
    ConnectionInfo *conn; 
    ConnectionInfo *sconn; 
    FDIRClientServerEntry server;

    do {
        if ((conn=get_connection(client_ctx, err_no)) == NULL) {
            break;
        }

        if ((*err_no=fdir_client_get_readable_server(
                        client_ctx, &server)) != 0)
        {
            break;
        }

        if ((sconn=get_cluster_connection(client_ctx, &server)) == NULL) {
            *err_no = ENOMEM;
            break;
        }
        if (sconn == conn) {
            return sconn;
        }

        if ((*err_no=make_connection(sconn)) != 0) {
            break;
        }

        return sconn;
    } while (0);

    logError("file: "__FILE__", line: %d, "
            "get_readable_connection fail, errno: %d",
            __LINE__, *err_no);
        return NULL;
}

int fdir_simple_connection_manager_init(FDIRConnectionManager *conn_manager)
{
    FDIRServerGroup *cluster_sarray;
    int result;

    cluster_sarray = (FDIRServerGroup *)malloc(sizeof(FDIRServerGroup));
    if (cluster_sarray == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__,
                (int)sizeof(FDIRServerGroup));
        return ENOMEM;
    }

    if ((result=fdir_alloc_group_servers(cluster_sarray, 4)) != 0) {
        return result;
    }

    conn_manager->args = cluster_sarray;
    conn_manager->get_connection = get_connection;
    conn_manager->get_master_connection = get_master_connection;
    conn_manager->get_readable_connection = get_readable_connection;

    conn_manager->release_connection = NULL;
    conn_manager->close_connection = conn_pool_disconnect_server;
    return 0;
}

void fdir_simple_connection_manager_destroy(FDIRConnectionManager *conn_manager)
{
    FDIRServerGroup *cluster_sarray;

    if (conn_manager->args != NULL) {
        cluster_sarray = (FDIRServerGroup *)conn_manager->args;
        if (cluster_sarray->servers != NULL) {
            free(cluster_sarray->servers);
            cluster_sarray->servers = NULL;
            cluster_sarray->count = cluster_sarray->alloc_size = 0;
        }

        free(cluster_sarray);
        conn_manager->args = NULL;
    }
}

#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fdir_proto.h"
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

static ConnectionInfo *get_spec_connection(FDIRClientContext *client_ctx,
        const ConnectionInfo *target, int *err_no)
{
    return conn_pool_get_connection((ConnectionPool *)client_ctx->
            conn_manager.args, target, err_no);
}

static ConnectionInfo *get_connection(FDIRClientContext *client_ctx,
        int *err_no)
{
    int index;
    int i;
    ConnectionInfo *server;
    ConnectionInfo *conn;

    index = rand() % client_ctx->server_group.count;
    server = client_ctx->server_group.servers + index;
    if ((conn=get_spec_connection(client_ctx, server, err_no)) != NULL) {
        return conn;
    }

    i = (index + 1) % client_ctx->server_group.count;
    while (i != index) {
        server = client_ctx->server_group.servers + i;
        if ((conn=get_spec_connection(client_ctx, server, err_no)) != NULL) {
            return conn;
        }

        i = (i + 1) % client_ctx->server_group.count;
    }

    logError("file: "__FILE__", line: %d, "
            "get_connection fail, configured server count: %d",
            __LINE__, client_ctx->server_group.count);
    return NULL;
}

static ConnectionInfo *get_master_connection(FDIRClientContext *client_ctx,
        int *err_no)
{
    ConnectionInfo *conn;
    ConnectionInfo mconn;
    FDIRClientServerEntry master;

    mconn = *(client_ctx->conn_manager.master_cache.conn);
    if (mconn.port > 0) {
        return get_spec_connection(client_ctx, &mconn, err_no);
    }

    do {
        if ((*err_no=fdir_client_get_master(client_ctx, &master)) != 0) {
            break;
        }

        if ((conn=get_spec_connection(client_ctx, &master.conn,
                        err_no)) == NULL)
        {
            break;
        }

        conn_pool_set_server_info(client_ctx->conn_manager.
                master_cache.conn, conn->ip_addr, conn->port);
        return conn;
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
    FDIRClientServerEntry server;

    do {
        if ((*err_no=fdir_client_get_readable_server(
                        client_ctx, &server)) != 0)
        {
            break;
        }

        if ((conn=get_spec_connection(client_ctx, &server.conn,
                        err_no)) == NULL)
        {
            break;
        }

        return conn;
    } while (0);

    logError("file: "__FILE__", line: %d, "
            "get_readable_connection fail, errno: %d",
            __LINE__, *err_no);
        return NULL;
}

static void release_connection(FDIRClientContext *client_ctx,
        ConnectionInfo *conn)
{
    conn_pool_close_connection_ex((ConnectionPool *)client_ctx->
            conn_manager.args, conn, false);
}

static void close_connection(FDIRClientContext *client_ctx,
        ConnectionInfo *conn)
{
    if (client_ctx->conn_manager.master_cache.conn->port == conn->port &&
            strcmp(client_ctx->conn_manager.master_cache.conn->ip_addr,
                conn->ip_addr) == 0)
    {
        client_ctx->conn_manager.master_cache.conn->port = 0;
    }

    conn_pool_close_connection_ex((ConnectionPool *)client_ctx->
            conn_manager.args, conn, true);
}

static int validate_connection_callback(ConnectionInfo *conn, void *args)
{
    FDIRResponseInfo response;
    int result;
    if ((result=fdir_active_test(conn, &response, g_fdir_client_vars.
                    network_timeout)) != 0)
    {
        fdir_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_pooled_connection_manager_init(FDIRConnectionManager *conn_manager,
        const int max_count_per_entry, const int max_idle_time)
{
    const int socket_domain = AF_INET;
    const int htable_init_capacity = 128;
    ConnectionPool *cp;
    int result;

    cp = (ConnectionPool *)malloc(sizeof(ConnectionPool));
    if (cp == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__,
                (int)sizeof(ConnectionPool));
        return ENOMEM;
    }

    if ((result=conn_pool_init_ex1(cp, g_fdir_client_vars.connect_timeout,
                    max_count_per_entry, max_idle_time, socket_domain,
                    htable_init_capacity, NULL, NULL,
                    validate_connection_callback, NULL, 0)) != 0)
    {
        return result;
    }

    conn_manager->args = cp;
    conn_manager->get_connection = get_connection;
    conn_manager->get_spec_connection = get_spec_connection;
    conn_manager->get_master_connection = get_master_connection;
    conn_manager->get_readable_connection = get_readable_connection;

    conn_manager->release_connection = release_connection;
    conn_manager->close_connection = close_connection;
    conn_manager->master_cache.conn = &conn_manager->master_cache.holder;
    conn_pool_set_server_info(conn_manager->master_cache.conn, "", 0);
    return 0;
}

void fdir_pooled_connection_manager_destroy(FDIRConnectionManager *conn_manager)
{
    ConnectionPool *cp;

    if (conn_manager->args != NULL) {
        cp = (ConnectionPool *)conn_manager->args;
        conn_pool_destroy(cp);
        free(cp);
        conn_manager->args = NULL;
    }
}

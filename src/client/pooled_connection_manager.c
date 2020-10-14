/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/idempotency/client/client_channel.h"
#include "fdir_proto.h"
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

static ConnectionInfo *get_spec_connection(FDIRClientContext *client_ctx,
        const ConnectionInfo *target, int *err_no)
{
    return conn_pool_get_connection((ConnectionPool *)client_ctx->
            conn_manager.args[0], target, err_no);
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

#define CM_MASTER_CACHE_MUTEX_LOCK(client_ctx) \
    PTHREAD_MUTEX_LOCK(&(client_ctx->conn_manager.master_cache.lock))

#define CM_MASTER_CACHE_MUTEX_UNLOCK(client_ctx) \
    PTHREAD_MUTEX_UNLOCK(&(client_ctx->conn_manager.master_cache.lock))

static ConnectionInfo *get_master_connection(FDIRClientContext *client_ctx,
        int *err_no)
{
    ConnectionInfo *conn;
    ConnectionInfo mconn;
    FDIRClientServerEntry master;
    SFNetRetryIntervalContext net_retry_ctx;
    int i;

    CM_MASTER_CACHE_MUTEX_LOCK(client_ctx);
    mconn = *(client_ctx->conn_manager.master_cache.conn);
    CM_MASTER_CACHE_MUTEX_UNLOCK(client_ctx);
    if (mconn.port > 0) {
        if ((conn=get_spec_connection(client_ctx, &mconn, err_no)) != NULL) {
            return conn;
        }
    }

    sf_init_net_retry_interval_context(&net_retry_ctx,
            &client_ctx->net_retry_cfg.interval_mm,
            &client_ctx->net_retry_cfg.connect);
    i = 0;
    while (1) {
        do {
            if ((*err_no=fdir_client_get_master(client_ctx, &master)) != 0) {
                break;
            }

            if ((conn=get_spec_connection(client_ctx, &master.conn,
                            err_no)) == NULL)
            {
                break;
            }

            CM_MASTER_CACHE_MUTEX_LOCK(client_ctx);
            conn_pool_set_server_info(client_ctx->conn_manager.
                    master_cache.conn, conn->ip_addr, conn->port);
            CM_MASTER_CACHE_MUTEX_UNLOCK(client_ctx);
            return conn;
        } while (0);

        SF_NET_RETRY_CHECK_AND_SLEEP(net_retry_ctx,
                client_ctx->net_retry_cfg.
                connect.times, ++i, *err_no);
    }

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
    SFNetRetryIntervalContext net_retry_ctx;
    int i;

    sf_init_net_retry_interval_context(&net_retry_ctx,
            &client_ctx->net_retry_cfg.interval_mm,
            &client_ctx->net_retry_cfg.connect);
    i = 0;
    while (1) {
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

        SF_NET_RETRY_CHECK_AND_SLEEP(net_retry_ctx,
                client_ctx->net_retry_cfg.
                connect.times, ++i, *err_no);
    }

    logError("file: "__FILE__", line: %d, "
            "get_readable_connection fail, errno: %d",
            __LINE__, *err_no);
        return NULL;
}

static void release_connection(FDIRClientContext *client_ctx,
        ConnectionInfo *conn)
{
    conn_pool_close_connection_ex((ConnectionPool *)client_ctx->
            conn_manager.args[0], conn, false);
}

static void close_connection(FDIRClientContext *client_ctx,
        ConnectionInfo *conn)
{
    CM_MASTER_CACHE_MUTEX_LOCK(client_ctx);
    if (client_ctx->conn_manager.master_cache.conn->port == conn->port &&
            strcmp(client_ctx->conn_manager.master_cache.conn->ip_addr,
                conn->ip_addr) == 0)
    {
        client_ctx->conn_manager.master_cache.conn->port = 0;
    }
    CM_MASTER_CACHE_MUTEX_UNLOCK(client_ctx);

    conn_pool_close_connection_ex((ConnectionPool *)client_ctx->
            conn_manager.args[0], conn, true);
}

static const struct fdir_connection_parameters *get_connection_params(
        struct fdir_client_context *client_ctx, ConnectionInfo *conn)
{
    return (FDIRConnectionParameters *)conn->args;
}

static int connect_done_callback(ConnectionInfo *conn, void *args)
{
    FDIRConnectionParameters *params;
    int result;

    params = (FDIRConnectionParameters *)conn->args;
    if (((FDIRClientContext *)args)->idempotency_enabled) {
        params->channel = idempotency_client_channel_get(conn->ip_addr,
                conn->port, ((FDIRClientContext *)args)->connect_timeout,
                &result);
        if (params->channel == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "server %s:%u, idempotency channel get fail, "
                    "result: %d, error info: %s", __LINE__, conn->ip_addr,
                    conn->port, result, STRERROR(result));
            return result;
        }
    } else {
        params->channel = NULL;
    }

    result = fdir_client_proto_join_server((FDIRClientContext *)args, conn, params);
    if (result == SF_RETRIABLE_ERROR_NO_CHANNEL && params->channel != NULL) {
        idempotency_client_channel_check_reconnect(params->channel);
    }
    return result;
}

static int validate_connection_callback(ConnectionInfo *conn, void *args)
{
    SFResponseInfo response;
    int result;
    if ((result=sf_active_test(conn, &response, ((FDIRClientContext *)
                        args)->network_timeout)) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_pooled_connection_manager_init(FDIRClientContext *client_ctx,
        FDIRConnectionManager *conn_manager,
        const int max_count_per_entry, const int max_idle_time)
{
    const int socket_domain = AF_INET;
    const int htable_init_capacity = 128;
    ConnectionPool *cp;
    int result;

    cp = (ConnectionPool *)fc_malloc(sizeof(ConnectionPool));
    if (cp == NULL) {
        return ENOMEM;
    }

    if ((result=conn_pool_init_ex1(cp, client_ctx->connect_timeout,
                    max_count_per_entry, max_idle_time, socket_domain,
                    htable_init_capacity, connect_done_callback, client_ctx,
                    validate_connection_callback, client_ctx,
                    sizeof(FDIRConnectionParameters))) != 0)
    {
        return result;
    }

    conn_manager->args[0] = cp;
    conn_manager->args[1] = NULL;
    conn_manager->get_connection = get_connection;
    conn_manager->get_spec_connection = get_spec_connection;
    conn_manager->get_master_connection = get_master_connection;
    conn_manager->get_readable_connection = get_readable_connection;

    conn_manager->release_connection = release_connection;
    conn_manager->close_connection = close_connection;
    conn_manager->get_connection_params = get_connection_params;

    conn_manager->master_cache.conn = &conn_manager->master_cache.holder;
    if ((result=init_pthread_lock(&conn_manager->master_cache.lock)) != 0) {
        return result;
    }

    conn_pool_set_server_info(conn_manager->master_cache.conn, "", 0);
    return 0;
}

void fdir_pooled_connection_manager_destroy(FDIRConnectionManager *conn_manager)
{
    ConnectionPool *cp;

    if (conn_manager->args[0] != NULL) {
        cp = (ConnectionPool *)conn_manager->args[0];
        conn_pool_destroy(cp);
        free(cp);
        conn_manager->args[0] = NULL;
    }
}

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
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

typedef struct fdir_cm_simple_extra {
    /* master connection cache */
    struct {
        ConnectionInfo *conn;
    } master_cache;
    FDIRClientContext *client_ctx;
    FDIRServerGroup *cluster_sarray;
    ConnectionExtraParams conn_extra_params;  //for RDMA
} FDIRCMSimpleExtra;

static inline int make_connection(SFConnectionManager *cm,
        ConnectionInfo *conn)
{
    const char *service_name = "fdir";
    int result;
    SFConnectionParameters conn_params;
    FDIRCMSimpleExtra *extra;

    if (G_COMMON_CONNECTION_CALLBACKS[conn->comm_type].is_connected(conn)) {
        return 0;
    }

    if ((result=G_COMMON_CONNECTION_CALLBACKS[conn->comm_type].
                make_connection(conn, service_name, cm->common_cfg->
                    connect_timeout * 1000, NULL, true)) != 0)
    {
        return result;
    }

    extra = (FDIRCMSimpleExtra *)cm->extra;
    if ((result=fdir_client_proto_join_server(extra->
                    client_ctx, conn, &conn_params)) != 0)
    {
        G_COMMON_CONNECTION_CALLBACKS[conn->comm_type].
            close_connection(conn);
    }

    return result;
}

static int check_realloc_group_servers(FDIRServerGroup *server_group)
{
    int bytes;
    int alloc_size;
    ConnectionInfo **servers;

    if (server_group->alloc_size > server_group->count) {
        return 0;
    }

    if (server_group->alloc_size > 0) {
        alloc_size = server_group->alloc_size * 2;
    } else {
        alloc_size = 4;
    }
    bytes = sizeof(ConnectionInfo *) * alloc_size;
    servers = (ConnectionInfo **)fc_malloc(bytes);
    if (servers == NULL) {
        return errno != 0 ? errno : ENOMEM;
    }
    memset(servers, 0, bytes);

    if (server_group->count > 0) {
        memcpy(servers, server_group->servers,
                sizeof(ConnectionInfo *) * server_group->count);
    }

    server_group->servers = servers;
    server_group->alloc_size = alloc_size;
    return 0;
}

static ConnectionInfo **add_to_server_list(FDIRCMSimpleExtra *extra,
        const ConnectionInfo *target, int *err_no)
{
    FDIRServerGroup *cluster_sarray;
    ConnectionInfo **conn;

    cluster_sarray = extra->cluster_sarray;
    conn = cluster_sarray->servers + cluster_sarray->count;
    if ((*conn=conn_pool_alloc_connection(target->comm_type, &extra->
                    conn_extra_params, err_no)) == NULL)
    {
        return NULL;
    }

    conn_pool_set_server_info(*conn, target->ip_addr, target->port);
    cluster_sarray->count++;
    return conn;
}

static ConnectionInfo *get_spec_connection(SFConnectionManager *cm,
        const ConnectionInfo *target, const bool shared, int *err_no)
{
    FDIRServerGroup *cluster_sarray;
    ConnectionInfo **conn;
    ConnectionInfo **end;

    cluster_sarray = ((FDIRCMSimpleExtra *)cm->extra)->cluster_sarray;
    end = cluster_sarray->servers + cluster_sarray->count;
    for (conn=cluster_sarray->servers; conn<end; conn++) {
        if (FC_CONNECTION_SERVER_EQUAL1(**conn, *target)) {
            break;
        }
    }

    if (conn == end) {
        if (check_realloc_group_servers(cluster_sarray) != 0) {
            *err_no = ENOMEM;
            return NULL;
        }

        if ((conn=add_to_server_list((FDIRCMSimpleExtra *)
                        cm->extra, target, err_no)) == NULL)
        {
            return NULL;
        }
    }

    if ((*err_no=make_connection(cm, *conn)) != 0) {
        return NULL;
    }
    return *conn;
}

static ConnectionInfo *get_connection(SFConnectionManager *cm,
        const int group_index, const bool shared, int *err_no)
{
    int index;
    int i;
    FDIRServerGroup *cluster_sarray;
    ConnectionInfo **server;
    ConnectionInfo *conn;

    cluster_sarray = ((FDIRCMSimpleExtra *)cm->extra)->cluster_sarray;
    index = rand() % cluster_sarray->count;
    server = cluster_sarray->servers + index;
    if ((conn=get_spec_connection(cm, *server, shared, err_no)) != NULL) {
        return conn;
    }

    i = (index + 1) % cluster_sarray->count;
    while (i != index) {
        server = cluster_sarray->servers + i;
        if ((conn=get_spec_connection(cm, *server, shared, err_no)) != NULL) {
            return conn;
        }

        i = (i + 1) % cluster_sarray->count;
    }

    logError("file: "__FILE__", line: %d, "
            "get_connection fail, configured server count: %d",
            __LINE__, cluster_sarray->count);
    return NULL;
}

static ConnectionInfo *get_master_connection(SFConnectionManager *cm,
        const int group_index, const bool shared, int *err_no)
{
    FDIRCMSimpleExtra *extra;
    ConnectionInfo *conn; 
    FDIRClientServerEntry master;
    SFNetRetryIntervalContext net_retry_ctx;
    int i;

    extra = (FDIRCMSimpleExtra *)cm->extra;
    if (extra->master_cache.conn != NULL) {
        return extra->master_cache.conn;
    }

    sf_init_net_retry_interval_context(&net_retry_ctx,
            &cm->common_cfg->net_retry_cfg.interval_mm,
            &cm->common_cfg->net_retry_cfg.connect);
    i = 0;
    while (1) {
        if ((*err_no=fdir_client_get_master(extra->
                        client_ctx, &master)) != 0)
        {
            SF_NET_RETRY_CHECK_AND_SLEEP(net_retry_ctx,
                    cm->common_cfg->net_retry_cfg.
                    connect.times, ++i, *err_no);
            continue;
        }

        if ((conn=get_spec_connection(cm, &master.conn,
                        shared, err_no)) == NULL)
        {
            break;
        }

        extra->master_cache.conn = conn;
        return conn;
    }

    logError("file: "__FILE__", line: %d, "
            "get_master_connection fail, errno: %d",
            __LINE__, *err_no);
    return NULL;
}

static ConnectionInfo *get_readable_connection(SFConnectionManager *cm,
        const int group_index, const bool shared, int *err_no)
{
    FDIRClientContext *client_ctx;
    ConnectionInfo *conn; 
    FDIRClientServerEntry server;
    SFNetRetryIntervalContext net_retry_ctx;
    int i;

    client_ctx = ((FDIRCMSimpleExtra *)cm->extra)->client_ctx;
    if (cm->common_cfg->read_rule == sf_data_read_rule_master_only) {
        return get_master_connection(cm, group_index, shared, err_no);
    }

    sf_init_net_retry_interval_context(&net_retry_ctx,
            &cm->common_cfg->net_retry_cfg.interval_mm,
            &cm->common_cfg->net_retry_cfg.connect);
    i = 0;
    while (1) {
        if ((*err_no=fdir_client_get_readable_server(
                        client_ctx, &server)) != 0)
        {
            SF_NET_RETRY_CHECK_AND_SLEEP(net_retry_ctx,
                    cm->common_cfg->net_retry_cfg.
                    connect.times, ++i, *err_no);
            continue;
        }

        if ((conn=get_spec_connection(cm, &server.conn,
                        shared, err_no)) == NULL)
        {
            break;
        }

        return conn;
    }

    logError("file: "__FILE__", line: %d, "
            "get_readable_connection fail, errno: %d",
            __LINE__, *err_no);
    return NULL;
}

static void close_connection(SFConnectionManager *cm, ConnectionInfo *conn)
{
    FDIRCMSimpleExtra *extra;
    extra = (FDIRCMSimpleExtra *)cm->extra;
    if (extra->master_cache.conn == conn) {
        extra->master_cache.conn = NULL;
    }

    G_COMMON_CONNECTION_CALLBACKS[conn->comm_type].
        close_connection(conn);
}

static const struct sf_connection_parameters *get_connection_params(
        SFConnectionManager *cm, ConnectionInfo *conn)
{
    return NULL;
}

static int copy_to_server_group_array(FDIRClientContext *client_ctx,
        FDIRCMSimpleExtra *extra)
{
    FCServerInfo *server;
    FCServerInfo *end;
    int server_count;
    int result;

    extra->cluster_sarray->count = 0;
    server_count = FC_SID_SERVER_COUNT(client_ctx->cluster.server_cfg);
    end = FC_SID_SERVERS(client_ctx->cluster.server_cfg) + server_count;
    for (server=FC_SID_SERVERS(client_ctx->cluster.server_cfg); server<end;
            server++)
    {
        if (add_to_server_list(extra, &server->group_addrs[client_ctx->
                    cluster.service_group_index].address_array.
                    addrs[0]->conn, &result) == NULL)
        {
            return result;
        }
    }

    /*
    {
        ConnectionInfo **conn;
        char formatted_ip[FORMATTED_IP_SIZE];
        printf("dir_server count: %d\n", extra->cluster_sarray->count);
        for (conn=extra->cluster_sarray->servers; conn<extra->cluster_sarray->
                servers + extra->cluster_sarray->count; conn++)
        {
            format_ip_address((*conn)->ip_addr, formatted_ip);
            printf("dir_server=%s:%u\n", formatted_ip, (*conn)->port);
        }
    }
    */

    return 0;
}

int fdir_simple_connection_manager_init(FDIRClientContext *client_ctx,
        SFConnectionManager *cm)
{
    FDIRCMSimpleExtra *extra;
    FDIRServerGroup *cluster_sarray;
    int server_count;
    int result;

    cluster_sarray = (FDIRServerGroup *)fc_malloc(sizeof(FDIRServerGroup));
    if (cluster_sarray == NULL) {
        return ENOMEM;
    }

    extra = (FDIRCMSimpleExtra *)fc_malloc(sizeof(FDIRCMSimpleExtra));
    if (extra == NULL) {
        return ENOMEM;
    }
    memset(extra, 0, sizeof(FDIRCMSimpleExtra));
    extra->cluster_sarray = cluster_sarray;
    extra->client_ctx = client_ctx;
    if ((result=conn_pool_set_rdma_extra_params(&extra->conn_extra_params,
                    &client_ctx->cluster.server_cfg, client_ctx->cluster.
                    service_group_index)) != 0)
    {
        return result;
    }

    server_count = FC_SID_SERVER_COUNT(client_ctx->cluster.server_cfg);
    if ((result=fdir_alloc_group_servers(cluster_sarray, server_count)) != 0) {
        return result;
    }
    copy_to_server_group_array(client_ctx, extra);

    cm->extra = extra;
    cm->common_cfg = &client_ctx->common_cfg;
    cm->ops.get_connection = get_connection;
    cm->ops.get_spec_connection = get_spec_connection;
    cm->ops.get_master_connection = get_master_connection;
    cm->ops.get_readable_connection = get_readable_connection;
    cm->ops.release_connection = NULL;
    cm->ops.close_connection = close_connection;
    cm->ops.get_connection_params = get_connection_params;
    return 0;
}

void fdir_simple_connection_manager_destroy(SFConnectionManager *cm)
{
    FDIRCMSimpleExtra *extra;

    extra = (FDIRCMSimpleExtra *)cm->extra;
    if (extra->cluster_sarray != NULL) {
        if (extra->cluster_sarray->servers != NULL) {
            free(extra->cluster_sarray->servers);
            extra->cluster_sarray->servers = NULL;
            extra->cluster_sarray->count = 0;
            extra->cluster_sarray->alloc_size = 0;
        }

        free(extra->cluster_sarray);
        extra->cluster_sarray = NULL;
    }
}

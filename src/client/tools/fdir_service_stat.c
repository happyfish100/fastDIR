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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "fastdir/client/fdir_client.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] "
            "server_id|host[:port]|all\n", argv[0],
            FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static void output(FDIRClientServiceStat *stat, const ConnectionInfo *conn)
{
    printf( "\tserver_id: %d\n"
            "\thost: %s:%u\n"
            "\tstatus: %d (%s)\n"
            "\tis_master: %s\n"
            "\tconnection : {current: %d, max: %d}\n"
            "\tbinlog : {current_version: %"PRId64, stat->server_id,
            conn->ip_addr, conn->port, stat->status,
            fdir_get_server_status_caption(stat->status),
            stat->is_master ? "true" : "false",
            stat->connection.current_count,
            stat->connection.max_count,
            stat->binlog.current_version);

    if (stat->is_master) {
        printf( ", writer: {next_version: %"PRId64", "
                "total_count: %"PRId64", "
                "waiting_count: %d, max_waitings: %d}",
                stat->binlog.writer.next_version,
                stat->binlog.writer.total_count,
                stat->binlog.writer.waiting_count,
                stat->binlog.writer.max_waitings);
    }

    printf( "}\n"
            "\tdentry : {current_inode_sn: %"PRId64", "
            "ns_count: %"PRId64", "
            "dir_count: %"PRId64", "
            "file_count: %"PRId64"}\n\n",
            stat->dentry.current_inode_sn,
            stat->dentry.counters.ns,
            stat->dentry.counters.dir,
            stat->dentry.counters.file);
}

static int service_stat_all()
{
    FCServerInfo *server;
    FCServerInfo *end;
    ConnectionInfo conn;
    FDIRClientServiceStat stat;
	int result;

    end = FC_SID_SERVERS(g_fdir_client_vars.client_ctx.cluster.server_cfg) +
        FC_SID_SERVER_COUNT(g_fdir_client_vars.client_ctx.cluster.server_cfg);
    for (server=FC_SID_SERVERS(g_fdir_client_vars.client_ctx.
                cluster.server_cfg); server<end; server++)
    {
        conn = server->group_addrs[g_fdir_client_vars.client_ctx.cluster.
            service_group_index].address_array.addrs[0]->conn;
        conn.sock = -1;
        if ((result=fdir_client_service_stat(&g_fdir_client_vars.
                        client_ctx, &conn, &stat)) != 0)
        {
            return result;
        }

        output(&stat, &conn);
    }

    return 0;
}

static int service_stat(char *host)
{
    ConnectionInfo conn;
    FDIRClientServiceStat stat;
    FCServerInfo *server;
	int result;
    int server_id;

    if (strcmp(host, "all") == 0) {
        return service_stat_all();
    }

    if (is_digital_string(host)) {
        server_id = strtol(host, NULL, 10);
        if ((server=fc_server_get_by_id(&g_fdir_client_vars.client_ctx.
                        cluster.server_cfg, server_id)) == NULL)
        {
            fprintf(stderr, "server id: %d not exist\n", server_id);
            return ENOENT;
        }

        conn = server->group_addrs[g_fdir_client_vars.client_ctx.cluster.
            service_group_index].address_array.addrs[0]->conn;
        conn.sock = -1;
    } else {
        if ((result=conn_pool_parse_server_info(host, &conn,
                        FDIR_SERVER_DEFAULT_SERVICE_PORT)) != 0)
        {
            return result;
        }
    }


    if ((result=fdir_client_service_stat(&g_fdir_client_vars.
                    client_ctx, &conn, &stat)) != 0)
    {
        return result;
    }

    output(&stat, &conn);
    return 0;
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    char *host;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    while ((ch=getopt(argc, argv, "hc:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            default:
                usage(argv);
                return 1;
        }
    }


    if (optind >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    if ((result=fdir_client_simple_init_with_auth(
                    config_filename, publish)) != 0)
    {
        return result;
    }

    host = argv[optind];
    return service_stat(host);
}

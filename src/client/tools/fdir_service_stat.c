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
            "host[:port]\n", argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static void output(FDIRClientServiceStat *stat)
{
    printf( "\tserver_id: %d\n"
            "\tstatus: %d (%s)\n"
            "\tis_master: %s\n"
            "\tconnection : {current: %d, max: %d}\n"
            "\tbinlog : {current_version: %"PRId64,
            stat->server_id, stat->status,
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

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    char *host;
    ConnectionInfo conn;
    FDIRClientServiceStat stat;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    while ((ch=getopt(argc, argv, "hc:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                break;
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

    host = argv[optind];
    if ((result=conn_pool_parse_server_info(host, &conn,
                    FDIR_SERVER_DEFAULT_SERVICE_PORT)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_simple_init_with_auth(
                    config_filename, publish)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_service_stat(&g_fdir_client_vars.
                    client_ctx, &conn, &stat)) != 0)
    {
        return result;
    }

    output(&stat);
    return 0;
}

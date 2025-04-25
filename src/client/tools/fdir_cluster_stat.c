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
    fprintf(stderr, "Usage: %s [-c config_filename=%s]\n"
            "\t[-A] for ACTIVE status only\n"
            "\t[-N] for None ACTIVE status\n"
            "\t[-M] for master only\n"
            "\t[-S] for slave only\n\n",
            argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static void output(FDIRClientClusterStatEntry *stats, const int count)
{
    FDIRClientClusterStatEntry *stat;
    FDIRClientClusterStatEntry *end;
    char formatted_ip[FORMATTED_IP_SIZE];

    end = stats + count;
    for (stat=stats; stat<end; stat++) {
        format_ip_address(stat->ip_addr, formatted_ip);
        printf( "server_id: %d, host: %s:%u, "
                "status: %d (%s), "
                "is_master: %d, "
                "data_version: %"PRId64"\n",
                stat->server_id,
                formatted_ip, stat->port,
                stat->status,
                fdir_get_server_status_caption(stat->status),
                stat->is_master, stat->confirmed_data_version
              );
    }
    if (count > 0) {
        printf("\nserver count: %d\n\n", count);
    }
}

int main(int argc, char *argv[])
{
#define CLUSTER_MAX_SERVER_COUNT  16
    const bool publish = false;
	int ch;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
    int count;
    FDIRClusterStatFilter filter;
    FDIRClientClusterStatEntry stats[CLUSTER_MAX_SERVER_COUNT];
	int result;

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    memset(&filter, 0, sizeof(filter));
    while ((ch=getopt(argc, argv, "hc:ANMS")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            case 'A':
            case 'N':
                filter.filter_by |= FDIR_CLUSTER_STAT_FILTER_BY_STATUS;
                filter.op_type = (ch == 'A' ? '=' : '!');
                filter.status = FDIR_SERVER_STATUS_ACTIVE;
                break;
            case 'M':
            case 'S':
                filter.filter_by |= FDIR_CLUSTER_STAT_FILTER_BY_IS_MASTER;
                filter.is_master = (ch == 'M');
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if ((result=fdir_client_simple_init_with_auth(
                    config_filename, publish)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_cluster_stat(&g_fdir_client_vars.client_ctx,
                    &filter, stats, CLUSTER_MAX_SERVER_COUNT, &count)) != 0)
    {
        fprintf(stderr, "fdir_client_cluster_stat fail, "
                "errno: %d, error info: %s\n", result, STRERROR(result));
        return result;
    }

    output(stats, count);
    return 0;
}

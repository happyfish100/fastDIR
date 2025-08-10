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

static bool include_inode_space = false;

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [options] <server_id|host[:port]|all>\n"
            "    options:\n"
            "\t-c <client config filename>: default %s\n"
            "\t-I: used space include spaces occupied by "
            "inodes when storage engine enabled\n"
            "\t-h: help for usage\n\n"
            "    eg: %s -c %s all\n\n",
            argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME,
            argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static void output(FDIRClientServiceStat *stat, const ConnectionInfo *conn)
{
    SFSpaceStat space;
    char storage_engine_buff[512];
    char up_time_buff[32];
    struct {
        char total[32];
        char used[32];
        char avail[32];
    } space_buffs, trunk_buffs;
    double space_used_ratio;
    double trunk_used_ratio;
    double success_ratio;
    double avg_reclaimed;
    string_t padding_strings[6];
    char tmp_buff[32];
    char formatted_ip[FORMATTED_IP_SIZE];
    int len;
    int unit_value;
    int max_len;
    int padding_len;
    int i;
    char *unit_caption;

    len = sprintf(storage_engine_buff, "enabled: %s", stat->
            storage_engine.enabled ? "true" : "false");
    if (stat->storage_engine.enabled) {
        if (stat->storage_engine.space.trunk.used > 0 && stat->storage_engine.
                space.trunk.used < 1024 * 1024 * 1024)
        {
            unit_value = 1024 * 1024;
            unit_caption = "MB";
        } else {
            unit_value = 1024 * 1024 * 1024;
            unit_caption = "GB";
        }

        space.total = stat->storage_engine.space.trunk.total +
            stat->storage_engine.space.disk_avail;
        space.used = stat->storage_engine.space.trunk.used +
            stat->storage_engine.space.inode_used_space;
        if (space.total <= 0) {
            space.total = 0;
            space_used_ratio = 0.00;
        } else {
            space_used_ratio = (double)space.used / (double)space.total;
        }
        space.avail = space.total - space.used;
        if (space.avail < 0) {
            space.avail = 0;
        }
        long_to_comma_str(space.total / unit_value, space_buffs.total);
        long_to_comma_str(space.used / unit_value, space_buffs.used);
        long_to_comma_str(space.avail / unit_value, space_buffs.avail);

        long_to_comma_str(stat->storage_engine.space.trunk.total /
                unit_value, trunk_buffs.total);
        long_to_comma_str(stat->storage_engine.space.trunk.used /
                unit_value, trunk_buffs.used);
        long_to_comma_str((stat->storage_engine.space.trunk.total -
                    stat->storage_engine.space.trunk.used) /
                unit_value, trunk_buffs.avail);
        if (stat->storage_engine.space.trunk.total <= 0) {
            trunk_used_ratio = 0.00;
        } else {
            trunk_used_ratio = (double)stat->storage_engine.space.trunk.
                used / (double)stat->storage_engine.space.trunk.total;
        }

        FC_SET_STRING(padding_strings[0], space_buffs.total);
        FC_SET_STRING(padding_strings[1], space_buffs.used);
        FC_SET_STRING(padding_strings[2], space_buffs.avail);
        FC_SET_STRING(padding_strings[3], trunk_buffs.total);
        FC_SET_STRING(padding_strings[4], trunk_buffs.used);
        FC_SET_STRING(padding_strings[5], trunk_buffs.avail);
        max_len = padding_strings[0].len;
        for (i=1; i<6; i++) {
            if (padding_strings[i].len > max_len) {
                max_len = padding_strings[i].len;
            }
        }
        for (i=0; i<6; i++) {
            padding_len = max_len - padding_strings[i].len;
            if (padding_len > 0) {
                memcpy(tmp_buff, padding_strings[i].str,
                        padding_strings[i].len + 1);
                memset(padding_strings[i].str, ' ', padding_len);
                memcpy(padding_strings[i].str + padding_len,
                        tmp_buff, padding_strings[i].len + 1);
            }
        }

        if (stat->storage_engine.reclaim.total_count > 0) {
            success_ratio = (double)stat->storage_engine.reclaim.success_count
                / (double)stat->storage_engine.reclaim.total_count;
            if (stat->storage_engine.reclaim.success_count > 0) {
                    avg_reclaimed = (double)stat->storage_engine.reclaim.
                        reclaimed_count / (double)stat->storage_engine.
                        reclaim.success_count;
            } else {
                avg_reclaimed = 0.00;
            }
        } else {
            success_ratio = 0.00;
            avg_reclaimed = 0.00;
        }

        sprintf(storage_engine_buff + len, ", data version "
                "{current: %"PRId64", delay: %"PRId64"},\n"
                "\t\tspace summary: {total: %s %s, used: %s %s (%.2f%%), "
                "avail: %s %s},\n\t\t  trunk space: {total: %s %s, "
                "used: %s %s (%.2f%%), avail: %s %s},\n"
                "\t\trelcaim stat:  {total: %"PRId64", "
                "success: {value: %"PRId64", ratio: %.2f%%},"
                "\n\t\t\t\treclaimed: {sum: %"PRId64", avg: %.2f}",
                stat->storage_engine.current_version,
                stat->storage_engine.version_delay, space_buffs.total,
                unit_caption, space_buffs.used, unit_caption,
                space_used_ratio * 100.00, space_buffs.avail,
                unit_caption, trunk_buffs.total, unit_caption,
                trunk_buffs.used, unit_caption, trunk_used_ratio * 100.00,
                trunk_buffs.avail, unit_caption,
                stat->storage_engine.reclaim.total_count,
                stat->storage_engine.reclaim.success_count,
                success_ratio * 100.00, stat->storage_engine.
                reclaim.reclaimed_count, avg_reclaimed);
    }
    formatDatetime(stat->up_time, "%Y-%m-%d %H:%M:%S",
            up_time_buff, sizeof(up_time_buff));

    format_ip_address(conn->ip_addr, formatted_ip);
    printf( "\tserver_id: %d\n"
            "\thost: %s:%u\n"
            "\tversion: %.*s\n"
            "\tstatus: %d (%s)\n"
            "\tis_master: %s\n"
            "\tauth_enabled: %s\n"
            "\tstorage_engine: {%s}\n"
            "\tup_time: %s\n"
            "\tconnection : {current: %d, max: %d}\n"
            "\tdata : {current_version: %"PRId64", "
            "confirmed_version: %"PRId64"}\n"
            "\tbinlog : {current_version: %"PRId64,
            stat->server_id, formatted_ip, conn->port,
            stat->version.len, stat->version.str, stat->status,
            fdir_get_server_status_caption(stat->status),
            stat->is_master ? "true" : "false",
            stat->auth_enabled ? "true" : "false",
            storage_engine_buff, up_time_buff,
            stat->connection.current_count,
            stat->connection.max_count,
            stat->data.current_version,
            stat->data.confirmed_version,
            stat->binlog.current_version);

    if (stat->is_master) {
        printf( ", writer: {next_version: %"PRId64", \n"
                "\t\t  total_count: %"PRId64", "
                "waiting_count: %d, max_waitings: %d}",
                stat->binlog.writer.next_version,
                stat->binlog.writer.total_count,
                stat->binlog.writer.waiting_count,
                stat->binlog.writer.max_waitings);
    }

    printf( "}\n"
            "\tdentry : {current_inode_sn: %"PRId64", "
            "ns_count: %"PRId64", \n"
            "\t\t  dir_count: %"PRId64", "
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
        if ((result=fdir_client_service_stat(&g_fdir_client_vars.client_ctx,
                        &conn, include_inode_space, &stat)) == 0)
        {
            output(&stat, &conn);
        }
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
        FCServerGroupInfo *server_group;

        if ((result=conn_pool_parse_server_info(host, &conn,
                        FDIR_SERVER_DEFAULT_SERVICE_PORT)) != 0)
        {
            return result;
        }

        server_group = fc_server_get_group_by_index(
                &g_fdir_client_vars.client_ctx.cluster.server_cfg,
                g_fdir_client_vars.client_ctx.cluster.service_group_index);
        conn.comm_type = server_group->comm_type;
    }

    if ((result=fdir_client_service_stat(&g_fdir_client_vars.client_ctx,
                    &conn, include_inode_space, &stat)) != 0)
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

    while ((ch=getopt(argc, argv, "hc:I")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            case 'I':
                include_inode_space = true;
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

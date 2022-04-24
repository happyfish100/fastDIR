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

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/local_ip_func.h"
#include "server_global.h"
#include "cluster_info.h"

#define CLUSTER_INFO_FILENAME                "cluster.info"

#define SERVER_SECTION_PREFIX_STR            "server-"
#define CLUSTER_INFO_ITEM_IS_MASTER          "is_master"
#define CLUSTER_INFO_ITEM_STATUS             "status"

static int cluster_info_write_to_file();

static int init_cluster_server_array()
{
    int bytes;
    FDIRClusterServerInfo *cs;
    FCServerInfo *server;
    FCServerInfo *end;

    bytes = sizeof(FDIRClusterServerInfo) *
        FC_SID_SERVER_COUNT(CLUSTER_SERVER_CONFIG);
    CLUSTER_SERVER_ARRAY.servers = (FDIRClusterServerInfo *)fc_malloc(bytes);
    if (CLUSTER_SERVER_ARRAY.servers == NULL) {
        return ENOMEM;
    }
    memset(CLUSTER_SERVER_ARRAY.servers, 0, bytes);

    end = FC_SID_SERVERS(CLUSTER_SERVER_CONFIG) +
        FC_SID_SERVER_COUNT(CLUSTER_SERVER_CONFIG);
    for (server=FC_SID_SERVERS(CLUSTER_SERVER_CONFIG),
            cs=CLUSTER_SERVER_ARRAY.servers; server<end; server++, cs++)
    {
        cs->server = server;
    }

    CLUSTER_SERVER_ARRAY.count = FC_SID_SERVER_COUNT(CLUSTER_SERVER_CONFIG);
    CLUSTER_SERVER_ARRAY.alives = 1;
    return 0;
}

static int find_myself_in_cluster_config(const char *filename)
{
    const char *local_ip;
    struct {
        const char *ip_addr;
        int port;
    } found;
    FCServerInfo *server;
    FDIRClusterServerInfo *myself;
    int ports[2];
    int count;
    int i;

    count = 0;
    ports[count++] = g_sf_context.inner_port;
    if (g_sf_context.outer_port != g_sf_context.inner_port) {
        ports[count++] = g_sf_context.outer_port;
    }

    found.ip_addr = NULL;
    found.port = 0;
    local_ip = get_first_local_ip();
    while (local_ip != NULL) {
        for (i=0; i<count; i++) {
            server = fc_server_get_by_ip_port(&CLUSTER_SERVER_CONFIG,
                    local_ip, ports[i]);
            if (server != NULL) {
                myself = CLUSTER_SERVER_ARRAY.servers +
                    (server - FC_SID_SERVERS(CLUSTER_SERVER_CONFIG));
                if (CLUSTER_MYSELF_PTR == NULL) {
                    CLUSTER_MYSELF_PTR = myself;
                } else if (myself != CLUSTER_MYSELF_PTR) {
                    logError("file: "__FILE__", line: %d, "
                            "cluster config file: %s, my ip and port "
                            "in more than one instances, %s:%u in "
                            "server id %d, and %s:%u in server id %d",
                            __LINE__, filename, found.ip_addr, found.port,
                            CLUSTER_MY_SERVER_ID, local_ip,
                            ports[i], myself->server->id);
                    return EEXIST;
                }

                found.ip_addr = local_ip;
                found.port = ports[i];
            }
        }

        local_ip = get_next_local_ip(local_ip);
    }

    if (CLUSTER_MYSELF_PTR == NULL) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, can't find myself "
                "by my local ip and listen port", __LINE__, filename);
        return ENOENT;
    }

    return 0;
}

FDIRClusterServerInfo *fdir_get_server_by_id(const int server_id)
{
    FCServerInfo *server;
    server = fc_server_get_by_id(&CLUSTER_SERVER_CONFIG, server_id);
    if (server == NULL) {
        return NULL;
    }

    return CLUSTER_SERVER_ARRAY.servers + (server -
            FC_SID_SERVERS(CLUSTER_SERVER_CONFIG));
}

static int load_servers_from_ini_ctx(IniContext *ini_context)
{
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *end;
    char section_name[64];

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        sprintf(section_name, "%s%d",
                SERVER_SECTION_PREFIX_STR,
                cs->server->id);
        cs->is_master = iniGetBoolValue(section_name,
                CLUSTER_INFO_ITEM_IS_MASTER, ini_context, false);
        cs->status = iniGetIntValue(section_name,
                CLUSTER_INFO_ITEM_STATUS, ini_context,
                FDIR_SERVER_STATUS_INIT);

        if (cs->status == FDIR_SERVER_STATUS_SYNCING ||
                cs->status == FDIR_SERVER_STATUS_ACTIVE)
        {
            cs->status = FDIR_SERVER_STATUS_OFFLINE;
        }
    }

    return 0;
}

static int load_cluster_info_from_file()
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", DATA_PATH_STR, CLUSTER_INFO_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return cluster_info_write_to_file();
        }
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    result = load_servers_from_ini_ctx(&ini_context);
    iniFreeContext(&ini_context);

    return result;
}

int cluster_info_init(const char *cluster_config_filename)
{
    int result;

    if ((result=init_cluster_server_array()) != 0) {
        return result;
    }
    if ((result=load_cluster_info_from_file()) != 0) {
        return result;
    }

    if ((result=find_myself_in_cluster_config(cluster_config_filename)) != 0) {
        return result;
    }

    return 0;
}

static int cluster_info_write_to_file()
{
    char full_filename[PATH_MAX];
    char buff[8 * 1024];
    char *p;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *end;
    int result;
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", DATA_PATH_STR, CLUSTER_INFO_FILENAME);

    p = buff;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        p += sprintf(p,
                "[%s%d]\n"
                "%s=%d\n"
                "%s=%d\n\n",
                SERVER_SECTION_PREFIX_STR, cs->server->id,
                CLUSTER_INFO_ITEM_IS_MASTER,
                cs == CLUSTER_MASTER_ATOM_PTR ? 1 : 0,
                CLUSTER_INFO_ITEM_STATUS,
                __sync_fetch_and_add(&cs->status, 0)
                );
    }

    len = p - buff;
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "write to file \"%s\" fail, "
            "errno: %d, error info: %s",
            __LINE__, full_filename,
            result, STRERROR(result));
    }

    return result;
}

static int cluster_info_sync_to_file(void *args)
{
    static int last_synced_version = 0;

    if (last_synced_version == CLUSTER_SERVER_ARRAY.change_version) {
        return 0;
    }

    last_synced_version = CLUSTER_SERVER_ARRAY.change_version;
    return cluster_info_write_to_file();
}

int cluster_info_setup_sync_to_file_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 1, cluster_info_sync_to_file, (void *)0x1234);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

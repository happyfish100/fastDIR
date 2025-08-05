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
#include "sf/sf_service.h"
#include "server_global.h"
#include "cluster_relationship.h"
#include "cluster_info.h"

#define CLUSTER_INFO_FILENAME_STR     "cluster.info"
#define CLUSTER_INFO_FILENAME_LEN     (sizeof(CLUSTER_INFO_FILENAME_STR) - 1)

#define SERVER_SECTION_PREFIX_STR      "server-"
#define SERVER_SECTION_PREFIX_LEN      (sizeof(SERVER_SECTION_PREFIX_STR) - 1)

#define CLUSTER_INFO_ITEM_IS_MASTER_STR "is_master"
#define CLUSTER_INFO_ITEM_IS_MASTER_LEN (sizeof(CLUSTER_INFO_ITEM_IS_MASTER_STR) - 1)

#define CLUSTER_INFO_ITEM_STATUS_STR    "status"
#define CLUSTER_INFO_ITEM_STATUS_LEN    (sizeof(CLUSTER_INFO_ITEM_STATUS_STR) - 1)

static int last_synced_version = 0;
static int last_refresh_file_time = 0;

static int cluster_info_write_to_file();

void cluster_info_set_status(FDIRClusterServerInfo *cs, const int new_status)
{
    int old_status;

    old_status = __sync_fetch_and_add(&cs->status, 0);
    if (old_status == new_status) {
        return;
    }

    if (old_status == FDIR_SERVER_STATUS_ACTIVE &&
            cs != CLUSTER_MYSELF_PTR)
    {
        FC_ATOMIC_DEC(CLUSTER_SERVER_ARRAY.active_count);
        if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
            cluster_add_to_detect_server_array(cs);
        }
    }

    do  {
        if (__sync_bool_compare_and_swap(&cs->status,
                    old_status, new_status))
        {
            break;
        }
        old_status = __sync_add_and_fetch(&cs->status, 0);
    } while (old_status != new_status);
    __sync_add_and_fetch(&CLUSTER_SERVER_ARRAY.change_version, 1);

    if (new_status == FDIR_SERVER_STATUS_ACTIVE &&
            cs != CLUSTER_MYSELF_PTR)
    {
        FC_ATOMIC_INC(CLUSTER_SERVER_ARRAY.active_count);
        if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
            cluster_remove_from_detect_server_array(cs);
        }
    }
}

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
    CLUSTER_SERVER_ARRAY.active_count = 1;
    return 0;
}

static int is_same_port(const char *caption, const int my_inner_port,
        const int my_outer_port, const int cluster_port, const char *filename)
{
    if (my_inner_port == my_outer_port) {
        if (my_inner_port != cluster_port) {
            logError("file: "__FILE__", line: %d, "
                    "%s listen port %d != port %d in the cluster "
                    "config file: %s", __LINE__, caption, my_inner_port,
                    cluster_port, filename);
            return EINVAL;
        }
    } else {
        if (my_inner_port != cluster_port && my_outer_port != cluster_port) {
            logError("file: "__FILE__", line: %d, "
                    "%s listen inner port %d and outer port %d NOT contain "
                    "port %d in the cluster config file: %s", __LINE__,
                    caption, my_inner_port, my_outer_port, cluster_port,
                    filename);
            return EINVAL;
        }
    }

    return 0;
}

static int check_ports(FCServerInfo *server, const char *filename)
{
    SFNetworkHandler *cluster_handler;
    SFNetworkHandler *service_handler;
    int result;

    cluster_handler = sf_get_first_network_handler_ex(&CLUSTER_SF_CTX);
    service_handler = sf_get_first_network_handler_ex(&SERVICE_SF_CTX);

    if ((result=is_same_port("cluster", cluster_handler->inner.port,
                    cluster_handler->outer.port,
                    CLUSTER_GROUP_ADDRESS_FIRST_PTR(server)->conn.port,
                    filename)) != 0)
    {
        return result;
    }

    if ((result=is_same_port("service", service_handler->inner.port,
                    service_handler->outer.port,
                    SERVICE_GROUP_ADDRESS_FIRST_PTR(server)->conn.port,
                    filename)) != 0)
    {
        return result;
    }

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
    SFNetworkHandler *service_handler;
    SFNetworkHandler *cluster_handler;
    char formatted_found_ip[FORMATTED_IP_SIZE];
    char formatted_local_ip[FORMATTED_IP_SIZE];
    int ports[4];
    int count;
    int i;

    service_handler = SERVICE_SF_CTX.handlers[SF_IPV4_ADDRESS_FAMILY_INDEX].
        handlers + SF_SOCKET_NETWORK_HANDLER_INDEX;
    cluster_handler = CLUSTER_SF_CTX.handlers[SF_IPV4_ADDRESS_FAMILY_INDEX].
        handlers + SF_SOCKET_NETWORK_HANDLER_INDEX;
    count = 0;
    ports[count++] = service_handler->inner.port;
    if (service_handler->outer.port != service_handler->inner.port) {
        ports[count++] = service_handler->outer.port;
    }
    ports[count++] = cluster_handler->inner.port;
    if (cluster_handler->outer.port != cluster_handler->inner.port) {
        ports[count++] = cluster_handler->outer.port;
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
                    format_ip_address(found.ip_addr, formatted_found_ip);
                    format_ip_address(local_ip, formatted_local_ip);
                    logError("file: "__FILE__", line: %d, "
                            "cluster config file: %s, my ip and port "
                            "in more than one instances, %s:%u in "
                            "server id %d, and %s:%u in server id %d",
                            __LINE__, filename, formatted_found_ip, found.port,
                            CLUSTER_MY_SERVER_ID, formatted_local_ip,
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

    return check_ports(CLUSTER_MYSELF_PTR->server, filename);
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

static inline void get_cluster_info_filename(
        char *full_filename, const int size)
{
    char *p;

    if (DATA_PATH_LEN + 1 + CLUSTER_INFO_FILENAME_LEN >= size) {
        snprintf(full_filename, size, "%s/%s", DATA_PATH_STR,
                CLUSTER_INFO_FILENAME_STR);
        return;
    }

    memcpy(full_filename, DATA_PATH_STR, DATA_PATH_LEN);
    p = full_filename + DATA_PATH_LEN;
    *p++ = '/';
    memcpy(p, CLUSTER_INFO_FILENAME_STR, CLUSTER_INFO_FILENAME_LEN);
    *(p + CLUSTER_INFO_FILENAME_LEN) = '\0';
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
        cs->is_old_master = iniGetBoolValue(section_name,
                CLUSTER_INFO_ITEM_IS_MASTER_STR, ini_context, false);
        cs->status = iniGetIntValue(section_name,
                CLUSTER_INFO_ITEM_STATUS_STR, ini_context,
                FDIR_SERVER_STATUS_INIT);

        if (cs->status == FDIR_SERVER_STATUS_SYNCING ||
                cs->status == FDIR_SERVER_STATUS_ACTIVE)
        {
            cs->status = FDIR_SERVER_STATUS_OFFLINE;
        }
    }

    return 0;
}

#define cluster_info_set_file_mtime() \
    cluster_info_set_file_mtime_ex(g_current_time)

static int cluster_info_set_file_mtime_ex(const time_t t)
{
    char full_filename[PATH_MAX];
    struct timeval times[2];

    times[0].tv_sec = t;
    times[0].tv_usec = 0;
    times[1].tv_sec = t;
    times[1].tv_usec = 0;

    get_cluster_info_filename(full_filename, sizeof(full_filename));
    if (utimes(full_filename, times) < 0) {
        logError("file: "__FILE__", line: %d, "
                "utimes file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EPERM;
    }
    return 0;
}

static int get_cluster_info_file_mtime()
{
    char full_filename[PATH_MAX];
    struct stat buf;

    get_cluster_info_filename(full_filename, sizeof(full_filename));
    if (stat(full_filename, &buf) < 0) {
        logError("file: "__FILE__", line: %d, "
                "stat file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EPERM;
    }

    CLUSTER_LAST_SHUTDOWN_TIME = buf.st_mtime;
    return 0;
}

static int load_cluster_info_from_file()
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    get_cluster_info_filename(full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            if ((result=cluster_info_write_to_file()) != 0) {
                return result;
            }

            return cluster_info_set_file_mtime_ex(
                    g_current_time - 86400);
        }
    }

    if ((result=get_cluster_info_file_mtime()) != 0) {
        return result;
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
    time_t t;
    struct tm tm_current;

    if ((result=init_cluster_server_array()) != 0) {
        return result;
    }
    if ((result=load_cluster_info_from_file()) != 0) {
        return result;
    }

    if ((result=find_myself_in_cluster_config(cluster_config_filename)) != 0) {
        return result;
    }

    t = g_current_time + 89;
    localtime_r(&t, &tm_current);
    tm_current.tm_sec = 0;
    last_refresh_file_time = mktime(&tm_current);
    last_synced_version = CLUSTER_SERVER_ARRAY.change_version;

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

    p = buff;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        *p++ = '[';
        memcpy(p, SERVER_SECTION_PREFIX_STR, SERVER_SECTION_PREFIX_LEN);
        p += SERVER_SECTION_PREFIX_LEN;
        p += fc_itoa(cs->server->id, p);
        *p++ = ']';
        *p++ = '\n';

        memcpy(p, CLUSTER_INFO_ITEM_IS_MASTER_STR,
                CLUSTER_INFO_ITEM_IS_MASTER_LEN);
        p += CLUSTER_INFO_ITEM_IS_MASTER_LEN;
        *p++ = '=';
        *p++ = (cs == CLUSTER_MASTER_ATOM_PTR ? '1' : '0');
        *p++ = '\n';

        memcpy(p, CLUSTER_INFO_ITEM_STATUS_STR,
                CLUSTER_INFO_ITEM_STATUS_LEN);
        p += CLUSTER_INFO_ITEM_STATUS_LEN;
        *p++ = '=';
        p += fc_itoa(FC_ATOMIC_GET(cs->status), p);
        *p++ = '\n';
        *p++ = '\n';
    }

    len = p - buff;
    get_cluster_info_filename(full_filename, sizeof(full_filename));
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
    if (!(CLUSTER_MASTER_ATOM_PTR != NULL &&
                CLUSTER_LAST_HEARTBEAT_TIME > 0))
    {
        return 0;
    }

    if (last_synced_version == CLUSTER_SERVER_ARRAY.change_version) {
        if (g_current_time - last_refresh_file_time > 60) {
            last_refresh_file_time = g_current_time;
            return cluster_info_set_file_mtime();
        }
        return 0;
    }

    last_synced_version = CLUSTER_SERVER_ARRAY.change_version;
    last_refresh_file_time = g_current_time;
    return cluster_info_write_to_file();
}

int cluster_info_setup_sync_to_file_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 1, cluster_info_sync_to_file, NULL);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

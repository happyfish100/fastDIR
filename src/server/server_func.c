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
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/md5.h"
#include "fastcommon/local_ip_func.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "sf/sf_binlog_writer.h"
#include "common/fdir_proto.h"
#include "server_global.h"
#include "cluster_info.h"
#include "server_func.h"

static int server_load_admin_config(IniContext *ini_context)
{
#define ADMIN_SECTION_NAME "admin"

    char *username;
    char *secret_key;
    char *buff;
    char *p;
    struct {
        int username;
        int secret_key;
    } lengths;
    int bytes;

    //TODO
    return 0;

    if ((username=iniGetRequiredStrValue(ADMIN_SECTION_NAME, "username",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }

    if ((secret_key=iniGetRequiredStrValue(ADMIN_SECTION_NAME, "secret_key",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }

    g_server_global_vars.admin.username.len = strlen(username);
    g_server_global_vars.admin.secret_key.len = strlen(secret_key);

    lengths.username = g_server_global_vars.admin.username.len + 1;
    lengths.secret_key = g_server_global_vars.admin.secret_key.len + 1;

    bytes = lengths.username + lengths.secret_key;
    buff = (char *)fc_malloc(bytes);
    if (buff == NULL) {
        return ENOMEM;
    }

    p = buff;
    g_server_global_vars.admin.username.str = p;
    p += lengths.username;

    g_server_global_vars.admin.secret_key.str = p;
    p += lengths.secret_key;

    memcpy(g_server_global_vars.admin.username.str, username, lengths.username);
    memcpy(g_server_global_vars.admin.secret_key.str, secret_key, lengths.secret_key);
    return 0;
}

static int get_bytes_item_config(IniContext *ini_context,
        const char *filename, const char *item_name,
        const int64_t default_value, int64_t *bytes)
{
    int result;
    char *value;

    value = iniGetStrValue(NULL, item_name, ini_context);
    if (value == NULL) {
        *bytes = default_value;
        return 0;
    }
    if ((result=parse_bytes(value, 1, bytes)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item: %s, value: %s is invalid",
                __LINE__, filename, item_name, value);
    }
    return result;
}

static void log_cluster_server_config()
{
    FastBuffer buffer;

    if (fast_buffer_init_ex(&buffer, 1024) != 0) {
        return;
    }
    fc_server_to_config_string(&CLUSTER_CONFIG_CTX, &buffer);
    log_it1(LOG_INFO, buffer.data, buffer.length);
    fast_buffer_destroy(&buffer);

    fc_server_to_log(&CLUSTER_CONFIG_CTX);
}

static int calc_cluster_config_sign()
{
    FastBuffer buffer;
    int result;

    if ((result=fast_buffer_init_ex(&buffer, 1024)) != 0) {
        return result;
    }
    fc_server_to_config_string(&CLUSTER_CONFIG_CTX, &buffer);
    my_md5_buffer(buffer.data, buffer.length, CLUSTER_CONFIG_SIGN_BUF);

    {
    char hex_buff[2 * CLUSTER_CONFIG_SIGN_LEN + 1];
    logInfo("cluster config length: %d, sign: %s", buffer.length,
            bin2hex((const char *)CLUSTER_CONFIG_SIGN_BUF,
                CLUSTER_CONFIG_SIGN_LEN, hex_buff));
    }
    fast_buffer_destroy(&buffer);
    return 0;
}

static int find_group_indexes_in_cluster_config(const char *filename)
{
    CLUSTER_GROUP_INDEX = fc_server_get_group_index(&CLUSTER_CONFIG_CTX,
            "cluster");
    if (CLUSTER_GROUP_INDEX < 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, cluster group not configurated",
                __LINE__, filename);
        return ENOENT;
    }

    SERVICE_GROUP_INDEX = fc_server_get_group_index(&CLUSTER_CONFIG_CTX,
            "service");
    if (SERVICE_GROUP_INDEX < 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, service group not configurated",
                __LINE__, filename);
        return ENOENT;
    }

    return 0;
}

static int server_load_cluster_id(IniContext *ini_context, const char *filename)
{
    char *cluster_id;
    char *endptr = NULL;

    cluster_id = iniGetStrValue(NULL, "cluster_id", ini_context);
    if (cluster_id == NULL || *cluster_id == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"cluster_id\" not exist or empty",
                __LINE__, filename);
        return ENOENT;
    }

    CLUSTER_ID = strtol(cluster_id, &endptr, 10);
    if (CLUSTER_ID <= 0 || (endptr != NULL && *endptr != '\0')) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, cluster_id: %s is invalid, "
                "it must be a natural number!", __LINE__,
                filename, cluster_id);
        return EINVAL;
    }

    if (CLUSTER_ID > FDIR_CLUSTER_ID_MAX) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, cluster_id: %s is too large, "
                "exceeds %d", __LINE__, filename,
                cluster_id, FDIR_CLUSTER_ID_MAX);
        return EINVAL;
    }

    return 0;
}

static int load_cluster_config(IniContext *ini_context, const char *filename)
{
    int result;
    char *cluster_config_filename;
    char full_cluster_filename[PATH_MAX];
    const int min_hosts_each_group = 1;
    const bool share_between_groups = true;

    if ((result=server_load_cluster_id(ini_context, filename)) != 0) {
        return result;
    }

    cluster_config_filename = iniGetStrValue(NULL,
            "cluster_config_filename", ini_context);
    if (cluster_config_filename == NULL || *cluster_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "item \"cluster_config_filename\" not exist or empty",
                __LINE__);
        return ENOENT;
    }

    resolve_path(filename, cluster_config_filename,
            full_cluster_filename, sizeof(full_cluster_filename));
    if ((result=fc_server_load_from_file_ex(&CLUSTER_CONFIG_CTX,
                    full_cluster_filename, FDIR_SERVER_DEFAULT_CLUSTER_PORT,
                    min_hosts_each_group, share_between_groups)) != 0)
    {
        return result;
    }

    if ((result=cluster_info_init(cluster_config_filename)) != 0) {
        return result;
    }
    if ((result=find_group_indexes_in_cluster_config(filename)) != 0) {
        return result;
    }
    if ((result=calc_cluster_config_sign()) != 0) {
        return result;
    }

    return 0;
}

static int load_data_path_config(IniContext *ini_context, const char *filename)
{
    char *data_path;

    data_path = iniGetStrValue(NULL, "data_path", ini_context);
    if (data_path == NULL) {
        data_path = "data";
    } else if (*data_path == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, empty data_path! "
                "please set data_path correctly.",
                __LINE__, filename);
        return EINVAL;
    }

    if (*data_path == '/') {
        DATA_PATH_LEN = strlen(data_path);
        DATA_PATH_STR = fc_strdup1(data_path, DATA_PATH_LEN);
        if (DATA_PATH_STR == NULL) {
            return ENOMEM;
        }
    } else {
        DATA_PATH_LEN = strlen(SF_G_BASE_PATH) + strlen(data_path) + 1;
        DATA_PATH_STR = (char *)fc_malloc(DATA_PATH_LEN + 1);
        if (DATA_PATH_STR == NULL) {
            return ENOMEM;
        }
        DATA_PATH_LEN = sprintf(DATA_PATH_STR, "%s/%s",
                SF_G_BASE_PATH, data_path);
    }
    chopPath(DATA_PATH_STR);

    if (access(DATA_PATH_STR, F_OK) != 0) {
        if (errno != ENOENT) {
            logError("file: "__FILE__", line: %d, "
                    "access %s fail, errno: %d, error info: %s",
                    __LINE__, DATA_PATH_STR, errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }

        if (mkdir(DATA_PATH_STR, 0775) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "mkdir %s fail, errno: %d, error info: %s",
                    __LINE__, DATA_PATH_STR, errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
        
        SF_CHOWN_RETURN_ON_ERROR(DATA_PATH_STR, geteuid(), getegid());
    }

    return 0;
}

static int load_dentry_max_data_size(IniContext *ini_context,
        const char *filename)
{
    int64_t bytes;
    int result;

    if ((result=get_bytes_item_config(ini_context, filename,
                    "dentry_max_data_size", 256, &bytes)) != 0)
    {
        return result;
    }

    DENTRY_MAX_DATA_SIZE = bytes;
    if (DENTRY_MAX_DATA_SIZE <= 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s , dentry_max_data_size: %d <= 0",
                __LINE__, filename, DENTRY_MAX_DATA_SIZE);
        return EINVAL;
    }

    if (DENTRY_MAX_DATA_SIZE > 4096) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s , dentry_max_data_size: %d > 4KB",
                __LINE__, filename, DENTRY_MAX_DATA_SIZE);
        return EOVERFLOW;
    }

    return 0;
}

static void server_log_configs()
{
    char sz_server_config[512];
    char sz_global_config[512];
    char sz_slowlog_config[256];
    char sz_service_config[128];
    char sz_cluster_config[128];

    sf_global_config_to_string(sz_global_config, sizeof(sz_global_config));
    sf_slow_log_config_to_string(&SLOW_LOG_CFG, "slow_log",
            sz_slowlog_config, sizeof(sz_slowlog_config));

    sf_context_config_to_string(&g_sf_context,
            sz_service_config, sizeof(sz_service_config));
    sf_context_config_to_string(&CLUSTER_SF_CTX,
            sz_cluster_config, sizeof(sz_cluster_config));

    snprintf(sz_server_config, sizeof(sz_server_config),
            "cluster_id = %d, my server id = %d, data_path = %s, "
            "data_threads = %d, dentry_max_data_size = %d, "
            "binlog_buffer_size = %d KB, "
            "slave_binlog_check_last_rows = %d, "
            "admin config {username: %s, secret_key: %s}, "
            "reload_interval_ms = %d ms, "
            "check_alive_interval = %d s, "
            "namespace_hashtable_capacity = %d, "
            "inode_hashtable_capacity = %"PRId64", "
            "inode_shared_locks_count = %d, "
            "cluster server count = %d",
            CLUSTER_ID, CLUSTER_MY_SERVER_ID,
            DATA_PATH_STR, DATA_THREAD_COUNT,
            DENTRY_MAX_DATA_SIZE, BINLOG_BUFFER_SIZE / 1024,
            SLAVE_BINLOG_CHECK_LAST_ROWS,
            g_server_global_vars.admin.username.str,
            g_server_global_vars.admin.secret_key.str,
            g_server_global_vars.reload_interval_ms,
            g_server_global_vars.check_alive_interval,
            g_server_global_vars.namespace_hashtable_capacity,
            INODE_HASHTABLE_CAPACITY, INODE_SHARED_LOCKS_COUNT,
            FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX));

    logInfo("%s, %s, service: {%s}, cluster: {%s}, %s",
            sz_global_config, sz_slowlog_config, sz_service_config,
            sz_cluster_config, sz_server_config);
    log_local_host_ip_addrs();
    log_cluster_server_config();
}

static int load_binlog_buffer_size(IniContext *ini_context,
        const char *filename)
{
    int64_t bytes;
    int result;

    if ((result=get_bytes_item_config(ini_context, filename,
                    "binlog_buffer_size", FDIR_DEFAULT_BINLOG_BUFFER_SIZE,
                    &bytes)) != 0)
    {
        return result;
    }
    if (bytes < 4096) {
        logWarning("file: "__FILE__", line: %d, "
                "config file: %s , binlog_buffer_size: %d is too small, "
                "set it to default: %d", __LINE__, filename,
                BINLOG_BUFFER_SIZE, FDIR_DEFAULT_BINLOG_BUFFER_SIZE);
        BINLOG_BUFFER_SIZE = FDIR_DEFAULT_BINLOG_BUFFER_SIZE;
    } else {
        BINLOG_BUFFER_SIZE = bytes;
    }

    return 0;
}

int server_load_config(const char *filename)
{
    const int task_buffer_extra_size = 0;
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    if ((result=sf_load_config("fdir_serverd", filename, &ini_context,
                    "service", FDIR_SERVER_DEFAULT_SERVICE_PORT,
                    FDIR_SERVER_DEFAULT_SERVICE_PORT,
                    task_buffer_extra_size)) != 0)
    {
        return result;
    }

    if ((result=sf_load_context_from_config(&CLUSTER_SF_CTX,
                    filename, &ini_context, "cluster",
                    FDIR_SERVER_DEFAULT_CLUSTER_PORT,
                    FDIR_SERVER_DEFAULT_CLUSTER_PORT)) != 0)
    {
        return result;
    }

    if ((result=load_data_path_config(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_dentry_max_data_size(&ini_context, filename)) != 0) {
        return result;
    }

    DATA_THREAD_COUNT = iniGetIntValue(NULL, "data_threads",
            &ini_context, FDIR_DEFAULT_DATA_THREAD_COUNT);
    if (DATA_THREAD_COUNT <= 0) {
        DATA_THREAD_COUNT = FDIR_DEFAULT_DATA_THREAD_COUNT;
    }

    if ((result=server_load_admin_config(&ini_context)) != 0) {
        return result;
    }

    if ((result=load_binlog_buffer_size(&ini_context, filename)) != 0) {
        return result;
    }

    SLAVE_BINLOG_CHECK_LAST_ROWS = iniGetIntValue(NULL,
            "slave_binlog_check_last_rows", &ini_context,
            FDIR_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS);
    if (SLAVE_BINLOG_CHECK_LAST_ROWS > FDIR_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS) {
        logWarning("file: "__FILE__", line: %d, "
                "config file: %s , slave_binlog_check_last_rows: %d "
                "is too large, set it to %d", __LINE__, filename,
                SLAVE_BINLOG_CHECK_LAST_ROWS,
                FDIR_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS);
        SLAVE_BINLOG_CHECK_LAST_ROWS = FDIR_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS;
    }

    g_server_global_vars.reload_interval_ms = iniGetIntValue(NULL,
            "reload_interval_ms", &ini_context,
            FDIR_SERVER_DEFAULT_RELOAD_INTERVAL);
    if (g_server_global_vars.reload_interval_ms <= 0) {
        g_server_global_vars.reload_interval_ms =
            FDIR_SERVER_DEFAULT_RELOAD_INTERVAL;
    }

    g_server_global_vars.check_alive_interval = iniGetIntValue(NULL,
            "check_alive_interval", &ini_context,
            FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL);
    if (g_server_global_vars.check_alive_interval <= 0) {
        g_server_global_vars.check_alive_interval =
            FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL;
    }

    g_server_global_vars.namespace_hashtable_capacity = iniGetIntValue(NULL,
            "namespace_hashtable_capacity", &ini_context,
            FDIR_NAMESPACE_HASHTABLE_DEFAULT_CAPACITY);
    if (g_server_global_vars.namespace_hashtable_capacity <= 0) {
        g_server_global_vars.namespace_hashtable_capacity =
            FDIR_NAMESPACE_HASHTABLE_DEFAULT_CAPACITY;
    }

    INODE_HASHTABLE_CAPACITY = iniGetIntValue(NULL,
            "inode_hashtable_capacity", &ini_context,
            FDIR_INODE_HASHTABLE_DEFAULT_CAPACITY);
    if (INODE_HASHTABLE_CAPACITY <= 0) {
        INODE_HASHTABLE_CAPACITY = FDIR_INODE_HASHTABLE_DEFAULT_CAPACITY;
    }

    INODE_SHARED_LOCKS_COUNT = iniGetIntValue(NULL,
            "inode_shared_locks_count", &ini_context,
            FDIR_INODE_SHARED_LOCKS_DEFAULT_COUNT);
    if (INODE_SHARED_LOCKS_COUNT <= 0) {
        INODE_SHARED_LOCKS_COUNT = FDIR_INODE_SHARED_LOCKS_DEFAULT_COUNT;
    }

    if ((result=load_cluster_config(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=sf_load_slow_log_config(filename, &ini_context,
                    &SLOW_LOG_CTX, &SLOW_LOG_CFG)) != 0)
    {
        return result;
    }

    iniFreeContext(&ini_context);

    g_sf_binlog_data_path = DATA_PATH_STR;

    load_local_host_ip_addrs();
    server_log_configs();

    return 0;
}

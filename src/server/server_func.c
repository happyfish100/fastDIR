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
#include <dlfcn.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/md5.h"
#include "fastcommon/local_ip_func.h"
#include "fastcommon/system_info.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "sf/sf_binlog_writer.h"
#include "fastcfs/auth/fcfs_auth_for_server.h"
#include "fastcfs/vote/fcfs_vote_client.h"
#include "common/fdir_proto.h"
#include "common/fdir_func.h"
#include "db/inode_add_mark.h"
#include "db/dentry_lru.h"
#include "server_global.h"
#include "cluster_info.h"
#include "server_func.h"

#define INODE_BINLOG_DEFAULT_SUBDIRS          128
#define INODE_BINLOG_MIN_SUBDIRS               16
#define INODE_BINLOG_MAX_SUBDIRS              256
#define DEFAULT_BATCH_STORE_ON_MODIFIES    102400
#define DEFAULT_BATCH_STORE_INTERVAL           60
#define DEFAULT_INDEX_DUMP_INTERVAL         86400
#define DEFAULT_ELIMINATE_INTERVAL              1

static void log_cluster_server_config()
{
    FastBuffer buffer;

    if (fast_buffer_init_ex(&buffer, 1024) != 0) {
        return;
    }
    fc_server_to_config_string(&CLUSTER_SERVER_CONFIG, &buffer);
    log_it1(LOG_INFO, buffer.data, buffer.length);
    fast_buffer_destroy(&buffer);

    fc_server_to_log(&CLUSTER_SERVER_CONFIG);
}

static int server_load_cluster_id(IniFullContext *ini_ctx)
{
    char *cluster_id;
    char *endptr = NULL;

    cluster_id = iniGetStrValue(ini_ctx->section_name,
            "cluster_id", ini_ctx->context);
    if (cluster_id == NULL || *cluster_id == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"cluster_id\" not exist or empty",
                __LINE__, ini_ctx->filename);
        return ENOENT;
    }

    CLUSTER_ID = strtol(cluster_id, &endptr, 10);
    if (CLUSTER_ID <= 0 || (endptr != NULL && *endptr != '\0')) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, cluster_id: %s is invalid, "
                "it must be a natural number!", __LINE__,
                ini_ctx->filename, cluster_id);
        return EINVAL;
    }

    if (CLUSTER_ID > FDIR_CLUSTER_ID_MAX) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, cluster_id: %s is too large, "
                "exceeds %d", __LINE__, ini_ctx->filename,
                cluster_id, FDIR_CLUSTER_ID_MAX);
        return EINVAL;
    }

    return 0;
}

static inline int load_master_election_config(IniFullContext *ini_ctx)
{
    int result;

    ELECTION_MASTER_LOST_TIMEOUT = iniGetIntCorrectValue(
            ini_ctx, "master_lost_timeout", 3, 1, 300);
    ELECTION_MAX_WAIT_TIME = iniGetIntCorrectValue(
            ini_ctx, "max_wait_time", 30, 1, 3600);
    ELECTION_MAX_SHUTDOWN_DURATION = iniGetIntCorrectValue(
            ini_ctx, "max_shutdown_duration", 300, 60, 86400);
    if ((result=sf_load_election_quorum_config(&MASTER_ELECTION_QUORUM,
                    ini_ctx)) == 0)
    {
        result = fcfs_vote_client_init_for_server(
                ini_ctx, &VOTE_NODE_ENABLED);
    }

    return result;
}

static inline int load_replication_quorum_config(IniFullContext *ini_ctx)
{
    ini_ctx->section_name = "data-replication";
    REPLICA_QUORUM_DEACTIVE_ON_FAILURES  = iniGetIntCorrectValue(
            ini_ctx, "deactive_on_failures", 3, 1, 100);
    return sf_load_replication_quorum_config(&REPLICATION_QUORUM, ini_ctx);
}

static int load_cluster_sub_config(const char *cluster_filename)
{
    IniContext ini_context;
    IniFullContext ini_ctx;
    int result;

    if ((result=iniLoadFromFile(cluster_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, cluster_filename, result);
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, cluster_filename,
            "master-election", &ini_context);
    if ((result=load_master_election_config(&ini_ctx)) != 0) {
        return result;
    }
    result = load_replication_quorum_config(&ini_ctx);

    iniFreeContext(&ini_context);
    return result;
}

static int load_cluster_config(IniFullContext *ini_ctx,
        char *full_cluster_filename)
{
    int result;

    if ((result=server_load_cluster_id(ini_ctx)) != 0) {
        return result;
    }

    if ((result=sf_load_cluster_config_ex(&CLUSTER_CONFIG,
                    ini_ctx, FDIR_SERVER_DEFAULT_CLUSTER_PORT,
                    full_cluster_filename, PATH_MAX)) != 0)
    {
        return result;
    }

    if ((result=load_cluster_sub_config(full_cluster_filename)) != 0) {
        return result;
    }

    REPLICA_QUORUM_NEED_MAJORITY = SF_REPLICATION_QUORUM_NEED_MAJORITY(
            REPLICATION_QUORUM, CLUSTER_SERVER_ARRAY.count);
    REPLICA_QUORUM_NEED_DETECT = SF_REPLICATION_QUORUM_NEED_DETECT(
            REPLICATION_QUORUM, CLUSTER_SERVER_ARRAY.count);
    return 0;
}

static int load_dentry_max_data_size(IniFullContext *ini_ctx)
{
    DENTRY_MAX_DATA_SIZE = iniGetByteCorrectValue(ini_ctx,
            "dentry_max_data_size", 256, 0, 1024 * 1024);
    if (DENTRY_MAX_DATA_SIZE <= 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s , dentry_max_data_size: %d <= 0",
                __LINE__, ini_ctx->filename, DENTRY_MAX_DATA_SIZE);
        return EINVAL;
    }

    if (DENTRY_MAX_DATA_SIZE > 4096) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s , dentry_max_data_size: %d > 4KB",
                __LINE__, ini_ctx->filename, DENTRY_MAX_DATA_SIZE);
        return EOVERFLOW;
    }

    return 0;
}

static int load_posix_acl(IniFullContext *ini_ctx)
{
    char *posix_acl;

    posix_acl = iniGetStrValue(ini_ctx->section_name,
            "posix_acl", ini_ctx->context);
    if (posix_acl == NULL || *posix_acl == '\0') {
        FDIR_POSIX_ACL = fdir_posix_acl_strict;
        return 0;
    }

    if (strcasecmp(posix_acl, "strict") == 0) {
        FDIR_POSIX_ACL = fdir_posix_acl_strict;
    } else if (strcasecmp(posix_acl, "none") == 0) {
        FDIR_POSIX_ACL = fdir_posix_acl_none;
    } else {
        logError("file: "__FILE__", line: %d, "
                "config file: %s , unkown posix_acl: %s",
                __LINE__, ini_ctx->filename, posix_acl);
        return EINVAL;
    }

    return 0;
}

#define LOAD_API(var, fname) \
    do { \
        var = (fname##_func)dlsym(dlhandle, #fname); \
        if (var == NULL) {  \
            logError("file: "__FILE__", line: %d, "  \
                    "dlsym api %s fail, error info: %s", \
                    __LINE__, #fname, dlerror()); \
            return ENOENT; \
        } \
    } while (0)


static int load_storage_engine_apis()
{
    void *dlhandle;

    dlhandle = dlopen(STORAGE_ENGINE_LIBRARY, RTLD_LAZY);
    if (dlhandle == NULL) {
        logError("file: "__FILE__", line: %d, "
                "dlopen %s fail, error info: %s", __LINE__,
                STORAGE_ENGINE_LIBRARY, dlerror());
        return EFAULT;
    }

    LOAD_API(STORAGE_ENGINE_INIT_API, fdir_storage_engine_init);
    LOAD_API(STORAGE_ENGINE_START_API, fdir_storage_engine_start);
    LOAD_API(STORAGE_ENGINE_TERMINATE_API, fdir_storage_engine_terminate);
    LOAD_API(STORAGE_ENGINE_ADD_INODE_API, fdir_storage_engine_add_inode);
    LOAD_API(STORAGE_ENGINE_SAVE_SEGMENT_INDEX_API,
            fdir_storage_engine_save_segment_index);
    LOAD_API(STORAGE_ENGINE_DUMP_INODE_BINLOGS_API,
            fdir_storage_engine_dump_inode_binlogs);
    LOAD_API(STORAGE_ENGINE_STORE_API, fdir_storage_engine_store);
    LOAD_API(STORAGE_ENGINE_REDO_API, fdir_storage_engine_redo);
    LOAD_API(STORAGE_ENGINE_FETCH_API, fdir_storage_engine_fetch);
    LOAD_API(STORAGE_ENGINE_SPACES_STAT_API, fdir_storage_engine_spaces_stat);

    return 0;
}

static int load_storage_engine_parames(IniFullContext *ini_ctx)
{
    int result;
    char *library;
    char *children_container;

    ini_ctx->section_name = "storage-engine";
    STORAGE_ENABLED = iniGetBoolValue(ini_ctx->section_name,
            "enabled", ini_ctx->context, false);
    if (!STORAGE_ENABLED) {
        return 0;
    }

    library = iniGetStrValue(ini_ctx->section_name,
            "library", ini_ctx->context);
    if (library == NULL) {
        library = "libfdirstorage.so";
    } else if (*library == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, empty library!",
                __LINE__, ini_ctx->filename, ini_ctx->section_name);
        return EINVAL;
    }
    if ((STORAGE_ENGINE_LIBRARY=fc_strdup(library)) == NULL) {
        return ENOMEM;
    }
    if ((result=load_storage_engine_apis()) != 0) {
        return result;
    }

    if ((result=sf_load_data_path_config_ex(ini_ctx, "data_path",
                    "db", &STORAGE_PATH)) != 0)
    {
        return result;
    }

    if (fc_string_equals(&STORAGE_PATH, &DATA_PATH)) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, storage path MUST be "
                "different from the global data path", __LINE__,
                ini_ctx->filename, ini_ctx->section_name);
        return EINVAL;
    }

    INODE_BINLOG_SUBDIRS = iniGetIntCorrectValue(ini_ctx,
            "inode_binlog_subdirs", INODE_BINLOG_DEFAULT_SUBDIRS,
            INODE_BINLOG_MIN_SUBDIRS, INODE_BINLOG_MAX_SUBDIRS);

    BATCH_STORE_ON_MODIFIES = iniGetIntValue(ini_ctx->section_name,
            "batch_store_on_modifies", ini_ctx->context,
            DEFAULT_BATCH_STORE_ON_MODIFIES);

    BATCH_STORE_INTERVAL = iniGetIntValue(ini_ctx->section_name,
            "batch_store_interval", ini_ctx->context,
            DEFAULT_BATCH_STORE_INTERVAL);

    INDEX_DUMP_INTERVAL = iniGetIntCorrectValue(ini_ctx,
            "index_dump_interval", DEFAULT_INDEX_DUMP_INTERVAL,
            1, 86400);

    if ((result=get_time_item_from_conf_ex(ini_ctx,
                    "index_dump_base_time",
                    &INDEX_DUMP_BASE_TIME,
                    0, 30, false)) != 0)
    {
        return result;
    }

    DENTRY_ELIMINATE_INTERVAL = iniGetIntValue(ini_ctx->section_name,
            "eliminate_interval", ini_ctx->context,
            DEFAULT_ELIMINATE_INTERVAL);
    if ((result=iniGetPercentValue(ini_ctx, "memory_limit",
                    &STORAGE_MEMORY_LIMIT, 0.80)) != 0)
    {
        return result;
    }

    if (STORAGE_MEMORY_LIMIT < 0.01) {
        logWarning("file: "__FILE__", line: %d, "
                "memory_limit: %%%.2f is too small, set to 1%%",
                __LINE__, STORAGE_MEMORY_LIMIT);
        STORAGE_MEMORY_LIMIT = 0.01;
    }
    if (STORAGE_MEMORY_LIMIT > 0.99) {
        logWarning("file: "__FILE__", line: %d, "
                "memory_limit: %%%.2f is too large, set to 99%%",
                __LINE__, STORAGE_MEMORY_LIMIT);
        STORAGE_MEMORY_LIMIT = 0.99;
    }

    children_container = iniGetStrValue(ini_ctx->section_name,
            "children_container", ini_ctx->context);
    if (children_container != NULL && strcasecmp(
                children_container, "skiplist") == 0)
    {
        CHILDREN_CONTAINER = fdir_children_container_skiplist;
    } else {
        CHILDREN_CONTAINER = fdir_children_container_sortedarray;
    }

    STORAGE_LOG_LEVEL_FOR_ENOENT = get_log_level(iniGetStrValue(
                ini_ctx->section_name, "log_level_for_enoent",
                ini_ctx->context), LOG_WARNING);

    return 0;
}

static void binlog_shrink_config_to_string(char *buff, const int size)
{
    int len;

    len = snprintf(buff, size, "binlog-shrink {"
            "dedup_enabled: %d", BINLOG_DEDUP_ENABLED);
    if (BINLOG_DEDUP_ENABLED) {
        len += snprintf(buff + len, size - len, ", target_dedup_ratio=%.2f%%"
                ", dedup_time=%02d:%02d", BINLOG_DEDUP_RATIO * 100.00,
                BINLOG_DEDUP_TIME.hour, BINLOG_DEDUP_TIME.minute);
    }
    len += snprintf(buff + len, size - len,
            ", keep_days: %d", BINLOG_KEEP_DAYS);
    if (BINLOG_KEEP_DAYS > 0) {
        len += snprintf(buff + len, size - len,
            ", delete_time=%02d:%02d}",
            BINLOG_DELETE_TIME.hour,
            BINLOG_DELETE_TIME.minute);
    } else {
        len += snprintf(buff + len, size - len, "}");
    }
}

static void server_log_configs()
{
    char sz_server_config[2048];
    char sz_global_config[512];
    char sz_slowlog_config[256];
    char sz_binlog_shrink_config[128];
    char sz_service_config[128];
    char sz_cluster_config[128];
    char sz_auth_config[1024];
    int len;

    sf_global_config_to_string(sz_global_config, sizeof(sz_global_config));
    sf_slow_log_config_to_string(&SLOW_LOG_CFG, "slow-log",
            sz_slowlog_config, sizeof(sz_slowlog_config));

    sf_context_config_to_string(&SERVICE_SF_CTX,
            sz_service_config, sizeof(sz_service_config));
    sf_context_config_to_string(&CLUSTER_SF_CTX,
            sz_cluster_config, sizeof(sz_cluster_config));

    fcfs_auth_for_server_cfg_to_string(&AUTH_CTX,
            sz_auth_config, sizeof(sz_auth_config));

    len = snprintf(sz_server_config, sizeof(sz_server_config),
            "cluster_id = %d, my server id = %d, posix_acl: %s, "
            "data_path = %s, data_threads = %d, "
            "dentry_max_data_size = %d, "
            "skiplist_max_level = %d, "
            "binlog_buffer_size = %d KB, "
            "slave_binlog_check_last_rows = %d, "
            "reload_interval_ms = %d ms, "
            "check_alive_interval = %d s, "
            "log_level_for_enoent = %s, "
            "namespace_hashtable_capacity = %d, "
            "node_hashtable_capacity = %d, "
            "inode_hashtable_capacity = %"PRId64", "
            "inode_shared_locks_count = %d, "
            "cluster server count = %d, "
            "master-election {quorum: %s, vote_node_enabled: %d, "
            "master_lost_timeout: %ds, max_wait_time: %ds, "
            "max_shutdown_duration: %ds}, "
            "data-replication {quorum: %s, deactive_on_failures: %d, "
            "quorum_need_majority: %d, quorum_need_detect: %d}, "
            "storage-engine { enabled: %d",
            CLUSTER_ID, CLUSTER_MY_SERVER_ID,
            (FDIR_POSIX_ACL == fdir_posix_acl_strict ? "strict" : "none"),
            DATA_PATH_STR, DATA_THREAD_COUNT,
            DENTRY_MAX_DATA_SIZE, SKIPLIST_MAX_LEVEL,
            BINLOG_BUFFER_SIZE / 1024,
            SLAVE_BINLOG_CHECK_LAST_ROWS,
            g_server_global_vars->reload_interval_ms,
            g_server_global_vars->check_alive_interval,
            get_log_level_caption(LOG_LEVEL_FOR_ENOENT),
            g_server_global_vars->namespace_hashtable_capacity,
            g_server_global_vars->node_hashtable_capacity,
            INODE_HASHTABLE_CAPACITY, INODE_SHARED_LOCKS_COUNT,
            FC_SID_SERVER_COUNT(CLUSTER_SERVER_CONFIG),
            sf_get_election_quorum_caption(MASTER_ELECTION_QUORUM),
            VOTE_NODE_ENABLED, ELECTION_MASTER_LOST_TIMEOUT,
            ELECTION_MAX_WAIT_TIME, ELECTION_MAX_SHUTDOWN_DURATION,
            sf_get_replication_quorum_caption(REPLICATION_QUORUM),
            REPLICA_QUORUM_DEACTIVE_ON_FAILURES,
            REPLICA_QUORUM_NEED_MAJORITY, REPLICA_QUORUM_NEED_DETECT,
            STORAGE_ENABLED);

    if (STORAGE_ENABLED) {
        len += snprintf(sz_server_config + len, sizeof(sz_server_config) - len,
                ", library: %s, data_path: %s, inode_binlog_subdirs: %d"
                ", batch_store_on_modifies: %d, batch_store_interval: %d s"
                ", index_dump_interval: %d s"
                ", index_dump_base_time: %02d:%02d"
                ", eliminate_interval: %d s, memory_limit: %.2f%%"
                ", children_container: %s, log_level_for_enoent: %s}",
                STORAGE_ENGINE_LIBRARY, STORAGE_PATH_STR,
                INODE_BINLOG_SUBDIRS, BATCH_STORE_ON_MODIFIES,
                BATCH_STORE_INTERVAL, INDEX_DUMP_INTERVAL,
                INDEX_DUMP_BASE_TIME.hour, INDEX_DUMP_BASE_TIME.minute,
                DENTRY_ELIMINATE_INTERVAL, STORAGE_MEMORY_LIMIT * 100,
                CHILDREN_CONTAINER == fdir_children_container_sortedarray ?
                "sortedarray" : "skiplist", get_log_level_caption(
                    STORAGE_LOG_LEVEL_FOR_ENOENT));
    } else {
        snprintf(sz_server_config + len, sizeof(sz_server_config) - len, "}");
    }

    binlog_shrink_config_to_string(sz_binlog_shrink_config,
            sizeof(sz_binlog_shrink_config));

    logInfo("fastDIR V%d.%d.%d, %s, %s, service: {%s}, "
            "cluster: {%s}", g_fdir_global_vars.version.major,
            g_fdir_global_vars.version.minor,
            g_fdir_global_vars.version.patch, sz_global_config,
            sz_slowlog_config, sz_service_config, sz_cluster_config);
    logInfo("%s, %s, %s", sz_server_config, sz_binlog_shrink_config,
            sz_auth_config);
    log_local_host_ip_addrs();
    log_cluster_server_config();
}

static int load_binlog_buffer_size(IniFullContext *ini_ctx)
{
    int64_t bytes;

    bytes = iniGetByteCorrectValue(ini_ctx, "binlog_buffer_size",
            FDIR_DEFAULT_BINLOG_BUFFER_SIZE, 1, 256 * 1024 * 1024);
    if (bytes < 4096) {
        logWarning("file: "__FILE__", line: %d, "
                "config file: %s , binlog_buffer_size: %d is too small, "
                "set it to default: %d", __LINE__, ini_ctx->filename,
                BINLOG_BUFFER_SIZE, FDIR_DEFAULT_BINLOG_BUFFER_SIZE);
        BINLOG_BUFFER_SIZE = FDIR_DEFAULT_BINLOG_BUFFER_SIZE;
    } else {
        BINLOG_BUFFER_SIZE = bytes;
    }

    return 0;
}

static int load_binlog_shrink_config(IniContext *ini_context,
        const char *filename)
{
    const char *section_name = "binlog-shrink";
    int result;
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, section_name, ini_context);
    BINLOG_DEDUP_ENABLED = iniGetBoolValue(section_name,
            "dedup_enabled", ini_context, true);
    if ((result=iniGetPercentValue(&ini_ctx, "target_dedup_ratio",
                    &BINLOG_DEDUP_RATIO, 0.20)) != 0)
    {
        return result;
    }

    if ((result=get_time_item_from_conf_ex(&ini_ctx, "dedup_time",
                    &BINLOG_DEDUP_TIME, 2, 0, false)) != 0)
    {
        return result;
    }

    BINLOG_KEEP_DAYS = iniGetIntValue(section_name,
            "keep_days", ini_context, 180);
    if ((result=get_time_item_from_conf_ex(&ini_ctx, "delete_time",
                    &BINLOG_DELETE_TIME, 5, 0, false)) != 0)
    {
        return result;
    }

    return 0;
}

int server_load_config(const char *filename)
{
    const bool double_buffers = true;
    const int task_buffer_extra_size = 0;
    IniFullContext ini_ctx;
    IniContext ini_context;
    char full_cluster_filename[PATH_MAX];
    DADataConfig data_cfg;
    FCServerGroupInfo *server_group;
    SFNetworkHandler *rdma_handler;
    SFNetworkHandler *service_handler;
    bool clear_segment_index;
    int result;

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, NULL, &ini_context);
    if ((result=sf_load_data_path_config(&ini_ctx, &DATA_PATH)) != 0) {
        return result;
    }

    load_local_host_ip_addrs();
    if ((result=load_cluster_config(&ini_ctx,
                    full_cluster_filename)) != 0)
    {
        return result;
    }

    server_group = fc_server_get_group_by_index(
            &CLUSTER_SERVER_CONFIG, SERVICE_GROUP_INDEX);
    if ((result=sf_load_config(NULL, server_group->comm_type,
                    filename, &ini_context, "service",
                    FDIR_SERVER_DEFAULT_SERVICE_PORT,
                    FDIR_SERVER_DEFAULT_SERVICE_PORT,
                    server_group->buffer_size,
                    task_buffer_extra_size)) != 0)
    {
        return result;
    }
    sf_service_set_smart_polling(&server_group->smart_polling);

    CLUSTER_SERVER_GROUP = fc_server_get_group_by_index(
            &CLUSTER_SERVER_CONFIG, CLUSTER_GROUP_INDEX);
    if ((result=sf_load_context_from_config(&CLUSTER_SF_CTX,
                    CLUSTER_SERVER_GROUP->comm_type, filename,
                    &ini_context, "cluster",
                    FDIR_SERVER_DEFAULT_CLUSTER_PORT,
                    FDIR_SERVER_DEFAULT_CLUSTER_PORT,
                    CLUSTER_SERVER_GROUP->buffer_size,
                    task_buffer_extra_size)) != 0)
    {
        return result;
    }
    sf_service_set_smart_polling_ex(&CLUSTER_SF_CTX,
            &CLUSTER_SERVER_GROUP->smart_polling);

    if ((result=cluster_info_init(full_cluster_filename)) != 0) {
        return result;
    }

    sf_set_address_family_by_ip(&SERVICE_SF_CTX, &SERVICE_GROUP_ADDRESS_ARRAY(
                CLUSTER_MYSELF_PTR->server));
    sf_set_address_family_by_ip(&CLUSTER_SF_CTX, &CLUSTER_GROUP_ADDRESS_ARRAY(
                CLUSTER_MYSELF_PTR->server));

    CLUSTER_NET_HANDLER = sf_get_first_network_handler_ex(&CLUSTER_SF_CTX);
    if ((rdma_handler=sf_get_rdma_network_handler2(&SERVICE_SF_CTX,
                    &CLUSTER_SF_CTX)) != NULL)
    {
        if ((result=sf_alloc_rdma_pd(&SERVICE_SF_CTX,
                        &SERVICE_GROUP_ADDRESS_ARRAY(
                            CLUSTER_MYSELF_PTR->server))) != 0)
        {
            return result;
        }
        if ((result=sf_alloc_rdma_pd(&CLUSTER_SF_CTX,
                        &CLUSTER_GROUP_ADDRESS_ARRAY(
                            CLUSTER_MYSELF_PTR->server))) != 0)
        {
            return result;
        }

        TASK_PADDING_SIZE = rdma_handler->get_connection_size();
        RDMA_INIT_CONNECTION = rdma_handler->init_connection;
        CLUSTER_RDMA_PD = CLUSTER_NET_HANDLER->pd;

        service_handler = sf_get_first_network_handler_ex(&SERVICE_SF_CTX);
        SERVICE_RDMA_PD = service_handler->pd;
    }

    if ((result=load_dentry_max_data_size(&ini_ctx)) != 0) {
        return result;
    }

    SKIPLIST_MAX_LEVEL = iniGetIntCorrectValue(&ini_ctx,
            "skiplist_max_level", 12, 8, 20);

    if ((result=load_posix_acl(&ini_ctx)) != 0) {
        return result;
    }

    DATA_THREAD_COUNT = iniGetIntValue(NULL, "data_threads",
            &ini_context, FDIR_DEFAULT_DATA_THREAD_COUNT);
    if (DATA_THREAD_COUNT <= 0) {
        DATA_THREAD_COUNT = FDIR_DEFAULT_DATA_THREAD_COUNT;
    }

    if ((result=load_binlog_buffer_size(&ini_ctx)) != 0) {
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

    g_server_global_vars->reload_interval_ms = iniGetIntValue(NULL,
            "reload_interval_ms", &ini_context,
            FDIR_SERVER_DEFAULT_RELOAD_INTERVAL);
    if (g_server_global_vars->reload_interval_ms <= 0) {
        g_server_global_vars->reload_interval_ms =
            FDIR_SERVER_DEFAULT_RELOAD_INTERVAL;
    }

    g_server_global_vars->check_alive_interval = iniGetIntValue(NULL,
            "check_alive_interval", &ini_context,
            FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL);
    if (g_server_global_vars->check_alive_interval <= 0) {
        g_server_global_vars->check_alive_interval =
            FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL;
    }

    g_server_global_vars->namespace_hashtable_capacity = iniGetIntValue(NULL,
            "namespace_hashtable_capacity", &ini_context,
            FDIR_NAMESPACE_HASHTABLE_DEFAULT_CAPACITY);
    if (g_server_global_vars->namespace_hashtable_capacity <= 0) {
        g_server_global_vars->namespace_hashtable_capacity =
            FDIR_NAMESPACE_HASHTABLE_DEFAULT_CAPACITY;
    }

    g_server_global_vars->node_hashtable_capacity = iniGetIntValue(NULL,
            "node_hashtable_capacity", &ini_context,
            FDIR_NODE_HASHTABLE_DEFAULT_CAPACITY);
    if (g_server_global_vars->node_hashtable_capacity <= 0) {
        g_server_global_vars->node_hashtable_capacity =
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

    if ((result=sf_load_slow_log_config(filename, &ini_context,
                    &SLOW_LOG_CTX, &SLOW_LOG_CFG)) != 0)
    {
        return result;
    }

    LOG_LEVEL_FOR_ENOENT = get_log_level(iniGetStrValue(NULL,
                "log_level_for_enoent", &ini_context), LOG_WARNING);

    fcfs_auth_client_init_full_ctx(&AUTH_CTX);
    if ((result=fcfs_auth_for_server_init(&AUTH_CTX, &ini_ctx,
                    full_cluster_filename)) != 0)
    {
        return result;
    }

    if ((result=load_storage_engine_parames(&ini_ctx)) != 0) {
        return result;
    }

    if ((SYSTEM_CPU_COUNT=get_sys_cpu_count()) <= 0) {
        logCrit("file: "__FILE__", line: %d, "
                "get CPU count fail", __LINE__);
        return EINVAL;
    }

    if ((result=get_sys_total_mem_size(&SYSTEM_TOTAL_MEMORY)) != 0) {
        return result;
    }

    if (DENTRY_ELIMINATE_INTERVAL > 0) {
        g_server_global_vars->storage.cfg.memory_limit = (int64_t)
            (SYSTEM_TOTAL_MEMORY * STORAGE_MEMORY_LIMIT *
             MEMORY_LIMIT_INODE_RATIO);
        if (g_server_global_vars->storage.cfg.memory_limit < 64 * 1024 * 1024) {
            g_server_global_vars->storage.cfg.memory_limit = 64 * 1024 * 1024;
        }
    } else {
        g_server_global_vars->storage.cfg.memory_limit = 0;  //no limit
    }

    data_cfg.path = STORAGE_PATH;
    data_cfg.binlog_buffer_size = BINLOG_BUFFER_SIZE;
    data_cfg.binlog_subdirs = INODE_BINLOG_SUBDIRS;
    data_cfg.trunk_index_dump_interval = INDEX_DUMP_INTERVAL;
    data_cfg.trunk_index_dump_base_time = INDEX_DUMP_BASE_TIME;
    if (STORAGE_ENABLED) {
        if ((result=inode_add_mark_load(&DUMP_INODE_ADD_STATUS)) != 0) {
            return result;
        }
        if (DUMP_INODE_ADD_STATUS != inode_add_mark_status_none &&
                fc_get_path_child_count(STORAGE_PATH.str) == 0)
        {
            DUMP_INODE_ADD_STATUS = inode_add_mark_status_none;
            if ((result=inode_add_mark_save(DUMP_INODE_ADD_STATUS)) != 0) {
                return result;
            }
        }

        clear_segment_index = (DUMP_INODE_ADD_STATUS ==
                inode_add_mark_status_doing);
        if ((result=STORAGE_ENGINE_INIT_API(&ini_ctx, CLUSTER_MY_SERVER_ID,
                        &g_server_global_vars->storage.cfg, &data_cfg,
                        clear_segment_index)) != 0)
        {
            return result;
        }
    }

    if ((result=load_binlog_shrink_config(&ini_context, filename)) != 0) {
        return result;
    }

    if (CLUSTER_SERVER_GROUP->comm_type != fc_comm_type_sock) {
        if ((result=conn_pool_global_init_for_rdma()) != 0) {
            return result;
        }
    }
    if ((result=conn_pool_set_rdma_extra_params_ex(
                    &CLUSTER_CONN_EXTRA_PARAMS, &CLUSTER_SERVER_CONFIG,
                    CLUSTER_GROUP_INDEX, double_buffers)) != 0)
    {
        return result;
    }

    iniFreeContext(&ini_context);
    server_log_configs();
    return 0;
}

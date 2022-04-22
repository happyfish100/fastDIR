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


#ifndef _FDIR_SERVER_GLOBAL_H
#define _FDIR_SERVER_GLOBAL_H

#include "fastcommon/common_define.h"
#include "fastcommon/server_id_func.h"
#include "fastcommon/thread_pool.h"
#include "fastcommon/array_allocator.h"
#include "sf/sf_global.h"
#include "sf/sf_cluster_cfg.h"
#include "db/db_interface.h"
#include "fastcfs/auth/client_types.h"
#include "common/fdir_global.h"
#include "server_types.h"

typedef struct server_global_vars {

    int namespace_hashtable_capacity;

    int dentry_max_data_size;

    int reload_interval_ms;

    int check_alive_interval;

    struct {
        int cpu_count;
        int64_t total_memory;
    } system;

    struct {
        FCFSAuthClientFullContext auth;
        uint16_t id;  //cluster id for generate inode
        FDIRClusterServerInfo *master;
        FDIRClusterServerInfo *myself;
        SFClusterConfig config;
        FDIRClusterServerArray server_array;

        struct {
            SFElectionQuorum quorum;
            bool force;
            int master_lost_timeout;
            int max_wait_time;
        } master_election;

        SFContext sf_context;  //for cluster communication
    } cluster;

    struct {
        struct {
            int64_t cluster;      //cluster id part
            volatile int64_t sn;  //sn part
        } generator;

        struct {
            int shared_locks_count;
            int64_t hashtable_capacity;
        } entries;
    } inode;

    struct {
        volatile uint64_t current_version; //binlog version
        string_t path;   //data path
        int binlog_buffer_size;
        int slave_binlog_check_last_rows;
        int thread_count;
        bool load_done;
    } data;  //for binlog

    struct {
        bool enabled;
        bool read_by_direct_io;
        int batch_store_on_modifies;
        int batch_store_interval;
        int eliminate_interval;
        FDIRStorageEngineConfig cfg;
        double memory_limit;   //ratio
        char *library;
        FDIRStorageEngineInterface api;
    } storage;

    SFSlowLogContext slow_log;

    FCThreadPool thread_pool;

    ArrayAllocatorContext dentry_parray_allocator;

} FDIRServerGlobalVars;

#define SYSTEM_CPU_COUNT       g_server_global_vars.system.cpu_count
#define SYSTEM_TOTAL_MEMORY    g_server_global_vars.system.total_memory

#define MASTER_ELECTION_QUORUM g_server_global_vars.cluster. \
    master_election.quorum
#define FORCE_MASTER_ELECTION  g_server_global_vars.cluster. \
    master_election.force
#define ELECTION_MASTER_LOST_TIMEOUT g_server_global_vars.cluster. \
    master_election.master_lost_timeout
#define ELECTION_MAX_WAIT_TIME   g_server_global_vars.cluster. \
    master_election.max_wait_time

#define CLUSTER_CONFIG          g_server_global_vars.cluster.config
#define CLUSTER_SERVER_CONFIG   CLUSTER_CONFIG.server_cfg
#define AUTH_CTX                g_server_global_vars.cluster.auth
#define AUTH_CLIENT_CTX         AUTH_CTX.ctx
#define AUTH_ENABLED            AUTH_CTX.enabled

#define CLUSTER_MYSELF_PTR      g_server_global_vars.cluster.myself
#define MYSELF_IS_MASTER        __sync_add_and_fetch( \
        &CLUSTER_MYSELF_PTR->is_master, 0)
#define CLUSTER_MASTER_PTR      g_server_global_vars.cluster.master
#define CLUSTER_MASTER_ATOM_PTR ((FDIRClusterServerInfo *)__sync_add_and_fetch( \
        &CLUSTER_MASTER_PTR, 0))


#define CLUSTER_SERVER_ARRAY    g_server_global_vars.cluster.server_array

#define CLUSTER_ID              g_server_global_vars.cluster.id
#define CLUSTER_MY_SERVER_ID    CLUSTER_MYSELF_PTR->server->id

#define CLUSTER_SF_CTX          g_server_global_vars.cluster.sf_context

#define DENTRY_MAX_DATA_SIZE    g_server_global_vars.dentry_max_data_size
#define BINLOG_BUFFER_SIZE      g_server_global_vars.data.binlog_buffer_size
#define SLAVE_BINLOG_CHECK_LAST_ROWS  g_server_global_vars.data. \
    slave_binlog_check_last_rows

#define CURRENT_INODE_SN        g_server_global_vars.inode.generator.sn
#define INODE_CLUSTER_PART      g_server_global_vars.inode.generator.cluster
#define INODE_SHARED_LOCKS_COUNT g_server_global_vars.inode.entries.shared_locks_count
#define INODE_HASHTABLE_CAPACITY g_server_global_vars.inode.entries.hashtable_capacity
#define DATA_CURRENT_VERSION    g_server_global_vars.data.current_version
#define DATA_THREAD_COUNT       g_server_global_vars.data.thread_count
#define DATA_LOAD_DONE          g_server_global_vars.data.load_done
#define DATA_PATH               g_server_global_vars.data.path
#define DATA_PATH_STR           DATA_PATH.str
#define DATA_PATH_LEN           DATA_PATH.len


#define STORAGE_ENABLED         g_server_global_vars.storage.enabled
#define STORAGE_PATH            g_server_global_vars.storage.cfg.path
#define STORAGE_PATH_STR        STORAGE_PATH.str
#define STORAGE_PATH_LEN        STORAGE_PATH.len

#define STORAGE_ENGINE_LIBRARY  g_server_global_vars.storage.library
#define BATCH_STORE_INTERVAL    g_server_global_vars.storage.batch_store_interval
#define BATCH_STORE_ON_MODIFIES g_server_global_vars.storage.batch_store_on_modifies
#define INODE_BINLOG_SUBDIRS    g_server_global_vars.storage.cfg.inode_binlog_subdirs
#define INDEX_DUMP_INTERVAL     g_server_global_vars.storage.cfg.index_dump_interval
#define INDEX_DUMP_BASE_TIME    g_server_global_vars.storage.cfg.index_dump_base_time
#define DENTRY_ELIMINATE_INTERVAL g_server_global_vars.storage.eliminate_interval
#define STORAGE_MEMORY_LIMIT      g_server_global_vars.storage.memory_limit
#define READ_BY_DIRECT_IO         g_server_global_vars.storage.read_by_direct_io

#define STORAGE_ENGINE_INIT_API      g_server_global_vars.storage.api.init
#define STORAGE_ENGINE_START_API     g_server_global_vars.storage.api.start
#define STORAGE_ENGINE_TERMINATE_API g_server_global_vars.storage.api.terminate
#define STORAGE_ENGINE_STORE_API     g_server_global_vars.storage.api.store
#define STORAGE_ENGINE_REDO_API      g_server_global_vars.storage.api.redo
#define STORAGE_ENGINE_FETCH_API     g_server_global_vars.storage.api.fetch

#define SLOW_LOG                g_server_global_vars.slow_log
#define SLOW_LOG_CFG            SLOW_LOG.cfg
#define SLOW_LOG_CTX            SLOW_LOG.ctx

#define THREAD_POOL             g_server_global_vars.thread_pool
#define DENTRY_PARRAY_ALLOCATOR g_server_global_vars.dentry_parray_allocator

#define SLAVE_SERVER_COUNT      (FC_SID_SERVER_COUNT(CLUSTER_SERVER_CONFIG) - 1)

#define REPLICA_KEY_BUFF        CLUSTER_MYSELF_PTR->key

#define CLUSTER_GROUP_INDEX     g_server_global_vars.cluster.config.cluster_group_index
#define SERVICE_GROUP_INDEX     g_server_global_vars.cluster.config.service_group_index

#define CLUSTER_GROUP_ADDRESS_ARRAY(server) \
    (server)->group_addrs[CLUSTER_GROUP_INDEX].address_array
#define SERVICE_GROUP_ADDRESS_ARRAY(server) \
    (server)->group_addrs[SERVICE_GROUP_INDEX].address_array

#define CLUSTER_GROUP_ADDRESS_FIRST_PTR(server) \
    (*(server)->group_addrs[CLUSTER_GROUP_INDEX].address_array.addrs)
#define SERVICE_GROUP_ADDRESS_FIRST_PTR(server) \
    (*(server)->group_addrs[SERVICE_GROUP_INDEX].address_array.addrs)

#define CLUSTER_GROUP_ADDRESS_FIRST_IP(server) \
    CLUSTER_GROUP_ADDRESS_FIRST_PTR(server)->conn.ip_addr
#define CLUSTER_GROUP_ADDRESS_FIRST_PORT(server) \
    CLUSTER_GROUP_ADDRESS_FIRST_PTR(server)->conn.port

#define SERVICE_GROUP_ADDRESS_FIRST_IP(server) \
    SERVICE_GROUP_ADDRESS_FIRST_PTR(server)->conn.ip_addr
#define SERVICE_GROUP_ADDRESS_FIRST_PORT(server) \
    SERVICE_GROUP_ADDRESS_FIRST_PTR(server)->conn.port

#define CLUSTER_CONFIG_SIGN_BUF g_server_global_vars.cluster.config.md5_digest

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRServerGlobalVars g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif

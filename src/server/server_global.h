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
#include "sf/idempotency/server/request_metadata.h"
#include "db/db_interface.h"
#include "db/inode_add_mark.h"
#include "fastcfs/auth/client_types.h"
#include "common/fdir_global.h"
#include "server_types.h"

struct ibv_pd;
typedef struct server_global_vars {

    int namespace_hashtable_capacity;
    int node_hashtable_capacity;

    int dentry_max_data_size;
    int skiplist_max_level;
    FDIRPosixACLPolicy posix_acl;

    int reload_interval_ms;

    int check_alive_interval;

    struct {
        int cpu_count;
        int64_t total_memory;
    } system;

    struct {
        const char *program_filename;
        const char *config_filename;
    } cmdline;

    struct {
        int task_padding_size;
        sf_init_connection_callback init_connection;
        struct ibv_pd *cluster_pd;
        struct ibv_pd *service_pd;
    } rdma;

    struct {
        FCFSAuthClientFullContext auth;
        uint16_t id;  //cluster id for generate inode
        volatile char is_my_term;
        FDIRClusterServerInfo *next_master;
        FDIRClusterServerInfo *master;
        FDIRClusterServerInfo *myself;
        SFClusterConfig config;
        FDIRClusterServerArray server_array;

        struct {
            SFElectionQuorum quorum;
            bool force;
            bool vote_node_enabled;
            int master_lost_timeout;
            int max_wait_time;
            int max_shutdown_duration;
        } master_election;

        /* follower ping master or master check brain-split */
        volatile time_t last_heartbeat_time;
        time_t last_shutdown_time;

        SFContext sf_context;  //for cluster communication
        FCServerGroupInfo *server_group;
        SFNetworkHandler *network_handler;
        ConnectionExtraParams conn_extra_params;
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
        volatile uint64_t current_version;   //binlog data version
        string_t path;   //data path
        int binlog_buffer_size;
        int slave_binlog_check_last_rows;
        int thread_count;
        volatile int add_inode_flags; //for store engine to load dump data
        volatile bool load_done;
        struct fdir_namespace_manager *ns_manager;
    } data;

    struct {
        SFReplicationQuorum quorum;
        int deactive_on_failures;

        /* cached result of SF_REPLICATION_QUORUM_NEED_MAJORITY */
        volatile char quorum_need_majority;

        /* cached result of SF_REPLICATION_QUORUM_NEED_DETECT */
        bool quorum_need_detect;

        int active_test_interval;

        IdempotencyRequestMetadataContext req_meta_ctx;

        /* slave side only */
        volatile struct replica_consumer_thread_context *consumer_ctx;
    } replication;

    struct {
        bool dedup_enabled;
        double target_dedup_ratio;
        TimeInfo dedup_time;
        volatile int64_t record_count;
        int keep_days;
        TimeInfo delete_time;
    } binlog;

    struct {
        volatile char dumping;  //if dump data in progress
        InodeAddMarkStatus inode_add_status; //for storage engine
        int64_t dentry_count;
        int64_t last_data_version;
        SFBinlogFilePosition next_position; //for normal binlog
    } full_dump;

    struct {
        bool enabled;
        int log_level_for_enoent;
        int batch_store_on_modifies;
        int batch_store_interval;
        int eliminate_interval;
        FDIRStorageEngineConfig cfg;
        double memory_limit;   //ratio
        FDIRChildrenContainer children_container;
        char *library;
        FDIRStorageEngineInterface api;
    } storage;

    int log_level_for_enoent;
    SFSlowLogContext slow_log;

    FCThreadPool thread_pool;

    ArrayAllocatorContext dentry_parray_allocator;

} FDIRServerGlobalVars;

#define SYSTEM_CPU_COUNT       g_server_global_vars->system.cpu_count
#define SYSTEM_TOTAL_MEMORY    g_server_global_vars->system.total_memory

#define CMDLINE_PROGRAM_FILENAME g_server_global_vars->cmdline.program_filename
#define CMDLINE_CONFIG_FILENAME  g_server_global_vars->cmdline.config_filename

#define TASK_PADDING_SIZE        g_server_global_vars->rdma.task_padding_size
#define RDMA_INIT_CONNECTION     g_server_global_vars->rdma.init_connection
#define CLUSTER_RDMA_PD          g_server_global_vars->rdma.cluster_pd
#define SERVICE_RDMA_PD          g_server_global_vars->rdma.service_pd

#define MASTER_ELECTION_QUORUM g_server_global_vars->cluster. \
    master_election.quorum
#define VOTE_NODE_ENABLED      g_server_global_vars->cluster. \
    master_election.vote_node_enabled
#define FORCE_MASTER_ELECTION  g_server_global_vars->cluster. \
    master_election.force
#define ELECTION_MASTER_LOST_TIMEOUT g_server_global_vars->cluster. \
    master_election.master_lost_timeout
#define ELECTION_MAX_WAIT_TIME g_server_global_vars->cluster. \
    master_election.max_wait_time
#define ELECTION_MAX_SHUTDOWN_DURATION g_server_global_vars->cluster. \
    master_election.max_shutdown_duration

#define CLUSTER_LAST_HEARTBEAT_TIME g_server_global_vars-> \
    cluster.last_heartbeat_time
#define CLUSTER_LAST_SHUTDOWN_TIME  g_server_global_vars-> \
    cluster.last_shutdown_time

#define CLUSTER_CONFIG          g_server_global_vars->cluster.config
#define CLUSTER_SERVER_CONFIG   CLUSTER_CONFIG.server_cfg
#define AUTH_CTX                g_server_global_vars->cluster.auth
#define AUTH_CLIENT_CTX         AUTH_CTX.ctx
#define AUTH_ENABLED            AUTH_CTX.enabled

#define MYSELF_IN_MASTER_TERM   g_server_global_vars->cluster.is_my_term
#define CLUSTER_MYSELF_PTR      g_server_global_vars->cluster.myself
#define MYSELF_IS_OLD_MASTER    __sync_add_and_fetch( \
        &CLUSTER_MYSELF_PTR->is_old_master, 0)

#define CLUSTER_MASTER_PTR      g_server_global_vars->cluster.master
#define CLUSTER_MASTER_ATOM_PTR ((FDIRClusterServerInfo *)__sync_add_and_fetch( \
        &CLUSTER_MASTER_PTR, 0))

#define CLUSTER_NEXT_MASTER     g_server_global_vars->cluster.next_master

#define CLUSTER_SERVER_ARRAY    g_server_global_vars->cluster.server_array

#define CLUSTER_ID              g_server_global_vars->cluster.id
#define CLUSTER_MY_SERVER_ID    CLUSTER_MYSELF_PTR->server->id
#define MY_CONFIRMED_VERSION    CLUSTER_MYSELF_PTR->confirmed_data_version

#define SERVICE_SF_CTX          g_sf_context
#define CLUSTER_SF_CTX          g_server_global_vars->cluster.sf_context
#define CLUSTER_SERVER_GROUP    g_server_global_vars->cluster.server_group
#define CLUSTER_NET_HANDLER     g_server_global_vars->cluster.network_handler
#define CLUSTER_CONN_EXTRA_PARAMS g_server_global_vars->cluster.conn_extra_params

#define SERVICE_NETWORK_TIMEOUT  SERVICE_SF_CTX.net_buffer_cfg.connect_timeout
#define CLUSTER_CONNECT_TIMEOUT  CLUSTER_SF_CTX.net_buffer_cfg.connect_timeout
#define CLUSTER_NETWORK_TIMEOUT  CLUSTER_SF_CTX.net_buffer_cfg.network_timeout

#define REPLICATION_QUORUM           g_server_global_vars->replication.quorum
#define REPLICA_QUORUM_NEED_MAJORITY g_server_global_vars-> \
    replication.quorum_need_majority
#define REPLICA_QUORUM_NEED_DETECT   g_server_global_vars-> \
    replication.quorum_need_detect
#define REPLICA_QUORUM_DEACTIVE_ON_FAILURES g_server_global_vars-> \
    replication.deactive_on_failures
#define REPLICA_REQ_META_CTX         g_server_global_vars-> \
    replication.req_meta_ctx
#define REPLICA_CONSUMER_CTX    g_server_global_vars->replication.consumer_ctx

#define DENTRY_MAX_DATA_SIZE    g_server_global_vars->dentry_max_data_size
#define SKIPLIST_MAX_LEVEL      g_server_global_vars->skiplist_max_level
#define FDIR_POSIX_ACL          g_server_global_vars->posix_acl
#define FDIR_USE_POSIX_ACL      (FDIR_POSIX_ACL == fdir_posix_acl_strict)

#define BINLOG_BUFFER_SIZE      g_server_global_vars->data.binlog_buffer_size
#define SLAVE_BINLOG_CHECK_LAST_ROWS  g_server_global_vars->data. \
    slave_binlog_check_last_rows

#define FDIR_NS_MANAGER         g_server_global_vars->data.ns_manager

#define CURRENT_INODE_SN        g_server_global_vars->inode.generator.sn
#define INODE_CLUSTER_PART      g_server_global_vars->inode.generator.cluster
#define INODE_SHARED_LOCKS_COUNT g_server_global_vars->inode.entries.shared_locks_count
#define INODE_HASHTABLE_CAPACITY g_server_global_vars->inode.entries.hashtable_capacity
#define DATA_CURRENT_VERSION    g_server_global_vars->data.current_version
#define DATA_THREAD_COUNT       g_server_global_vars->data.thread_count
#define DATA_LOAD_DONE          g_server_global_vars->data.load_done
#define ADD_INODE_FLAGS         g_server_global_vars->data.add_inode_flags
#define DATA_PATH               g_server_global_vars->data.path
#define DATA_PATH_STR           DATA_PATH.str
#define DATA_PATH_LEN           DATA_PATH.len

#define STORAGE_ENABLED         g_server_global_vars->storage.enabled
#define STORAGE_PATH            g_server_global_vars->storage.cfg.path
#define STORAGE_PATH_STR        STORAGE_PATH.str
#define STORAGE_PATH_LEN        STORAGE_PATH.len

#define STORAGE_ENGINE_LIBRARY  g_server_global_vars->storage.library
#define BATCH_STORE_INTERVAL    g_server_global_vars->storage.batch_store_interval
#define BATCH_STORE_ON_MODIFIES g_server_global_vars->storage.batch_store_on_modifies
#define INODE_BINLOG_SUBDIRS    g_server_global_vars->storage.cfg.inode_binlog_subdirs
#define INDEX_DUMP_INTERVAL     g_server_global_vars->storage.cfg.index_dump_interval
#define INDEX_DUMP_BASE_TIME    g_server_global_vars->storage.cfg.index_dump_base_time
#define DENTRY_ELIMINATE_INTERVAL g_server_global_vars->storage.eliminate_interval
#define STORAGE_MEMORY_LIMIT      g_server_global_vars->storage.memory_limit
#define CHILDREN_CONTAINER        g_server_global_vars->storage.children_container
#define STORAGE_LOG_LEVEL_FOR_ENOENT g_server_global_vars->storage.log_level_for_enoent

#define STORAGE_ENGINE_INIT_API      g_server_global_vars->storage.api.init
#define STORAGE_ENGINE_START_API     g_server_global_vars->storage.api.start
#define STORAGE_ENGINE_TERMINATE_API g_server_global_vars->storage.api.terminate
#define STORAGE_ENGINE_ADD_INODE_API g_server_global_vars->storage.api.add_inode
#define STORAGE_ENGINE_SAVE_SEGMENT_INDEX_API   \
    g_server_global_vars->storage.api.save_segment_index
#define STORAGE_ENGINE_DUMP_INODE_BINLOGS_API   \
    g_server_global_vars->storage.api.dump_inode_binlogs
#define STORAGE_ENGINE_STORE_API     g_server_global_vars->storage.api.store
#define STORAGE_ENGINE_REDO_API      g_server_global_vars->storage.api.redo
#define STORAGE_ENGINE_FETCH_API     g_server_global_vars->storage.api.fetch
#define STORAGE_ENGINE_SPACES_STAT_API     \
    g_server_global_vars->storage.api.spaces_stat

#define FULL_DUMPING           g_server_global_vars->full_dump.dumping
#define DUMP_INODE_ADD_STATUS  g_server_global_vars->full_dump.inode_add_status
#define DUMP_DENTRY_COUNT      g_server_global_vars->full_dump.dentry_count
#define DUMP_LAST_DATA_VERSION g_server_global_vars->full_dump.last_data_version
#define DUMP_NEXT_POSITION     g_server_global_vars->full_dump.next_position

#define BINLOG_RECORD_COUNT  g_server_global_vars->binlog.record_count
#define BINLOG_DEDUP_ENABLED g_server_global_vars->binlog.dedup_enabled
#define BINLOG_DEDUP_RATIO   g_server_global_vars->binlog.target_dedup_ratio
#define BINLOG_DEDUP_TIME    g_server_global_vars->binlog.dedup_time
#define BINLOG_KEEP_DAYS     g_server_global_vars->binlog.keep_days
#define BINLOG_DELETE_TIME   g_server_global_vars->binlog.delete_time

#define LOG_LEVEL_FOR_ENOENT    g_server_global_vars->log_level_for_enoent
#define SLOW_LOG                g_server_global_vars->slow_log
#define SLOW_LOG_CFG            SLOW_LOG.cfg
#define SLOW_LOG_CTX            SLOW_LOG.ctx

#define THREAD_POOL             g_server_global_vars->thread_pool
#define DENTRY_PARRAY_ALLOCATOR g_server_global_vars->dentry_parray_allocator

#define SLAVE_SERVER_COUNT      (FC_SID_SERVER_COUNT(CLUSTER_SERVER_CONFIG) - 1)
#define REPLICA_KEY_BUFF        CLUSTER_MYSELF_PTR->key

#define CLUSTER_GROUP_INDEX     g_server_global_vars->cluster.config.cluster_group_index
#define SERVICE_GROUP_INDEX     g_server_global_vars->cluster.config.service_group_index

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

#define CLUSTER_CONFIG_SIGN_BUF g_server_global_vars->cluster.config.md5_digest

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRServerGlobalVars *g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif

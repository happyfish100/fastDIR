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
#include "sf/sf_global.h"
#include "sf/sf_cluster_cfg.h"
#include "fastcfs/auth/client_types.h"
#include "common/fdir_global.h"
#include "server_types.h"

typedef struct server_global_vars {

    int namespace_hashtable_capacity;

    int dentry_max_data_size;

    int reload_interval_ms;

    int check_alive_interval;

    struct {
        FCFSAuthClientFullContext auth;
        uint16_t id;  //cluster id for generate inode
        FDIRClusterServerInfo *master;
        FDIRClusterServerInfo *myself;
        SFClusterConfig config;
        FDIRClusterServerArray server_array;

        struct {
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
    } data;

    SFSlowLogContext slow_log;

} FDIRServerGlobalVars;

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
#define DATA_PATH               g_server_global_vars.data.path
#define DATA_PATH_STR           DATA_PATH.str
#define DATA_PATH_LEN           DATA_PATH.len

#define SLOW_LOG                g_server_global_vars.slow_log
#define SLOW_LOG_CFG            SLOW_LOG.cfg
#define SLOW_LOG_CTX            SLOW_LOG.ctx

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

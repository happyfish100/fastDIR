
#ifndef _FDIR_SERVER_GLOBAL_H
#define _FDIR_SERVER_GLOBAL_H

#include "fastcommon/common_define.h"
#include "fastcommon/server_id_func.h"
#include "server_types.h"

typedef struct server_global_vars {
    struct {
        string_t username;
        string_t secret_key;
    } admin;

    int namespace_hashtable_capacity;

    int reload_interval_ms;

    int check_alive_interval;

    struct {
        bool is_master;  //if I am master
        FCServerInfo *myself;
        struct {
            FCServerConfig ctx;
            unsigned char md5_digest[16];
            int cluster_group_index;
            int service_group_index;
        } config;
        FDIRServerCluster top; //topology
    } cluster;

    struct {
        int64_t version;
    } data;
} FDIRServerGlobalVars;

#define CLUSTER_CONFIG_CTX      g_server_global_vars.cluster.config.ctx

#define MYSELF_IS_MASTER        g_server_global_vars.cluster.is_master
#define CLUSTER_MYSELF_PTR      g_server_global_vars.cluster.myself
#define CLUSTER_MASTER_PTR      g_server_global_vars.cluster.top.master
#define CLUSTER_ACTIVE_SLAVES   g_server_global_vars.cluster.top.slaves.actives
#define CLUSTER_INACTIVE_SLAVES g_server_global_vars.cluster.top.slaves.inactives

#define DATA_VERSION            g_server_global_vars.data.version

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

#define CLUSTER_CONFIG_SIGN_BUF g_server_global_vars.cluster.config.md5_digest
#define CLUSTER_CONFIG_SIGN_LEN 16

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRServerGlobalVars g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif

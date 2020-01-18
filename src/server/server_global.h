
#ifndef _FDIR_SERVER_GLOBAL_H
#define _FDIR_SERVER_GLOBAL_H

#include "fastcommon/common_define.h"

typedef struct server_reload_all_configs_policy {
    int min_version_changed;
    int min_interval;
    int max_interval;
} FDIRServerReloadAllConfigsPolicy;

typedef struct server_global_vars {
    struct {
        char *host;
        int port;
        char *user;
        char *password;
        char *database;
        int ping_interval;
    } db_config;

    struct {
        string_t username;
        string_t secret_key;
    } admin;

    int reload_interval_ms;

    int check_alive_interval;

    FDIRServerReloadAllConfigsPolicy reload_all_configs_policy;

} FDIRServerGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRServerGlobalVars g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif

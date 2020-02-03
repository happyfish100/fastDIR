
#ifndef _FDIR_SERVER_GLOBAL_H
#define _FDIR_SERVER_GLOBAL_H

#include "fastcommon/common_define.h"

typedef struct server_global_vars {
    struct {
        string_t username;
        string_t secret_key;
    } admin;

    int reload_interval_ms;

    int check_alive_interval;

} FDIRServerGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRServerGlobalVars g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif

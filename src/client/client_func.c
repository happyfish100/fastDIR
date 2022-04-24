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
#include "sf/sf_cluster_cfg.h"
#include "fastcfs/auth/fcfs_auth_client.h"
#include "fdir_func.h"
#include "client_global.h"
#include "simple_connection_manager.h"
#include "pooled_connection_manager.h"
#include "client_func.h"

int fdir_alloc_group_servers(FDIRServerGroup *server_group,
        const int alloc_size)
{
    int bytes;

    bytes = sizeof(ConnectionInfo) * alloc_size;
    server_group->servers = (ConnectionInfo *)fc_malloc(bytes);
    if (server_group->servers == NULL) {
        return errno != 0 ? errno : ENOMEM;
    }
    memset(server_group->servers, 0, bytes);

    server_group->alloc_size = alloc_size;
    server_group->count = 0;
    return 0;
}

static int fdir_client_do_init_ex(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx)
{
    char *pBasePath;
    char full_cluster_filename[PATH_MAX];
    int result;

    pBasePath = iniGetStrValue(NULL, "base_path", ini_ctx->context);
    if (pBasePath == NULL) {
        strcpy(g_fdir_client_vars.base_path, "/tmp");
    } else {
        snprintf(g_fdir_client_vars.base_path,
                sizeof(g_fdir_client_vars.base_path),
                "%s", pBasePath);
        chopPath(g_fdir_client_vars.base_path);
    }

    client_ctx->common_cfg.connect_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "connect_timeout",
            ini_ctx->context, SF_DEFAULT_CONNECT_TIMEOUT, true);
    if (client_ctx->common_cfg.connect_timeout <= 0) {
        client_ctx->common_cfg.connect_timeout = SF_DEFAULT_CONNECT_TIMEOUT;
    }

    client_ctx->common_cfg.network_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "network_timeout",
            ini_ctx->context, SF_DEFAULT_NETWORK_TIMEOUT, true);
    if (client_ctx->common_cfg.network_timeout <= 0) {
        client_ctx->common_cfg.network_timeout = SF_DEFAULT_NETWORK_TIMEOUT;
    }

    if ((result=sf_load_read_rule_config(&client_ctx->common_cfg.
                    read_rule, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=sf_load_cluster_config_ex(&client_ctx->cluster, ini_ctx,
                    FDIR_SERVER_DEFAULT_CLUSTER_PORT, full_cluster_filename,
                    PATH_MAX)) != 0)
    {
        return result;
    }

    if ((result=sf_load_net_retry_config(&client_ctx->
                    common_cfg.net_retry_cfg, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=fcfs_auth_load_config(&client_ctx->auth,
                    full_cluster_filename)) != 0)
    {
        return result;
    }

    return 0;
}

void fdir_client_log_config_ex(FDIRClientContext *client_ctx,
        const char *extra_config, const bool log_base_path)
{
    char base_path_output[PATH_MAX];
    char net_retry_output[256];

    if (log_base_path) {
        snprintf(base_path_output, sizeof(base_path_output),
                "base_path=%s, ", g_fdir_client_vars.base_path);
    } else {
        *base_path_output = '\0';
    }

    sf_net_retry_config_to_string(&client_ctx->common_cfg.net_retry_cfg,
            net_retry_output, sizeof(net_retry_output));
    logInfo("FastDIR v%d.%d.%d, %s"
            "connect_timeout=%d, "
            "network_timeout=%d, "
            "read_rule: %s, %s, "
            "dir_server_count=%d%s%s",
            g_fdir_global_vars.version.major,
            g_fdir_global_vars.version.minor,
            g_fdir_global_vars.version.patch,
            base_path_output,
            client_ctx->common_cfg.connect_timeout,
            client_ctx->common_cfg.network_timeout,
            sf_get_read_rule_caption(client_ctx->common_cfg.read_rule),
            net_retry_output, FC_SID_SERVER_COUNT(client_ctx->cluster.server_cfg),
            extra_config != NULL ? ", " : "",
            extra_config != NULL ? extra_config : "");
}

int fdir_client_load_from_file_ex1(FDIRClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx)
{
    IniContext iniContext;
    int result;

    client_ctx->auth.ctx = auth_ctx;
    if (ini_ctx->context == NULL) {
        if ((result=iniLoadFromFile(ini_ctx->filename, &iniContext)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "load conf file \"%s\" fail, ret code: %d",
                    __LINE__, ini_ctx->filename, result);
            return result;
        }
        ini_ctx->context = &iniContext;
    }

    result = fdir_client_do_init_ex(client_ctx, ini_ctx);

    if (ini_ctx->context == &iniContext) {
        iniFreeContext(&iniContext);
        ini_ctx->context = NULL;
    }

    return result;
}

static inline void fdir_client_common_init(FDIRClientContext *client_ctx,
        FDIRClientConnManagerType conn_manager_type)
{
    client_ctx->conn_manager_type = conn_manager_type;
    client_ctx->cloned = false;
    srand(time(NULL));
}

int fdir_client_init_ex1(FDIRClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx,
        const SFConnectionManager *cm)
{
    int result;
    FDIRClientConnManagerType conn_manager_type;

    memset(client_ctx, 0, sizeof(FDIRClientContext));
    if ((result=fdir_client_load_from_file_ex1(client_ctx,
                    auth_ctx, ini_ctx)) != 0)
    {
        return result;
    }

    if (cm == NULL) {
        if ((result=fdir_simple_connection_manager_init(
                        client_ctx, &client_ctx->cm)) != 0)
        {
            return result;
        }
        conn_manager_type = conn_manager_type_simple;
    } else if (cm != &client_ctx->cm) {
        client_ctx->cm = *cm;
        conn_manager_type = conn_manager_type_other;
    } else {
        conn_manager_type = conn_manager_type_other;
    }
    fdir_client_common_init(client_ctx, conn_manager_type);
    return 0;
}

int fdir_client_simple_init_ex1(FDIRClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx)
{
    int result;

    memset(client_ctx, 0, sizeof(FDIRClientContext));
    if ((result=fdir_client_load_from_file_ex1(client_ctx,
                    auth_ctx, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=fdir_simple_connection_manager_init(
                    client_ctx, &client_ctx->cm)) != 0)
    {
        return result;
    }

    fdir_client_common_init(client_ctx, conn_manager_type_simple);
    return 0;
}

int fdir_client_pooled_init_ex1(FDIRClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx,
        const int max_count_per_entry, const int max_idle_time,
        const bool bg_thread_enabled)
{
    int result;

    memset(client_ctx, 0, sizeof(FDIRClientContext));
    if ((result=fdir_client_load_from_file_ex1(client_ctx,
                    auth_ctx, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=fdir_pooled_connection_manager_init(client_ctx,
                    &client_ctx->cm, max_count_per_entry,
                    max_idle_time, bg_thread_enabled)) != 0)
    {
        return result;
    }

    fdir_client_common_init(client_ctx, conn_manager_type_pooled);
    return 0;
}

void fdir_client_destroy_ex(FDIRClientContext *client_ctx)
{
    if (client_ctx->cloned) {
        return;
    }

    fc_server_destroy(&client_ctx->cluster.server_cfg);

    if (client_ctx->conn_manager_type == conn_manager_type_simple) {
        fdir_simple_connection_manager_destroy(&client_ctx->cm);
    } else if (client_ctx->conn_manager_type == conn_manager_type_pooled) {
        fdir_pooled_connection_manager_destroy(&client_ctx->cm);
    }
    memset(client_ctx, 0, sizeof(FDIRClientContext));
}


#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "client_global.h"
#include "simple_connection_manager.h"
#include "pooled_connection_manager.h"
#include "client_func.h"

static int copy_dir_servers(FDIRServerGroup *server_group,
        const char *filename, IniItem *dir_servers, const int count)
{
    IniItem *item;
    IniItem *end;
    ConnectionInfo *server;
    int result;

    server = server_group->servers;
    end = dir_servers + count;
    for (item=dir_servers; item<end; item++,server++) {
        if ((result=conn_pool_parse_server_info(item->value,
                        server, FDIR_SERVER_DEFAULT_SERVICE_PORT)) != 0)
        {
            return result;
        }
    }
    server_group->count = count;

    /*
    {
        printf("dir_server count: %d\n", server_group->count);
        for (server=server_group->servers; server<server_group->servers+
                server_group->count; server++)
        {
            printf("dir_server=%s:%d\n", server->ip_addr, server->port);
        }
    }
    */

    return 0;
}

int fdir_alloc_group_servers(FDIRServerGroup *server_group,
        const int alloc_size)
{
    int bytes;

    bytes = sizeof(ConnectionInfo) * alloc_size;
    server_group->servers = (ConnectionInfo *)malloc(bytes);
    if (server_group->servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return errno != 0 ? errno : ENOMEM;
    }
    memset(server_group->servers, 0, bytes);

    server_group->alloc_size = alloc_size;
    server_group->count = 0;
    return 0;
}

int fdir_load_server_group_ex(FDIRServerGroup *server_group,
        const char *conf_filename, IniContext *pIniContext)
{
    int result;
    IniItem *dir_servers;
    int count;

    dir_servers = iniGetValuesEx(NULL, "dir_server",
            pIniContext, &count);
    if (count == 0) {
        logError("file: "__FILE__", line: %d, "
            "conf file \"%s\", item \"dir_server\" not exist",
            __LINE__, conf_filename);
        return ENOENT;
    }

    if ((result=fdir_alloc_group_servers(server_group, count)) != 0) {
        return result;
    }

    if ((result=copy_dir_servers(server_group, conf_filename,
            dir_servers, count)) != 0)
    {
        server_group->count = 0;
        free(server_group->servers);
        server_group->servers = NULL;
        return result;
    }

    return 0;
}

static int fdir_client_do_init_ex(FDIRClientContext *client_ctx,
        const char *conf_filename, IniContext *iniContext)
{
    char *pBasePath;
    int result;

    pBasePath = iniGetStrValue(NULL, "base_path", iniContext);
    if (pBasePath == NULL) {
        strcpy(g_fdir_client_vars.base_path, "/tmp");
    } else {
        snprintf(g_fdir_client_vars.base_path,
                sizeof(g_fdir_client_vars.base_path),
                "%s", pBasePath);
        chopPath(g_fdir_client_vars.base_path);
        if (!fileExists(g_fdir_client_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" can't be accessed, error info: %s",
                __LINE__, g_fdir_client_vars.base_path,
                STRERROR(errno));
            return errno != 0 ? errno : ENOENT;
        }
        if (!isDir(g_fdir_client_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" is not a directory!",
                __LINE__, g_fdir_client_vars.base_path);
            return ENOTDIR;
        }
    }

    g_fdir_client_vars.connect_timeout = iniGetIntValue(NULL,
            "connect_timeout", iniContext, DEFAULT_CONNECT_TIMEOUT);
    if (g_fdir_client_vars.connect_timeout <= 0) {
        g_fdir_client_vars.connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    }

    g_fdir_client_vars.network_timeout = iniGetIntValue(NULL,
            "network_timeout", iniContext, DEFAULT_NETWORK_TIMEOUT);
    if (g_fdir_client_vars.network_timeout <= 0) {
        g_fdir_client_vars.network_timeout = DEFAULT_NETWORK_TIMEOUT;
    }

    if ((result=fdir_load_server_group_ex(&client_ctx->server_group,
                    conf_filename, iniContext)) != 0)
    {
        return result;
    }

#ifdef DEBUG_FLAG
    logDebug("FastDIR v%d.%02d, "
            "base_path=%s, "
            "connect_timeout=%d, "
            "network_timeout=%d, "
            "dir_server_count=%d",
            g_fdir_global_vars.version.major,
            g_fdir_global_vars.version.minor,
            g_fdir_client_vars.base_path,
            g_fdir_client_vars.connect_timeout,
            g_fdir_client_vars.network_timeout,
            client_ctx->server_group.count);
#endif

    return 0;
}

int fdir_client_load_from_file_ex(FDIRClientContext *client_ctx,
        const char *conf_filename)
{
    IniContext iniContext;
    int result;

    if ((result=iniLoadFromFile(conf_filename, &iniContext)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "load conf file \"%s\" fail, ret code: %d",
            __LINE__, conf_filename, result);
        return result;
    }

    result = fdir_client_do_init_ex(client_ctx, conf_filename,
                &iniContext);
    iniFreeContext(&iniContext);

    return result;
}

static inline void fdir_client_common_init(FDIRClientContext *client_ctx,
        FDIRClientConnManagerType conn_manager_type)
{
    client_ctx->conn_manager_type = conn_manager_type;
    client_ctx->cloned = false;
    srand(time(NULL));
}

int fdir_client_init_ex(FDIRClientContext *client_ctx,
        const char *conf_filename, const FDIRConnectionManager *conn_manager)
{
    int result;
    FDIRClientConnManagerType conn_manager_type;

    if ((result=fdir_client_load_from_file_ex(
                    client_ctx, conf_filename)) != 0)
    {
        return result;
    }

    if (conn_manager == NULL) {
        if ((result=fdir_simple_connection_manager_init(
                        &client_ctx->conn_manager)) != 0)
        {
            return result;
        }
        conn_manager_type = conn_manager_type_simple;
    } else if (conn_manager != &client_ctx->conn_manager) {
        client_ctx->conn_manager = *conn_manager;
        conn_manager_type = conn_manager_type_other;
    } else {
        conn_manager_type = conn_manager_type_other;
    }
    fdir_client_common_init(client_ctx, conn_manager_type);
    return 0;
}

int fdir_client_simple_init_ex(FDIRClientContext *client_ctx,
        const char *conf_filename)
{
    int result;

    if ((result=fdir_client_load_from_file_ex(
                    client_ctx, conf_filename)) != 0)
    {
        return result;
    }

    if ((result=fdir_simple_connection_manager_init(
                    &client_ctx->conn_manager)) != 0)
    {
        return result;
    }

    fdir_client_common_init(client_ctx, conn_manager_type_simple);
    return 0;
}

int fdir_client_pooled_init_ex(FDIRClientContext *client_ctx,
        const char *conf_filename, const int max_count_per_entry,
        const int max_idle_time)
{
    int result;

    if ((result=fdir_client_load_from_file_ex(
                    client_ctx, conf_filename)) != 0)
    {
        return result;
    }

    if ((result=fdir_pooled_connection_manager_init(
                    &client_ctx->conn_manager, max_count_per_entry,
                    max_idle_time)) != 0)
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
    if (client_ctx->server_group.servers == NULL) {
        return;
    }

    free(client_ctx->server_group.servers);
    if (client_ctx->conn_manager_type == conn_manager_type_simple) {
        fdir_simple_connection_manager_destroy(&client_ctx->conn_manager);
    } else if (client_ctx->conn_manager_type == conn_manager_type_pooled) {
        fdir_pooled_connection_manager_destroy(&client_ctx->conn_manager);
    }
    memset(client_ctx, 0, sizeof(FDIRClientContext));
}

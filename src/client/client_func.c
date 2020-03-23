
#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "client_global.h"
#include "simple_connection_manager.h"
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

    {
        printf("dir_server count: %d\n", server_group->count);
        for (server=server_group->servers; server<server_group->servers+
                server_group->count; server++)
        {
            printf("dir_server=%s:%d\n", server->ip_addr, server->port);
        }
    }

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
        strcpy(g_client_global_vars.base_path, "/tmp");
    } else {
        snprintf(g_client_global_vars.base_path,
                sizeof(g_client_global_vars.base_path),
                "%s", pBasePath);
        chopPath(g_client_global_vars.base_path);
        if (!fileExists(g_client_global_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" can't be accessed, error info: %s",
                __LINE__, g_client_global_vars.base_path,
                STRERROR(errno));
            return errno != 0 ? errno : ENOENT;
        }
        if (!isDir(g_client_global_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" is not a directory!",
                __LINE__, g_client_global_vars.base_path);
            return ENOTDIR;
        }
    }

    g_client_global_vars.connect_timeout = iniGetIntValue(NULL,
            "connect_timeout", iniContext, DEFAULT_CONNECT_TIMEOUT);
    if (g_client_global_vars.connect_timeout <= 0) {
        g_client_global_vars.connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    }

    g_client_global_vars.network_timeout = iniGetIntValue(NULL,
            "network_timeout", iniContext, DEFAULT_NETWORK_TIMEOUT);
    if (g_client_global_vars.network_timeout <= 0) {
        g_client_global_vars.network_timeout = DEFAULT_NETWORK_TIMEOUT;
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
            g_client_global_vars.base_path,
            g_client_global_vars.connect_timeout,
            g_client_global_vars.network_timeout,
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

int fdir_client_init_ex(FDIRClientContext *client_ctx,
        const char *conf_filename, const FDIRConnectionManager *conn_manager)
{
    int result;
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
        client_ctx->is_simple_conn_mananger = true;
    } else if (conn_manager != &client_ctx->conn_manager) {
        client_ctx->conn_manager = *conn_manager;
        client_ctx->is_simple_conn_mananger = false;
    } else {
        client_ctx->is_simple_conn_mananger = false;
    }

    srand(time(NULL));
    return 0;
}

void fdir_client_destroy_ex(FDIRClientContext *client_ctx)
{
    if (client_ctx->server_group.servers == NULL) {
        return;
    }

    free(client_ctx->server_group.servers);
    if (client_ctx->is_simple_conn_mananger) {
        fdir_simple_connection_manager_destroy(&client_ctx->conn_manager);
    }
    memset(client_ctx, 0, sizeof(FDIRClientContext));
}

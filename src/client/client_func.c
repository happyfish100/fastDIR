
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
            printf("dir_server=%s:%u\n", server->ip_addr, server->port);
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
    server_group->servers = (ConnectionInfo *)fc_malloc(bytes);
    if (server_group->servers == NULL) {
        return errno != 0 ? errno : ENOMEM;
    }
    memset(server_group->servers, 0, bytes);

    server_group->alloc_size = alloc_size;
    server_group->count = 0;
    return 0;
}

int fdir_load_server_group_ex(FDIRServerGroup *server_group,
        IniFullContext *ini_ctx)
{
    int result;
    IniItem *dir_servers;
    int count;

    dir_servers = iniGetValuesEx(ini_ctx->section_name,
            "dir_server", ini_ctx->context, &count);
    if (count == 0) {
        logError("file: "__FILE__", line: %d, "
            "conf file \"%s\", item \"dir_server\" not exist",
            __LINE__, ini_ctx->filename);
        return ENOENT;
    }

    if ((result=fdir_alloc_group_servers(server_group, count)) != 0) {
        return result;
    }

    if ((result=copy_dir_servers(server_group, ini_ctx->filename,
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
        IniFullContext *ini_ctx)
{
    char *pBasePath;
    char net_retry_output[256];
    int result;

    pBasePath = iniGetStrValue(NULL, "base_path", ini_ctx->context);
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

    client_ctx->connect_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "connect_timeout",
            ini_ctx->context, DEFAULT_CONNECT_TIMEOUT, true);
    if (client_ctx->connect_timeout <= 0) {
        client_ctx->connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    }

    client_ctx->network_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "network_timeout",
            ini_ctx->context, DEFAULT_NETWORK_TIMEOUT, true);
    if (client_ctx->network_timeout <= 0) {
        client_ctx->network_timeout = DEFAULT_NETWORK_TIMEOUT;
    }

    sf_load_read_rule_config(&client_ctx->read_rule, ini_ctx);

    if ((result=fdir_load_server_group_ex(&client_ctx->
                    server_group, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=sf_load_net_retry_config(&client_ctx->
                    net_retry_cfg, ini_ctx)) != 0)
    {
        return result;
    }

    sf_net_retry_config_to_string(&client_ctx->net_retry_cfg,
            net_retry_output, sizeof(net_retry_output));

    logDebug("FastDIR v%d.%02d, "
            "base_path=%s, "
            "connect_timeout=%d, "
            "network_timeout=%d, "
            "read_rule: %s, %s, "
            "dir_server_count=%d",
            g_fdir_global_vars.version.major,
            g_fdir_global_vars.version.minor,
            g_fdir_client_vars.base_path,
            client_ctx->connect_timeout,
            client_ctx->network_timeout,
            sf_get_read_rule_caption(client_ctx->read_rule),
            net_retry_output,
            client_ctx->server_group.count);

    return 0;
}

int fdir_client_load_from_file_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx)
{
    IniContext iniContext;
    int result;

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
        IniFullContext *ini_ctx, const FDIRConnectionManager *conn_manager)
{
    int result;
    FDIRClientConnManagerType conn_manager_type;

    memset(client_ctx, 0, sizeof(FDIRClientContext));
    if ((result=fdir_client_load_from_file_ex1(client_ctx, ini_ctx)) != 0) {
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

int fdir_client_simple_init_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx)
{
    int result;

    memset(client_ctx, 0, sizeof(FDIRClientContext));
    if ((result=fdir_client_load_from_file_ex1(client_ctx, ini_ctx)) != 0) {
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

int fdir_client_pooled_init_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx, const int max_count_per_entry,
        const int max_idle_time)
{
    int result;

    memset(client_ctx, 0, sizeof(FDIRClientContext));
    if ((result=fdir_client_load_from_file_ex1(client_ctx, ini_ctx)) != 0) {
        return result;
    }

    if ((result=fdir_pooled_connection_manager_init(client_ctx,
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

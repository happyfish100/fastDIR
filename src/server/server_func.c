
#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "server_types.h"
#include "server_global.h"
#include "server_func.h"

static int server_load_admin_config(IniContext *ini_context)
{
#define ADMIN_SECTION_NAME "admin"

    char *username;
    char *secret_key;
    char *buff;
    char *p;
    struct {
        int username;
        int secret_key;
    } lengths;
    int bytes;

    if ((username=iniGetRequiredStrValue(ADMIN_SECTION_NAME, "username",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }

    if ((secret_key=iniGetRequiredStrValue(ADMIN_SECTION_NAME, "secret_key",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }

    g_server_global_vars.admin.username.len = strlen(username);
    g_server_global_vars.admin.secret_key.len = strlen(secret_key);

    lengths.username = g_server_global_vars.admin.username.len + 1;
    lengths.secret_key = g_server_global_vars.admin.secret_key.len + 1;

    bytes = lengths.username + lengths.secret_key;
    buff = (char *)malloc(bytes);
    if (buff == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    p = buff;
    g_server_global_vars.admin.username.str = p;
    p += lengths.username;

    g_server_global_vars.admin.secret_key.str = p;
    p += lengths.secret_key;

    memcpy(g_server_global_vars.admin.username.str, username, lengths.username);
    memcpy(g_server_global_vars.admin.secret_key.str, secret_key, lengths.secret_key);
    return 0;
}

int server_load_config(const char *filename)
{
    IniContext ini_context;
    char server_config_str[1024];
    int result;

    memset(&ini_context, 0, sizeof(IniContext));
    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    if ((result=sf_load_config("fdir_serverd", filename, &ini_context,
                    FDIR_SERVER_DEFAULT_INNER_PORT,
                    FDIR_SERVER_DEFAULT_OUTER_PORT)) != 0)
    {
        return result;
    }

    if ((result=server_load_admin_config(&ini_context)) != 0) {
        return result;
    }

    g_server_global_vars.reload_interval_ms = iniGetIntValue(NULL,
            "reload_interval_ms", &ini_context,
            FDIR_SERVER_DEFAULT_RELOAD_INTERVAL);
    if (g_server_global_vars.reload_interval_ms <= 0) {
        g_server_global_vars.reload_interval_ms = FDIR_SERVER_DEFAULT_RELOAD_INTERVAL;
    }

    g_server_global_vars.check_alive_interval = iniGetIntValue(NULL,
            "check_alive_interval", &ini_context,
            FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL);
    if (g_server_global_vars.check_alive_interval <= 0) {
        g_server_global_vars.check_alive_interval = FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL;
    }

    g_server_global_vars.namespace_hashtable_capacity = iniGetIntValue(NULL,
            "namespace_hashtable_capacity", &ini_context,
            FDIR_NAMESPACE_HASHTABLE_CAPACITY);
    if (g_server_global_vars.namespace_hashtable_capacity <= 0) {
        g_server_global_vars.namespace_hashtable_capacity =
            FDIR_NAMESPACE_HASHTABLE_CAPACITY;
    }

    iniFreeContext(&ini_context);

    snprintf(server_config_str, sizeof(server_config_str),
            "admin config {username: %s, secret_key: %s}, "
            "reload_interval_ms = %d ms, "
            "check_alive_interval = %d s"
            "namespace_hashtable_capacity = %d",
            g_server_global_vars.admin.username.str,
            g_server_global_vars.admin.secret_key.str,
            g_server_global_vars.reload_interval_ms,
            g_server_global_vars.check_alive_interval,
            g_server_global_vars.namespace_hashtable_capacity);
    sf_log_config_ex(server_config_str);
    return 0;
}

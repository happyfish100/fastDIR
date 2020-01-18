
#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "server_types.h"
#include "server_global.h"
#include "server_func.h"

static int server_load_db_config(IniContext *ini_context)
{
#define DB_SECTION_NAME "mysql"

    char *host;
    char *user;
    char *password;
    char *database;
    char *buff;
    char *p;
    struct {
        int host;
        int user;
        int password;
        int database;
    } lengths;
    int bytes;

    if ((host=iniGetRequiredStrValue(DB_SECTION_NAME, "host",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }
    g_server_global_vars.db_config.port = iniGetIntValue(DB_SECTION_NAME,
            "port", ini_context, 3306);
    if ((user=iniGetRequiredStrValue(DB_SECTION_NAME, "user",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }
    if ((password=iniGetRequiredStrValueEx(DB_SECTION_NAME, "password",
                    ini_context, 0)) == NULL)
    {
        return ENOENT;
    }
    if ((database=iniGetRequiredStrValue(DB_SECTION_NAME, "database",
                    ini_context)) == NULL)
    {
        return ENOENT;
    }
    g_server_global_vars.db_config.ping_interval = iniGetIntValue(
            DB_SECTION_NAME, "ping_interval", ini_context, 14400);

    lengths.host = strlen(host) + 1;
    lengths.user = strlen(user) + 1;
    lengths.password = strlen(password) + 1;
    lengths.database = strlen(database) + 1;

    bytes = lengths.host + lengths.user + lengths.password + lengths.database;
    buff = (char *)malloc(bytes);
    if (buff == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    p = buff;
    g_server_global_vars.db_config.host = p;
    p += lengths.host;

    g_server_global_vars.db_config.user = p;
    p += lengths.user;

    g_server_global_vars.db_config.password = p;
    p += lengths.password;

    g_server_global_vars.db_config.database = p;
    p += lengths.database;

    memcpy(g_server_global_vars.db_config.host, host, lengths.host);
    memcpy(g_server_global_vars.db_config.user, user, lengths.user);
    memcpy(g_server_global_vars.db_config.password, password, lengths.password);
    memcpy(g_server_global_vars.db_config.database, database, lengths.database);

    return 0;
}

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

    if ((result=server_load_db_config(&ini_context)) != 0) {
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

    g_server_global_vars.reload_all_configs_policy.min_version_changed =
        iniGetIntValue("reload_all_configs_policy",
                "min_version_changed", &ini_context, 100);
    if (g_server_global_vars.reload_all_configs_policy.min_version_changed <= 0) {
        g_server_global_vars.reload_all_configs_policy.min_version_changed = 100;
    }

    g_server_global_vars.reload_all_configs_policy.min_interval =
        iniGetIntValue("reload_all_configs_policy",
                "min_interval", &ini_context, 3600);
    if (g_server_global_vars.reload_all_configs_policy.min_interval <= 0) {
        g_server_global_vars.reload_all_configs_policy.min_interval = 3600;
    }

    g_server_global_vars.reload_all_configs_policy.max_interval =
        iniGetIntValue("reload_all_configs_policy",
                "max_interval", &ini_context, 86400);
    if (g_server_global_vars.reload_all_configs_policy.max_interval <=
            g_server_global_vars.reload_all_configs_policy.min_interval)
    {
        g_server_global_vars.reload_all_configs_policy.max_interval = 86400;
    }

    iniFreeContext(&ini_context);

    snprintf(server_config_str, sizeof(server_config_str),
            "db config {host: %s, port: %d, user: %s, "
            "password: %s, database: %s, ping_interval: %d s}, "
            "admin config {username: %s, secret_key: %s} "
            "reload_interval_ms: %d ms, "
            "check_alive_interval: %d s, "
            "reload_all_configs_policy {min_version_changed: %d, "
            "min_interval: %d s, max_interval: %d s}",
            g_server_global_vars.db_config.host,
            g_server_global_vars.db_config.port,
            g_server_global_vars.db_config.user,
            g_server_global_vars.db_config.password,
            g_server_global_vars.db_config.database,
            g_server_global_vars.db_config.ping_interval,
            g_server_global_vars.admin.username.str,
            g_server_global_vars.admin.secret_key.str,
            g_server_global_vars.reload_interval_ms,
            g_server_global_vars.check_alive_interval,
            g_server_global_vars.reload_all_configs_policy.min_version_changed,
            g_server_global_vars.reload_all_configs_policy.min_interval,
            g_server_global_vars.reload_all_configs_policy.max_interval);
    sf_log_config_ex(server_config_str);
    return 0;
}

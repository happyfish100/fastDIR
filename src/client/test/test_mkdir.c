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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "fastdir/client/fdir_client.h"

#define SUBDIR_COUNT  500

static char *ns = "test";
static char *base_path = "/test";
static char true_base_path[PATH_MAX];
static bool ignore_exist_error = false;
static int total_count = 0;
static int ignore_count = 0;
static FDIRClientOwnerModePair omp;

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] "
            "[-n namespace=test] [-b base_path=/test] "
            "[-i for ignoring exist error]\n", argv[0],
            FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static int create_dentry(FDIRDEntryFullName *fullname)
{
	int result;
    FDIRDEntryInfo dentry;

    ++total_count;
    if ((result=fdir_client_create_dentry(&g_fdir_client_vars.client_ctx,
                    fullname, &omp, &dentry)) != 0)
    {
        if (ignore_exist_error && result == EEXIST) {
            ++ignore_count;
            result = 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "create_dentry %.*s fail, namespace: %s, "
                    "errno: %d, error info: %s", __LINE__,
                    fullname->path.len, fullname->path.str,
                    fullname->ns.str, result, STRERROR(result));
        }
    }
    return result;
}

static int create_base_path()
{
#define MAX_SUBDIR_COUNT 8

    string_t path;
    string_t parts[MAX_SUBDIR_COUNT];
    FDIRDEntryFullName fullname;
    int result;
    int count;
    int len;
    int i;

    FC_SET_STRING(path, base_path);
    count = split_string_ex(&path, '/', parts, MAX_SUBDIR_COUNT, true);

    FC_SET_STRING(fullname.ns, ns);
    fullname.path.str = true_base_path;
    strcpy(true_base_path, "/");
    len = 1;
    fullname.path.len = len;
    if ((result=create_dentry(&fullname)) != 0) {
        if (result != EEXIST) {
            return result;
        }
    }

    for (i=0; i<count; i++) {
        if (i > 0) {
            *(true_base_path + len++) = '/';
        }
        memcpy(true_base_path + len, parts[i].str, parts[i].len);
        len += parts[i].len;

        fullname.path.len = len;
        if ((result=create_dentry(&fullname)) != 0) {
            if (result != EEXIST) {
                return result;
            }
        }
    }

    *(true_base_path + len) = '\0';
    return 0;
}

static int test_mkdir()
{
    FDIRDEntryFullName fullname;
    char path[256];
	int result;
    int i;
    int k;

    if ((result=create_base_path()) != 0) {
        return result;
    }

    FC_SET_STRING(fullname.ns, ns);
    fullname.path.str = path;
    for (i=0; i<SUBDIR_COUNT; i++) {
        fullname.path.len = sprintf(path, "%s/%03d",
                base_path, i + 1);
        if ((result=create_dentry(&fullname)) != 0) {
            return result;
        }
        for (k=0; k<SUBDIR_COUNT; k++) {
            fullname.path.len = sprintf(path, "%s/%03d/%03d",
                    base_path, i + 1, k + 1);
            if ((result=create_dentry(&fullname)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    string_t poolname;
    char time_buff[32];
    int64_t start_time; 
    int64_t time_used;
	int result;

    while ((ch=getopt(argc, argv, "hic:n:b:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'n':
                ns = optarg;
                break;
            case 'b':
                base_path = optarg;
                break;
            case 'c':
                config_filename = optarg;
                break;
            case 'i':
                ignore_exist_error = true;
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    FC_SET_STRING(poolname, ns);
    if ((result=fdir_client_simple_init_with_auth_ex(
                    config_filename, &poolname, publish)) != 0)
    {
        return result;
    }

    fdir_client_log_config(&g_fdir_client_vars.client_ctx);
    logDebug("idempotency_enabled: %d", g_fdir_client_vars.
            client_ctx.idempotency_enabled);

    omp.mode = 0755 | S_IFDIR;
    omp.uid = geteuid();
    omp.gid = getegid();
    start_time = get_current_time_ms();
    result = test_mkdir();
    time_used = get_current_time_ms() - start_time;
    printf("create %d dentry, ignore count: %d, time used: %s ms\n", 
            total_count, ignore_count,
            long_to_comma_str(time_used, time_buff));

    return result;
}

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

#define SUBDIR_COUNT  300

static char *ns = "test";
static char *base_path = "/test";
static bool ignore_noent_error = false;
static int total_count = 0;
static int ignore_count = 0;

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] "
            "[-n namespace=test] [-b base_path=/test] "
            "[-i for ignoring not exist error]\n", argv[0],
            FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static int remove_dentry(FDIRDEntryFullName *fullname)
{
    const int flags = 0;
	int result;

    ++total_count;
    if ((result=fdir_client_remove_dentry(&g_fdir_client_vars.
                    client_ctx, fullname, flags)) != 0)
    {
        if (ignore_noent_error && result == ENOENT) {
            ++ignore_count;
            result = 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "remove_dentry %.*s fail, namespace: %s, "
                    "errno: %d, error info: %s", __LINE__,
                    fullname->path.len, fullname->path.str,
                    fullname->ns.str, result, STRERROR(result));
        }
    }
    return result;
}

static int test_rmdir()
{
    FDIRDEntryFullName fullname;
    char path[256];
    int64_t inode;
	int result;
    int i;
    int k;

    FC_SET_STRING(fullname.ns, ns);
    fullname.path.str = path;
    fullname.path.len = sprintf(path, "%s", base_path);

    if ((result=fdir_client_lookup_inode_by_path(&g_fdir_client_vars.
                    client_ctx, &fullname, &inode)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "lookup path %.*s fail, namespace: %s, "
                "errno: %d, error info: %s", __LINE__,
                fullname.path.len, fullname.path.str,
                fullname.ns.str, result, STRERROR(result));
        return result;
    }

    for (i=0; i<SUBDIR_COUNT; i++) {
        for (k=0; k<SUBDIR_COUNT; k++) {
            fullname.path.len = sprintf(path, "%s/%03d/%03d",
                    base_path, i + 1, k + 1);
            if ((result=remove_dentry(&fullname)) != 0) {
                return result;
            }
        }

        fullname.path.len = sprintf(path, "%s/%03d",
                base_path, i + 1);
        if ((result=remove_dentry(&fullname)) != 0) {
            return result;
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
                ignore_noent_error = true;
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

    start_time = get_current_time_ms();
    result = test_rmdir();
    time_used = get_current_time_ms() - start_time;
    printf("remove %d dentry, ignore count: %d, time used: %s ms\n", 
            total_count, ignore_count,
            long_to_comma_str(time_used, time_buff));

    return result;
}

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
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "fastdir/client/fdir_client.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] [-m mode] "
            "[-u uid] [-g gid] <-n namespace> <path>\n",
            argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    mode_t mode;
    char *ns;
    char *pt;
    FDIRClientOperFnamePair path;
	int result;
    int base;
    char *endptr;
    FDIRDEntryInfo dentry;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    mode = 0755;
    FDIR_SET_OPERATOR(path.oper, geteuid(), getegid(), 0, NULL);
    ns = NULL;
    while ((ch=getopt(argc, argv, "hc:g:m:n:u:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'n':
                ns = optarg;
                break;
            case 'c':
                config_filename = optarg;
                break;
            case 'm':
                if (optarg[0] == '0') {
                    base = 8;
                } else {
                    base = 10;
                }
                mode = strtol(optarg, &endptr, base);
                break;
            case 'u':
                path.oper.uid = strtol(optarg, &endptr, 10);
                break;
            case 'g':
                path.oper.gid = strtol(optarg, &endptr, 10);
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if (ns == NULL || optind >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    pt = argv[optind];
    FC_SET_STRING(path.fullname.ns, ns);
    FC_SET_STRING(path.fullname.path, pt);
    if ((result=fdir_client_simple_init_with_auth_ex(config_filename,
                    &path.fullname.ns, publish)) != 0)
    {
        return result;
    }

    mode |= S_IFDIR;
    return fdir_client_create_dentry(&g_fdir_client_vars.
            client_ctx, &path, mode, &dentry);
}

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
    fprintf(stderr, "Usage: %s [-c config_filename=%s] [-k attr name to set] \n"
            "\t[-x attr name to remove] [-v attr value to set] \n"
            "\t<-n namespace> <path>\n\n", argv[0],
            FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    int operation;
    char *ns;
    char *path;
    FDIRDEntryFullName fullname;
    key_value_pair_t xattr;
    int flags;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    memset(&xattr, 0, sizeof(xattr));
    ns = NULL;
    xattr.value.len = -1;
    operation = 0;
    flags = 0;
    while ((ch=getopt(argc, argv, "hc:n:k:x:v:")) != -1) {
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
            case 'k':
                operation = 's';
                FC_SET_STRING(xattr.key, optarg);
                break;
            case 'x':
                operation = 'r';
                FC_SET_STRING(xattr.key, optarg);
                break;
            case 'v':
                FC_SET_STRING(xattr.value, optarg);
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if (operation == 0) {
        fprintf(stderr, "please input operation type by -k or -x\n\n");
        usage(argv);
        return 1;
    }

    if (ns == NULL || optind >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    path = argv[optind];
    FC_SET_STRING(fullname.ns, ns);
    FC_SET_STRING(fullname.path, path);
    if ((result=fdir_client_simple_init_with_auth_ex(config_filename,
                    &fullname.ns, publish)) != 0)
    {
        return result;
    }

    if (operation == 's') {
        if (xattr.value.len < 0) {
            fprintf(stderr, "please input attribute value by -v\n\n");
            usage(argv);
            return 1;
        }
        return fdir_client_set_xattr_by_path(&g_fdir_client_vars.
                client_ctx, &fullname, &xattr, flags);
    } else {
        return fdir_client_remove_xattr_by_path(&g_fdir_client_vars.
                client_ctx, &fullname, &xattr.key, flags);
    }
}

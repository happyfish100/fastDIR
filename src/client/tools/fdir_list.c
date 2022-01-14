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
    fprintf(stderr, "Usage: %s [-c config_filename=%s] "
            "<-n namespace> <path>\n", argv[0],
            FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static void output_dentry_array(FDIRClientDentryArray *array)
{
    FDIRClientDentry *dentry;
    FDIRClientDentry *end;

    printf("count: %d\n", array->count);
    end = array->entries + array->count;
    for (dentry=array->entries; dentry<end; dentry++) {
        printf("%.*s\n", dentry->name.len, dentry->name.str);
    }
}

int main(int argc, char *argv[])
{
    const bool publish = true;  //TODO
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    char *ns;
    char *path;
    FDIRDEntryFullName entry_info;
    FDIRClientDentryArray array;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    ns = NULL;
    while ((ch=getopt(argc, argv, "hc:n:")) != -1) {
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

    path = argv[optind];
    FC_SET_STRING(entry_info.ns, ns);
    FC_SET_STRING(entry_info.path, path);
    if ((result=fdir_client_simple_init_with_auth_ex(config_filename,
                    &entry_info.ns, publish)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_dentry_array_init(&array)) != 0) {
        return result;
    }

    if ((result=fdir_client_list_dentry_by_path(&g_fdir_client_vars.client_ctx,
                    &entry_info, &array)) != 0)
    {
        return result;
    }
    output_dentry_array(&array);
    fdir_client_dentry_array_free(&array);
    return 0;
}

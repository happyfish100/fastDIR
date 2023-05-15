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
    fprintf(stderr, "Usage: %s [-c config_filename=%s]\n",
            argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static void output_namespace_array(int server_id,
        FDIRClientNamespaceArray *array)
{
    FDIRClientNamespaceEntry *entry;
    FDIRClientNamespaceEntry *end;
    char dir_count[32];
    char file_count[32];
    char used_bytes[32];

    printf("\nserver id: %d, namespace count: %d\n",
            server_id, array->count);
    printf("%-32s %20s %20s %24s\n", "namespace",
            "dir_count", "file_count", "used_bytes");
    end = array->entries + array->count;
    for (entry=array->entries; entry<end; entry++) {
        long_to_comma_str(entry->dir_count, dir_count);
        long_to_comma_str(entry->file_count, file_count);
        long_to_comma_str(entry->used_bytes, used_bytes);
        printf("%-32.*s %20s %20s %24s\n",
                entry->ns_name.len, entry->ns_name.str,
                dir_count, file_count, used_bytes);
    }
    printf("\n");
}

int main(int argc, char *argv[])
{
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
    FDIRClientNamespaceArray array;
	int ch;
    int server_id;
	int result;

    while ((ch=getopt(argc, argv, "hc:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if (optind < argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    if ((result=fdir_client_simple_init(config_filename)) != 0) {
        return result;
    }

    if ((result=fdir_client_namespace_array_init(&array)) != 0) {
        return result;
    }

    if ((result=fdir_client_namespace_list(&g_fdir_client_vars.
                    client_ctx, &server_id, &array)) != 0)
    {
        return result;
    }
    output_namespace_array(server_id, &array);
    fdir_client_namespace_array_free(&array);
    return 0;
}

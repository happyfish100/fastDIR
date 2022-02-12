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

static FDIRDEntryFullName fullname;
static char v[FDIR_XATTR_MAX_VALUE_SIZE];
static string_t value;
static bool hexdump = false;

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] [-k attribute_name]\n"
            "\t[-d dump all attributes] <-H hexdump for value> <-n namespace>"
            " <path>\n\n", argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static int get_xattr(const string_t *name)
{
    const int flags = 0;
    char fixed_buff[1024];
    char *hex_buff;
    int result;
    if ((result=fdir_client_get_xattr_by_path(&g_fdir_client_vars.client_ctx,
                    &fullname, name, &value, FDIR_XATTR_MAX_VALUE_SIZE,
                    flags)) != 0)
    {
        return result;
    }

    printf("%.*s=", name->len, name->str);
    if (hexdump) {
        if (value.len < sizeof(fixed_buff) / 2) {
            hex_buff = fixed_buff;
        } else {
            hex_buff = (char *)fc_malloc(2 * value.len + 1);
            if (hex_buff == NULL) {
                return ENOMEM;
            }
        }

        bin2hex(value.str, value.len, hex_buff);
        printf("%s\n", hex_buff);
        if (hex_buff != fixed_buff) {
            free(hex_buff);
        }
    } else {
        printf("%.*s\n", value.len, value.str);
    }

    return 0;
}

static int name_compare(const string_t *s1, const string_t *s2)
{
    return strcmp(s1->str, s2->str);
}

static int dump_xattrs()
{
#define MAX_NM_LIST_SIZE  (8 * 1024)

    const int flags = 0;
    char buff[MAX_NM_LIST_SIZE];
    string_t list;
    string_t names[FDIR_XATTR_KVARRAY_MAX_ELEMENTS];
    string_t *nm;
    string_t *end;
    int count;
    int result;

    list.str = buff;
    if ((result=fdir_client_list_xattr_by_path(&g_fdir_client_vars.client_ctx,
                    &fullname, &list, MAX_NM_LIST_SIZE, flags)) != 0)
    {
        return result;
    }

    count = split_string_ex(&list, '\0', names,
            FDIR_XATTR_KVARRAY_MAX_ELEMENTS, true);
    qsort(names, count, sizeof(string_t), (int (*)(const void *,
                    const void *))name_compare);
    end = names + count;
    for (nm=names; nm<end; nm++) {
        if ((result=get_xattr(nm)) != 0) {
            break;
        }
    }

    return result;
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    bool dump_all;
    char *ns;
    char *path;
    string_t name;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    memset(&name, 0, sizeof(name));
    ns = NULL;
    dump_all = false;
    while ((ch=getopt(argc, argv, "hc:n:k:dH")) != -1) {
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
                FC_SET_STRING(name, optarg);
                break;
            case 'd':
                dump_all = true;
                break;
            case 'H':
                hexdump = true;
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
    FC_SET_STRING(fullname.ns, ns);
    FC_SET_STRING(fullname.path, path);
    if ((result=fdir_client_simple_init_with_auth_ex(config_filename,
                    &fullname.ns, publish)) != 0)
    {
        return result;
    }

    value.str = v;
    if (name.len > 0) {
        return get_xattr(&name);
    } else if (dump_all) {
        return dump_xattrs();
    } else {
        fprintf(stderr, "please input option by -k or -d\n\n");
        usage(argv);
        return 1;
    }
}

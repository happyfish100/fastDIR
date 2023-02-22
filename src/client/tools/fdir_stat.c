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

#define FDIR_QUERY_TYPE_FULLNAME  'f'
#define FDIR_QUERY_TYPE_INODE     'i'
#define FDIR_QUERY_TYPE_PNAME     'p'

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s]\n"
            "\t[-t for query type, %c for full path, %c for inode, "
            "%c for parent inode and name]\n"
            "\t[-l for follow_symlink]\n"
            "\t[-F output full filepath]\n"
            "\t<-n namespace> <path | inode | parent_inode name>\n\n",
            argv[0], FDIR_CLIENT_DEFAULT_CONFIG_FILENAME,
            FDIR_QUERY_TYPE_FULLNAME, FDIR_QUERY_TYPE_INODE,
            FDIR_QUERY_TYPE_PNAME);
}

static void output_dentry_stat(FDIRDEntryInfo *dentry)
{
    char ctime[32];
    char atime[32];
    char mtime[32];
    char *type;
    int perm;

    perm = dentry->stat.mode & (~S_IFMT);
    if (S_ISFIFO(dentry->stat.mode)) {
        type = "FIFO";
    } else if (S_ISCHR(dentry->stat.mode)) {
        type = "character device";
    } else if (S_ISDIR(dentry->stat.mode)) {
        type = "directory";
    } else if (S_ISBLK(dentry->stat.mode)) {
        type = "block device";
    } else if (S_ISREG(dentry->stat.mode)) {
        type = "regular file";
    } else if (S_ISLNK(dentry->stat.mode)) {
        type = "symbolic link";
    } else if (S_ISSOCK(dentry->stat.mode)) {
        type = "socket";
    } else {
        type = "UNKOWN";
    }

    formatDatetime(dentry->stat.atime, "%Y-%m-%d %H:%M:%S",
            atime, sizeof(atime));
    formatDatetime(dentry->stat.ctime, "%Y-%m-%d %H:%M:%S",
            ctime, sizeof(ctime));
    formatDatetime(dentry->stat.mtime, "%Y-%m-%d %H:%M:%S",
            mtime, sizeof(mtime));
    printf("type: %s, inode: %"PRId64", size: %"PRId64"\n"
            "stat change time: %s\nmodify time: %s\naccess time: %s\n"
            "uid: %d, gid: %d, perm: 0%03o\n", type, dentry->inode,
            dentry->stat.size, ctime, mtime, atime, dentry->stat.uid,
            dentry->stat.gid, perm);
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FDIR_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    int flags;
    char type;
    bool output_fullname;
    char *ns_str;
    char *path;
    char *endptr;
    string_t ns;
    FDIRClientOperFnamePair fname;
    FDIRClientOperPnamePair opname;
    FDIRClientOperInodePair oino;
    FDIRDEntryInfo dentry;
    string_t fullname;
    char buff[PATH_MAX];
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    flags = 0;
    ns_str = NULL;
    type = 0;
    output_fullname = false;
    while ((ch=getopt(argc, argv, "hc:n:lt:F")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'n':
                ns_str = optarg;
                break;
            case 'c':
                config_filename = optarg;
                break;
            case 'l':
                flags |= FDIR_FLAGS_FOLLOW_SYMLINK;
                break;
            case 't':
                type = optarg[0];
                break;
            case 'F':
                output_fullname = true;
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if (ns_str == NULL || optind >= argc) {
        usage(argv);
        return 1;
    }

    if (type == 0) {
        if (strtoll(argv[optind], &endptr, 10) > 0 && *endptr == '\0') {
            if (optind + 1 == argc) {
                type = FDIR_QUERY_TYPE_INODE;
            } else {
                type = FDIR_QUERY_TYPE_PNAME;
            }
        } else {
            type = FDIR_QUERY_TYPE_FULLNAME;
        }
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    FC_SET_STRING(ns, ns_str);
    if ((result=fdir_client_simple_init_with_auth_ex(
                    config_filename, &ns, publish)) != 0)
    {
        return result;
    }

    FC_SET_STRING_EX(fullname, buff, 0);
    if (type == FDIR_QUERY_TYPE_INODE) {  //inode
        oino.inode = strtoll(argv[optind], &endptr, 10);
        if (*endptr != '\0') {
            fprintf(stderr, "invalid inode number: %s\n", argv[optind]);
            return EINVAL;
        }
        FDIR_SET_OPERATOR(oino.oper, 0, 0, 0, NULL);
        if ((result=fdir_client_stat_dentry_by_inode(&g_fdir_client_vars.
                        client_ctx, &ns, &oino, flags, &dentry)) != 0)
        {
            return result;
        }

        if (output_fullname && (result=fdir_client_get_fullname_by_inode(
                        &g_fdir_client_vars.client_ctx, &ns, &oino, flags,
                        &fullname, sizeof(buff))) != 0)
        {
            return result;
        }
    } else if (type == FDIR_QUERY_TYPE_PNAME) {  //pname
        if (ns_str == NULL || optind + 1 >= argc) {
            usage(argv);
            return 1;
        }

        opname.pname.parent_inode = strtoll(argv[optind], &endptr, 10);
        if (*endptr != '\0') {
            fprintf(stderr, "invalid parent inode number: %s\n", argv[optind]);
            return EINVAL;
        }
        FC_SET_STRING(opname.pname.name, argv[optind + 1]);
        if ((result=fdir_client_stat_dentry_by_pname(&g_fdir_client_vars.
                        client_ctx, &ns, &opname, flags, &dentry)) != 0)
        {
            return result;
        }

        if (output_fullname && (result=fdir_client_get_fullname_by_pname(
                        &g_fdir_client_vars.client_ctx, &ns, &opname, flags,
                        &fullname, sizeof(buff))) != 0)
        {
            return result;
        }
    } else {
        path = argv[optind];
        fname.fullname.ns = ns;
        FC_SET_STRING(fname.fullname.path, path);
        FDIR_SET_OPERATOR(fname.oper, 0, 0, 0, NULL);
        if ((result=fdir_client_stat_dentry_by_path(&g_fdir_client_vars.
                        client_ctx, &fname, flags, &dentry)) != 0)
        {
            return result;
        }
    }

    output_dentry_stat(&dentry);
    if (output_fullname) {
        printf("filepath: %s\n\n", type == FDIR_QUERY_TYPE_FULLNAME ?
                fname.fullname.path.str : fullname.str);
    }

    return 0;
}

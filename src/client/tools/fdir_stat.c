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
            "[-l for follow_symlink] "
            "<-n namespace> <path>\n", argv[0],
            FDIR_CLIENT_DEFAULT_CONFIG_FILENAME);
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
    printf("type: %s, inode: %"PRId64", size: %"PRId64", "
            "stat change time: %s, modify time: %s, access time: %s, "
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
    char *ns;
    char *path;
    FDIRDEntryFullName fullname;
    FDIRDEntryInfo dentry;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    flags = 0;
    ns = NULL;
    while ((ch=getopt(argc, argv, "hc:n:l:")) != -1) {
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
            case 'l':
                flags |= FDIR_FLAGS_FOLLOW_SYMLINK;
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

    if ((result=fdir_client_stat_dentry_by_path(&g_fdir_client_vars.
                    client_ctx, &fullname, flags, &dentry)) != 0)
    {
        return result;
    }
    output_dentry_stat(&dentry);
    return 0;
}

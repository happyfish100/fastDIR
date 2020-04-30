#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "fastdir/fdir_client.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename] "
            "<-n namespace> <path>\n", argv[0]);
}

static void output_dentry_stat(FDIRDEntryInfo *dentry)
{
    char ctime[32];
    char mtime[32];
    char *type;
    int perm;

    perm = dentry->stat.mode & (~S_IFMT);
    if ((dentry->stat.mode & S_IFSOCK)) {
        type = "socket";
    } else if ((dentry->stat.mode & S_IFLNK)) {
        type = "symbolic link";
    } else if ((dentry->stat.mode & S_IFREG)) {
        type = "regular file";
    } else if ((dentry->stat.mode & S_IFBLK)) {
        type = "block device";
    } else if ((dentry->stat.mode & S_IFDIR)) {
        type = "directory";
    } else if ((dentry->stat.mode & S_IFCHR)) {
        type = "character device";
    } else if ((dentry->stat.mode & S_IFIFO)) {
        type = "FIFO";
    } else {
        type = "UNKOWN";
    }

    formatDatetime(dentry->stat.ctime, "%Y-%m-%d %H:%M:%S",
            ctime, sizeof(ctime));
    formatDatetime(dentry->stat.mtime, "%Y-%m-%d %H:%M:%S",
            mtime, sizeof(mtime));
    printf("type: %s, inode: %"PRId64", size: %"PRId64", create time: %s, "
            "modify time: %s, perm: %03o\n", type, dentry->inode,
            dentry->stat.size, ctime, mtime, perm);
}

int main(int argc, char *argv[])
{
	int ch;
    const char *config_filename = "/etc/fdir/client.conf";
    char *ns;
    char *path;
    FDIRDEntryFullName fullname;
    FDIRDEntryInfo dentry;
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
                break;
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
    if ((result=fdir_client_init(config_filename)) != 0) {
        return result;
    }

    FC_SET_STRING(fullname.ns, ns);
    FC_SET_STRING(fullname.path, path);
    if ((result=fdir_client_stat_dentry(&g_fdir_client_vars.client_ctx,
                    &fullname, &dentry)) != 0)
    {
        return result;
    }
    output_dentry_stat(&dentry);
    return 0;
}

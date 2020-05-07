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

static int64_t inode;

static char *config_filename = "/etc/fdir/client.conf";
volatile int thread_count = 0;

static void output_dentry_stat(FDIRDEntryInfo *dentry)
{
    char ctime[32];
    char mtime[32];
    char *type;
    int perm;

    perm = dentry->stat.mode & (~S_IFMT);
    if ((dentry->stat.mode & S_IFIFO)) {
        type = "FIFO";
    } else if ((dentry->stat.mode & S_IFCHR)) {
        type = "character device";
    } else if ((dentry->stat.mode & S_IFDIR)) {
        type = "directory";
    } else if ((dentry->stat.mode & S_IFBLK)) {
        type = "block device";
    } else if ((dentry->stat.mode & S_IFREG)) {
        type = "regular file";
    } else if ((dentry->stat.mode & S_IFLNK)) {
        type = "symbolic link";
    } else if ((dentry->stat.mode & S_IFSOCK)) {
        type = "socket";
    } else {
        type = "UNKOWN";
    }

    formatDatetime(dentry->stat.ctime, "%Y-%m-%d %H:%M:%S",
            ctime, sizeof(ctime));
    formatDatetime(dentry->stat.mtime, "%Y-%m-%d %H:%M:%S",
            mtime, sizeof(mtime));
    printf("type: %s, inode: %"PRId64", size: %"PRId64", create time: %s, "
            "modify time: %s, perm: 0%03o\n", type, dentry->inode,
            dentry->stat.size, ctime, mtime, perm);
}

static void *thread_func(void *args)
{
    long thread_index;
    FDIRClientContext client_ctx;
    FDIRDEntryInfo dentry;
	int result;

    thread_index = (long)args;

    if ((result=fdir_client_init_ex(&client_ctx,
                    config_filename, NULL)) != 0)
    {
        return NULL;
    }

    do {
        if ((result=fdir_client_flock_dentry(&client_ctx,
                        LOCK_EX, inode)) != 0)
        {
            fprintf(stderr, "flock_dentry fail, thread: %ld, inode: %"PRId64", "
                    "errno: %d, error info: %s\n", thread_index,
                    inode, result, STRERROR(result));
            break;
        }

        if ((result=fdir_client_stat_dentry_by_inode(&client_ctx,
                        inode, &dentry)) != 0)
        {
            break;
        }

        printf("thread index: %ld\n", thread_index);
        output_dentry_stat(&dentry);

        if ((result=fdir_client_flock_dentry(&client_ctx,
                        LOCK_UN, inode)) != 0)
        {
            fprintf(stderr, "flock_dentry fail, thread: %ld, inode: %"PRId64", "
                    "errno: %d, error info: %s\n", thread_index,
                    inode, result, STRERROR(result));
            break;
        }
    } while (0);

    //TODO  unlock
    __sync_sub_and_fetch(&thread_count, 1);

    return NULL;
}

int main(int argc, char *argv[])
{
	int ch;
    char *ns;
    char *path;
    FDIRDEntryFullName fullname;
	int result;
    pthread_t tid;
    long i;

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

    if ((result=fdir_client_lookup_inode(&g_fdir_client_vars.client_ctx,
                    &fullname, &inode)) != 0)
    {
        return result;
    }

    for (i=0; i<2; i++) {
        if (fc_create_thread(&tid, thread_func, (void *)i, 64 * 1024) == 0) {
            __sync_add_and_fetch(&thread_count, 1);
        }
    }

    while (thread_count != 0) {
        usleep(10000);
    }

    return 0;
}

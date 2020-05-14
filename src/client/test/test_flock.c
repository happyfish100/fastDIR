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
    fprintf(stderr, "Usage: %s [-c config_filename = /etc/fdir/client.conf] "
            "[-N non-block] [-S do NOT output dentry stat] "
            "[-s sleep micro seconds = 0] [-t thread count = 8] "
            "<-n namespace> <path>\n", argv[0]);
}

static int64_t inode;

static char *config_filename = "/etc/fdir/client.conf";
static int flock_flags = 0;
static int usleep_time = 0;
static int threads = 8;
static bool output_stat = true;
static volatile int thread_count = 0;
static volatile int success_count = 0;

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
    FDIRClientSession session;
    FDIRDEntryInfo dentry;
    char buff[32];
    int operation;
	int result;

    thread_index = (long)args;
    memset(&session, 0, sizeof(session));

    do {
        int64_t offset;
        int64_t length;
        if ((result=fdir_client_init_ex(&client_ctx,
                        config_filename, NULL)) != 0)
        {
            break;
        }

        if ((result=fdir_client_init_session(&client_ctx,
                        &session)) != 0)
        {
            break;
        }


        if (thread_index % 2 == 0) {
            operation = LOCK_SH;
        } else {
            operation = LOCK_EX;
        }

        offset = thread_index;
        length = 4 * offset;
        if ((result=fdir_client_flock_dentry_ex(&session,
                        inode, operation | flock_flags, offset, length)) != 0)
        {
            fprintf(stderr, "dentry lock fail, thread: %ld, inode: %"PRId64", "
                    "errno: %d, error info: %s\n", thread_index,
                    inode, result, STRERROR(result));
            break;
        }

        if ((result=fdir_client_stat_dentry_by_inode(&client_ctx,
                        inode, &dentry)) != 0)
        {
            break;
        }


        if (output_stat) {
            printf("[%s] thread index: %ld\n",
                    formatDatetime(time(NULL), "%Y-%m-%d %H:%M:%S",
                        buff, sizeof(buff)), thread_index);
            output_dentry_stat(&dentry);
        }

        if (usleep_time > 0) {
            usleep(usleep_time);
        }

        if ((result=fdir_client_flock_dentry_ex(&session,
                        inode, LOCK_UN | flock_flags, offset, length)) != 0)
        {
            fprintf(stderr, "dentry unlock fail, thread: %ld, inode: %"PRId64", "
                    "errno: %d, error info: %s\n", thread_index,
                    inode, result, STRERROR(result));
            break;
        }
        __sync_add_and_fetch(&success_count, 1);
    } while (0);

    fdir_client_close_session(&session, result != 0);
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
    int64_t start_time;
    int64_t time_used;
    char time_buff[32];

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    ns = NULL;
    while ((ch=getopt(argc, argv, "hc:n:NSs:t:")) != -1) {
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
            case 'N':
                flock_flags = LOCK_NB;
                break;
            case 'S':
                output_stat  = false;
                break;
            case 's':
                usleep_time = strtol(optarg, NULL, 10);
                break;
            case 't':
                threads = strtol(optarg, NULL, 10);
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

    start_time = get_current_time_ms();

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

    for (i=0; i<threads; i++) {
        if (fc_create_thread(&tid, thread_func, (void *)i, 64 * 1024) == 0) {
            __sync_add_and_fetch(&thread_count, 1);
        }
    }

    while (thread_count != 0) {
        usleep(10000);
    }

    time_used = get_current_time_ms() - start_time;
    printf("threads: %d, success_count: %d, time used: %s ms\n",
            threads, __sync_add_and_fetch(&success_count, 0),
            long_to_comma_str(time_used, time_buff));

    return 0;
}

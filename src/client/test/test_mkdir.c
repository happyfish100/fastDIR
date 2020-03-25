#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "fastdir/fdir_client.h"

#define SUBDIR_COUNT  300

static char *ns = "test";
static char *base_path = "/test";
static char true_base_path[PATH_MAX];
static bool ignore_exist_error = false;
static int total_count = 0;
static int ignore_count = 0;

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename] "
            "[-n namespace=test] [-b base_path=/test] "
            "[-i for ignoring exist error]\n", argv[0]);
}

static int create_dentry(FDIRDEntryFullName *fullname)
{
	int result;
    mode_t mode = 0755 | S_IFDIR;
    const int flags = 0;

    ++total_count;
    if ((result=fdir_client_create_dentry(&g_client_global_vars.client_ctx,
                    fullname, flags, mode)) != 0)
    {
        if (ignore_exist_error && result == EEXIST) {
            ++ignore_count;
            result = 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "create_dentry %.*s fail, namespace: %s, "
                    "errno: %d, error info: %s", __LINE__,
                    fullname->path.len, fullname->path.str,
                    fullname->ns.str, result, STRERROR(result));
        }
    }
    return result;
}

static int create_base_path()
{
#define MAX_SUBDIR_COUNT 8

    string_t path;
    string_t parts[MAX_SUBDIR_COUNT];
    FDIRDEntryFullName fullname;
    int result;
    int count;
    int len;
    int i;

    FC_SET_STRING(path, base_path);
    count = split_string_ex(&path, '/', parts, MAX_SUBDIR_COUNT, true);

    FC_SET_STRING(fullname.ns, ns);
    fullname.path.str = true_base_path;
    strcpy(true_base_path, "/");
    len = 1;
    fullname.path.len = len;
    if ((result=create_dentry(&fullname)) != 0) {
        if (result != EEXIST) {
            return result;
        }
    }

    for (i=0; i<count; i++) {
        if (i > 0) {
            *(true_base_path + len++) = '/';
        }
        memcpy(true_base_path + len, parts[i].str, parts[i].len);
        len += parts[i].len;

        fullname.path.len = len;
        if ((result=create_dentry(&fullname)) != 0) {
            if (result != EEXIST) {
                return result;
            }
        }
    }

    *(true_base_path + len) = '\0';
    return 0;
}

static int test_case()
{
    FDIRDEntryFullName entry_info;
    char path[256];
	int result;
    int i;
    int k;

    if ((result=create_base_path()) != 0) {
        return result;
    }

    FC_SET_STRING(entry_info.ns, ns);
    entry_info.path.str = path;
    for (i=0; i<SUBDIR_COUNT; i++) {
        entry_info.path.len = sprintf(path, "%s/%03d",
                base_path, i + 1);
        if ((result=create_dentry(&entry_info)) != 0) {
            return result;
        }
        for (k=0; k<SUBDIR_COUNT; k++) {
            entry_info.path.len = sprintf(path, "%s/%03d/%03d",
                    base_path, i + 1, k + 1);
            if ((result=create_dentry(&entry_info)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

int main(int argc, char *argv[])
{
	int ch;
    char time_buff[32];
    const char *config_filename = "/etc/fdir/client.conf";
    int64_t start_time; 
    int64_t time_used;
	int result;

    while ((ch=getopt(argc, argv, "hic:n:b:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'n':
                ns = optarg;
                break;
            case 'b':
                base_path = optarg;
                break;
            case 'c':
                config_filename = optarg;
                break;
            case 'i':
                ignore_exist_error = true;
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    if ((result=fdir_client_init(config_filename)) != 0) {
        return result;
    }

    start_time = get_current_time_ms();
    result = test_case();
    time_used = get_current_time_ms() - start_time;
    printf("create %d dentry, ignore count: %d, time used: %s ms\n", 
            total_count, ignore_count,
            long_to_comma_str(time_used, time_buff));

    return result;
}

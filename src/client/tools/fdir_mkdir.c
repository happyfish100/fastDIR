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
    fprintf(stderr, "Usage: %s [-c config_filename] [-m mode] "
            "<-n namespace> <path>\n", argv[0]);
}

int main(int argc, char *argv[])
{
	int ch;
    const char *config_filename = "/etc/fdir/client.conf";
    char *ns;
    char *path;
    FDIRDEntryFullName fullname;
	int result;
    int base;
    char *endptr;
    mode_t mode = 0755;
    FDIRDEntryInfo dentry;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    ns = NULL;
    while ((ch=getopt(argc, argv, "hc:m:n:")) != -1) {
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
            case 'm':
                if (optarg[0] == '0') {
                    base = 8;
                } else {
                    base = 10;
                }
                mode = strtol(optarg, &endptr, base);
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

    mode |= S_IFDIR;
    FC_SET_STRING(fullname.ns, ns);
    FC_SET_STRING(fullname.path, path);
    return fdir_client_create_dentry(&g_fdir_client_vars.client_ctx,
                    &fullname, mode, &dentry);
}

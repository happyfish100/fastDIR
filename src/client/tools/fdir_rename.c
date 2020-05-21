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
    fprintf(stderr, "Usage: %s [-c config_filename] [-s swap two files] "
            "[-f force overwrite] <-n namespace> <old path> <new path>\n",
            argv[0]);
}

int main(int argc, char *argv[])
{
	int ch;
    const char *config_filename = "/etc/fdir/client.conf";
    char *ns;
    char *src_path;
    char *dest_path;
    FDIRDEntryFullName src_fullname;
    FDIRDEntryFullName dest_fullname;
    int flags;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    flags = RENAME_NOREPLACE;
    ns = NULL;
    while ((ch=getopt(argc, argv, "hc:n:sf")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                break;
            case 'n':
                ns = optarg;
                break;
            case 's':
                flags |= (RENAME_EXCHANGE & (~RENAME_NOREPLACE));
                break;
            case 'f':
                flags &= ~RENAME_NOREPLACE;
                break;
            case 'c':
                config_filename = optarg;
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if (ns == NULL || optind + 1 >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    src_path = argv[optind];
    dest_path = argv[optind + 1];
    if ((result=fdir_client_simple_init(config_filename)) != 0) {
        return result;
    }

    FC_SET_STRING(src_fullname.ns, ns);
    FC_SET_STRING(src_fullname.path, src_path);
    FC_SET_STRING(dest_fullname.ns, ns);
    FC_SET_STRING(dest_fullname.path, dest_path);
    return fdir_client_rename_dentry(&g_fdir_client_vars.client_ctx,
                    &src_fullname, &dest_fullname, flags);
}

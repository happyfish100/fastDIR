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
    fprintf(stderr, "Usage: %s [-c config_filename]\n", argv[0]);
}

static void output(FDIRClientClusterStatEntry *stats, const int count)
{
    FDIRClientClusterStatEntry *stat;
    FDIRClientClusterStatEntry *end;

    end = stats + count;
    for (stat=stats; stat<end; stat++) {
        printf( "server_id: %d, host: %s:%d, "
                "status: %d (%s), "
                "is_master: %d\n",
                stat->server_id,
                stat->ip_addr, stat->port,
                stat->status,
                fdir_get_server_status_caption(stat->status),
                stat->is_master
              );
    }
    printf("\nserver count: %d\n\n", count);
}

int main(int argc, char *argv[])
{
#define CLUSTER_MAX_SERVER_COUNT  16
	int ch;
    const char *config_filename = "/etc/fdir/client.conf";
    int count;
    FDIRClientClusterStatEntry stats[CLUSTER_MAX_SERVER_COUNT];
	int result;

    /*
    if (argc < 2) {
        usage(argv);
        return 1;
    }
    */

    while ((ch=getopt(argc, argv, "hc:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                break;
            case 'c':
                config_filename = optarg;
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

    if ((result=fdir_client_cluster_stat(&g_fdir_client_vars.client_ctx,
                    stats, CLUSTER_MAX_SERVER_COUNT, &count)) != 0)
    {
        fprintf(stderr, "fdir_client_cluster_stat fail, "
                "errno: %d, error info: %s\n", result, STRERROR(result));
        return result;
    }

    output(stats, count);
    return 0;
}

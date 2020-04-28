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
            "host[:port]\n", argv[0]);
}


static void output(FDIRClientServiceStat *stat)
{
    printf( "\tserver_id: %d\n"
            "\tstatus: %d (%s)\n"
            "\tis_master: %d\n"
            "\tconnection : {current: %d, max: %d}\n"
            "\tdentry : {current_data_version: %"PRId64", "
            "current_inode_sn: %"PRId64", "
            "ns_count: %"PRId64", "
            "dir_count: %"PRId64", "
            "file_count: %"PRId64"}\n\n",
            stat->server_id, stat->status,
            fdir_get_server_status_caption(stat->status),
            stat->is_master,
            stat->connection.current_count,
            stat->connection.max_count,
            stat->dentry.current_data_version,
            stat->dentry.current_inode_sn,
            stat->dentry.counters.ns,
            stat->dentry.counters.dir,
            stat->dentry.counters.file
          );
}

int main(int argc, char *argv[])
{
	int ch;
    const char *config_filename = "/etc/fdir/client.conf";
    char *host;
    ConnectionInfo conn;
    FDIRClientServiceStat stat;
	int result;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

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

    if (optind >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    host = argv[optind];
    if ((result=fdir_client_init(config_filename)) != 0) {
        return result;
    }

    if ((result=conn_pool_parse_server_info(host, &conn,
                    FDIR_SERVER_DEFAULT_SERVICE_PORT)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_service_stat(&g_fdir_client_vars.client_ctx,
                    conn.ip_addr, conn.port, &stat)) != 0) {
        return result;
    }

    output(&stat);
    return 0;
}

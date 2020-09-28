#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_write.h"

SFBinlogWriterContext g_binlog_writer_ctx;

int binlog_write_init()
{
    int result;

    if ((result=sf_binlog_writer_init_by_version(&g_binlog_writer_ctx.writer,
                    FDIR_BINLOG_SUBDIR_NAME, DATA_CURRENT_VERSION + 1,
                    BINLOG_BUFFER_SIZE, 4096)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_init_thread(&g_binlog_writer_ctx.thread,
            &g_binlog_writer_ctx.writer, SF_BINLOG_WRITER_TYPE_ORDER_BY_VERSION,
            FDIR_BINLOG_MAX_RECORD_SIZE);
}

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
#include "sf/sf_global.h"
#include "server_global.h"
#include "server_binlog.h"
#include "data_thread.h"
#include "data_loader.h"

int server_load_data()
{
    BinlogReplayContext replay_ctx;
    BinlogReadThreadContext reader_ctx;
    BinlogReadThreadResult *r;
    int64_t start_time;
    int64_t end_time;
    char time_buff[32];
    int result;

    start_time = get_current_time_ms();

    if ((result=binlog_read_thread_init(&reader_ctx, NULL, 0,
                    BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    if ((result=binlog_replay_init(&replay_ctx, 64)) != 0) {
        return result;
    }

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((r=binlog_read_thread_fetch_result(&reader_ctx)) == NULL) {
            result = EINTR;
            break;
        }

        logInfo("errno: %d, buffer length: %d", r->err_no, r->buffer.length);
        if (r->err_no == ENOENT) {
            break;
        } else if (r->err_no != 0) {
            result = r->err_no;
            break;
        }

        if ((result=binlog_replay_deal_buffer(&replay_ctx,
                        r->buffer.buff, r->buffer.length)) != 0)
        {
            break;
        }

        binlog_read_thread_return_result_buffer(&reader_ctx, r);
    }

    binlog_replay_destroy(&replay_ctx);
    binlog_read_thread_terminate(&reader_ctx);

    end_time = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "load data done. record count: %"PRId64", "
            "skip count: %"PRId64", warning count: %"PRId64
            ", fail count: %"PRId64", time used: %s ms",
            __LINE__, replay_ctx.record_count,
            replay_ctx.skip_count, replay_ctx.warning_count,
            replay_ctx.fail_count, long_to_comma_str(
                end_time - start_time, time_buff));
    return result;
}

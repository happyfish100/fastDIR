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

typedef struct data_loader_context {
    struct {
        int count;
        FDIRBinlogRecord *records;
    } record_array;
    volatile int waiting_count;
    struct timespec ts;
    int last_errno;
    volatile int fail_count;
    int64_t record_count;
} DataLoaderContext;

static void data_thread_deal_done_callback(const int result,
        FDIRBinlogRecord *record)
{
    DataLoaderContext *loader_ctx;

    loader_ctx = (DataLoaderContext *)record->notify.args;
    if (result != 0) {
        loader_ctx->last_errno = result;
        __sync_add_and_fetch(&loader_ctx->fail_count, 1);
    }
    __sync_sub_and_fetch(&loader_ctx->waiting_count, 1);
}

static int server_deal_binlog_buffer(DataLoaderContext *loader_ctx,
        BinlogReadThreadResult *r)
{
    const char *p;
    const char *end;
    const char *rend;
    FDIRBinlogRecord *record;
    char error_info[FDIR_ERROR_INFO_SIZE];
    int result;
    int count;

    count = 0;
    p = r->buffer.buff;
    end = p + r->buffer.length;
    while (p < end) {
        record = loader_ctx->record_array.records + count;
        if ((result=binlog_unpack_record(p, end - p, record,
                        &rend, error_info, sizeof(error_info))) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "%s", __LINE__, error_info);
            return result;
        }
        p = rend;

        loader_ctx->record_count++;
        __sync_add_and_fetch(&loader_ctx->waiting_count, 1);

        record->notify.args = loader_ctx;
        record->notify.func = data_thread_deal_done_callback;
        if ((result=push_to_data_thread_queue(record)) != 0) {
            return result;
        }

        if (++count == loader_ctx->record_array.count) {
            while (__sync_add_and_fetch(&loader_ctx->waiting_count, 0) != 0) {
                nanosleep(&loader_ctx->ts, NULL);
            }
            if (loader_ctx->fail_count > 0) {
                return loader_ctx->last_errno;
            }
            count = 0;
        }
    }

    logInfo("record_count: %"PRId64", waiting_count: %d", loader_ctx->record_count,
            __sync_add_and_fetch(&loader_ctx->waiting_count, 0));

    while (__sync_add_and_fetch(&loader_ctx->waiting_count, 0) != 0) {
        nanosleep(&loader_ctx->ts, NULL);
    }
    return loader_ctx->fail_count > 0 ? loader_ctx->last_errno : 0;
}

int server_load_data()
{
    DataLoaderContext loader_ctx;
    BinlogReadThreadContext reader_ctx;
    BinlogReadThreadResult *r;
    int64_t start_time;
    int64_t end_time;
    int result;
    int bytes;

    start_time = get_current_time_ms();

    if ((result=binlog_read_thread_init(&reader_ctx, NULL, 0,
                    BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    loader_ctx.record_count = 0;
    loader_ctx.last_errno = 0;
    loader_ctx.fail_count = 0;
    loader_ctx.waiting_count = 0;
    loader_ctx.ts.tv_sec = 0;
    loader_ctx.ts.tv_nsec = 10 * 1000;
    loader_ctx.record_array.count = 8 * DATA_THREAD_COUNT;
    bytes = sizeof(FDIRBinlogRecord) * loader_ctx.record_array.count;
    loader_ctx.record_array.records = (FDIRBinlogRecord *)malloc(bytes);
    if (loader_ctx.record_array.records == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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

        if ((result=server_deal_binlog_buffer(&loader_ctx, r)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&reader_ctx, r);
    }

    free(loader_ctx.record_array.records);
    binlog_read_thread_terminate(&reader_ctx);

    end_time = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "load data done. record count: %"PRId64", fail count: %d, "
            "time used: %"PRId64"ms", __LINE__, loader_ctx.record_count,
            loader_ctx.fail_count, end_time - start_time);
    return result;
}

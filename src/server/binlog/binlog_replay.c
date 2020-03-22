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
#include "../server_global.h"
#include "../data_thread.h"
#include "binlog_pack.h"
#include "binlog_replay.h"

static void data_thread_deal_done_callback(
        struct fdir_binlog_record *record,
        const int result, const bool is_error)
{
    BinlogReplayContext *replay_ctx;
    int log_level;

    replay_ctx = (BinlogReplayContext *)record->notify.args;
    if (result != 0) {
        if (is_error) {
            log_level = LOG_ERR;
            replay_ctx->last_errno = result;
            __sync_add_and_fetch(&replay_ctx->fail_count, 1);
        } else {
            log_level = LOG_WARNING;
            __sync_add_and_fetch(&replay_ctx->warning_count, 1);
        }

        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, "
                "%s dentry fail, errno: %d, error info: %s, "
                "namespace: %.*s, path: %.*s",
                __LINE__, get_operation_caption(record->operation),
                result, STRERROR(result),
                record->fullname.ns.len, record->fullname.ns.str,
                record->fullname.path.len, record->fullname.path.str);
    }
    if (replay_ctx->notify.func != NULL) {
        replay_ctx->notify.func(result, record, replay_ctx->notify.args);
    }
    __sync_sub_and_fetch(&replay_ctx->waiting_count, 1);
}

int binlog_replay_init_ex(BinlogReplayContext *replay_ctx,
        binlog_replay_notify_func notify_func, void *args,
        const int batch_size)
{
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *rend;
    int bytes;

    replay_ctx->record_count = 0;
    replay_ctx->skip_count = 0;
    replay_ctx->warning_count = 0;
    replay_ctx->fail_count = 0;
    replay_ctx->last_errno = 0;
    replay_ctx->waiting_count = 0;
    replay_ctx->ts.tv_sec = 0;
    replay_ctx->ts.tv_nsec = 10 * 1000;
    replay_ctx->notify.func = notify_func;
    replay_ctx->notify.args  = args;
    replay_ctx->data_current_version = __sync_add_and_fetch(
            &DATA_CURRENT_VERSION, 0);
    replay_ctx->record_array.size = batch_size * DATA_THREAD_COUNT;
    bytes = sizeof(FDIRBinlogRecord) * replay_ctx->record_array.size;
    replay_ctx->record_array.records = (FDIRBinlogRecord *)malloc(bytes);
    if (replay_ctx->record_array.records == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(replay_ctx->record_array.records, 0, bytes);

    rend = replay_ctx->record_array.records + replay_ctx->record_array.size;
    for (record=replay_ctx->record_array.records; record<rend; record++) {
        record->notify.func = data_thread_deal_done_callback;
        record->notify.args = replay_ctx;
    }

    return 0;
}

void binlog_replay_destroy(BinlogReplayContext *replay_ctx)
{
    if (replay_ctx->record_array.records != NULL) {
        free(replay_ctx->record_array.records);
        replay_ctx->record_array.records = NULL;
    }
}

int binlog_replay_deal_buffer(BinlogReplayContext *replay_ctx,
         const char *buff, const int len)
{
    const char *p;
    const char *end;
    const char *rend;
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *rec_end;
    char error_info[FDIR_ERROR_INFO_SIZE];
    int result;

    p = buff;
    end = p + len;
    while (p < end) {
        record = replay_ctx->record_array.records;
        while (p < end) {
            if ((result=binlog_unpack_record(p, end - p, record,
                            &rend, error_info, sizeof(error_info))) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "%s", __LINE__, error_info);
                return result;
            }
            p = rend;

            replay_ctx->record_count++;
            if (record->data_version <= replay_ctx->data_current_version) {
                replay_ctx->skip_count++;
                if (replay_ctx->notify.func != NULL) {
                    replay_ctx->notify.func(0, record, replay_ctx->notify.args);
                }
                continue;
            }

            replay_ctx->data_current_version = record->data_version;
            if (++record - replay_ctx->record_array.records ==
                    replay_ctx->record_array.size)
            {
                break;
            }
        }

        rec_end = record;
        __sync_add_and_fetch(&replay_ctx->waiting_count,
                rec_end - replay_ctx->record_array.records);
        for (record=replay_ctx->record_array.records;
                record<rec_end; record++)
        {
            if ((result=push_to_data_thread_queue(record)) != 0) {
                replay_ctx->fail_count++;
                return result;
            }
        }

        /*
        logInfo("count2: %d, waiting_count: %d",
                (int)(rec_end - replay_ctx->record_array.records),
                replay_ctx->waiting_count);
                */

        while (__sync_add_and_fetch(&replay_ctx->waiting_count, 0) != 0) {
            nanosleep(&replay_ctx->ts, NULL);
        }
        if (replay_ctx->fail_count > 0) {
            return replay_ctx->last_errno;
        }
    }

    /*
    logInfo("record_count: %"PRId64", waiting_count: %d, skip_count: %"PRId64,
            replay_ctx->record_count, __sync_add_and_fetch(&replay_ctx->waiting_count, 0),
            replay_ctx->skip_count);
            */

    return replay_ctx->fail_count > 0 ? replay_ctx->last_errno : 0;
}

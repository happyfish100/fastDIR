/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

//binlog_replay.h

#ifndef _BINLOG_REPLAY_H_
#define _BINLOG_REPLAY_H_

#include <pthread.h>
#include "fastcommon/fast_mpool.h"
#include "binlog_types.h"

typedef void (*binlog_replay_notify_func)(const int result,
        struct fdir_binlog_record *record, void *args);

typedef struct binlog_replay_context {
    struct {
        int size;
        FDIRBinlogRecord *records;
    } record_array;
    struct fast_mpool_man mpool;   //for binlog_unpack_record
    int64_t data_current_version;
    volatile int waiting_count;
    int last_errno;
    int64_t record_count;
    int64_t skip_count;
    int64_t warning_count;
    volatile int64_t fail_count;
    pthread_lock_cond_pair_t lcp;
    struct {
        binlog_replay_notify_func func;
        void *args;
    } notify;
} BinlogReplayContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replay_init_ex(BinlogReplayContext *replay_ctx,
        binlog_replay_notify_func notify_func, void *args,
        const int batch_size);

#define binlog_replay_init(replay_ctx, batch_size) \
        binlog_replay_init_ex(replay_ctx, NULL, NULL, batch_size)

void binlog_replay_destroy(BinlogReplayContext *replay_ctx);

int binlog_replay_deal_buffer(BinlogReplayContext *replay_ctx,
         const char *buff, const int len,
         SFBinlogFilePosition *binlog_position);

#ifdef __cplusplus
}
#endif

#endif

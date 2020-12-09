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

//replica_consumer_thread.h

#ifndef _REPLICA_CONSUMER_THREAD_H_
#define _REPLICA_CONSUMER_THREAD_H_

#include "fastcommon/fast_mblock.h"
#include "binlog_types.h"
#include "binlog_replay.h"

#define REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT   64
#define REPLICA_CONSUMER_THREAD_BUFFER_COUNT      \
    REPLICA_CONSUMER_THREAD_INPUT_BUFFER_COUNT

typedef struct replica_consumer_thread_result {
    short err_no;
    int64_t data_version;
} RecordProcessResult;

typedef struct replica_consumer_thread_context {
    volatile bool continue_flag;
    bool running;
    pthread_t tid;
    struct fast_mblock_man result_allocator;
    ServerBinlogRecordBuffer binlog_buffers[REPLICA_CONSUMER_THREAD_BUFFER_COUNT];
    struct {
        struct common_blocked_queue free;   //free ServerBinlogRecordBuffer ptr
        struct common_blocked_queue input;  //input ServerBinlogRecordBuffer ptr
        struct common_blocked_queue result; //record deal result
    } queues;

    struct fast_task_info *task;
    ServerBinlogRecordBuffer *recv_rbuffer;

    BinlogReplayContext replay_ctx;
} ReplicaConsumerThreadContext;

#ifdef __cplusplus
extern "C" {
#endif

ReplicaConsumerThreadContext *replica_consumer_thread_init(
        struct fast_task_info *task, const int buffer_size, int *err_no);

int deal_replica_push_request(ReplicaConsumerThreadContext *ctx,
        char *binlog_buff, const int length,
        const SFVersionRange *data_version);

int deal_replica_push_task(ReplicaConsumerThreadContext *ctx);

void replica_consumer_thread_terminate(ReplicaConsumerThreadContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

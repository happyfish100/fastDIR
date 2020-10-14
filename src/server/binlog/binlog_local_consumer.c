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
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_write.h"
#include "binlog_replication.h"
#include "binlog_producer.h"
#include "binlog_local_consumer.h"

static FDIRSlaveReplicationArray slave_replication_array;

static int init_binlog_local_consumer_array()
{
    int result;
    int count;
    int bytes;
    FDIRClusterServerInfo *server;
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;

    count = CLUSTER_SERVER_ARRAY.count - 1;
    if (count == 0) {
        slave_replication_array.count = count;
        return 0;
    }

    bytes = sizeof(FDIRSlaveReplication) * count;
    slave_replication_array.replications = (FDIRSlaveReplication *)
        fc_malloc(bytes);
    if (slave_replication_array.replications == NULL) {
        return ENOMEM;
    }
    memset(slave_replication_array.replications, 0, bytes);

    server = CLUSTER_SERVER_ARRAY.servers;
    end = slave_replication_array.replications + count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        if (server == CLUSTER_MYSELF_PTR) {
            ++server;   //skip myself
        }

        replication->index = replication - slave_replication_array.replications;
        replication->slave = server++;
        replication->connection_info.conn.sock = -1;
        if ((result=init_pthread_lock(&replication->context.queue.lock)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "init_pthread_lock fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    slave_replication_array.count = count;
    return 0;
}

int binlog_local_consumer_init()
{
    int result;

    if ((result=init_binlog_local_consumer_array()) != 0) {
        return result;
    }
    return binlog_write_init();
}

int binlog_local_consumer_replication_start()
{
    int result;
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        if ((result=binlog_replication_bind_thread(replication)) != 0) {
            return result;
        }
    }

    return 0;
}

void binlog_local_consumer_destroy()
{
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;
    if (slave_replication_array.replications == NULL) {
        return;
    }

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        pthread_mutex_destroy(&replication->context.queue.lock);
    }
    free(slave_replication_array.replications);
    slave_replication_array.replications = NULL;
}

void binlog_local_consumer_terminate()
{
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;

    binlog_write_finish();
    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications;
            replication<end; replication++)
    {
        if (replication->task != NULL) {
            ioevent_notify_thread(replication->task->thread_data);
        }
    }
}

static void push_to_slave_replica_queues(FDIRSlaveReplication *replication,
        ServerBinlogRecordBuffer *rbuffer)
{
    bool notify;

    rbuffer->nexts[replication->index] = NULL;
    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    if (replication->context.queue.tail == NULL) {
        replication->context.queue.head = rbuffer;
        notify = true;
    } else {
        replication->context.queue.tail->nexts[replication->index] = rbuffer;
        notify = false;
    }

    replication->context.queue.tail = rbuffer;
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (notify) {
        ioevent_notify_thread(replication->task->thread_data);
    }
}

int binlog_local_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer)
{
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;
    struct fast_task_info *task;

    __sync_add_and_fetch(&rbuffer->reffer_count,
            slave_replication_array.count);

    task = (struct fast_task_info *)rbuffer->args;
    __sync_add_and_fetch(&((FDIRServerTaskArg *)task->arg)->context.
            service.waiting_rpc_count, slave_replication_array.count);

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++) {
        push_to_slave_replica_queues(replication, rbuffer);
    }

    return 0;
}

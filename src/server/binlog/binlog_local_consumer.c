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
#include "binlog_write_thread.h"
#include "binlog_replication.h"
#include "binlog_local_consumer.h"

static FDIRSlaveReplicationArray slave_replication_array;

static int init_binlog_local_consumer_array()
{
    static bool inited = false;
    int result;
    int count;
    int bytes;
    FDIRClusterServerInfo *server;
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;

    if (inited) {
        return 0;
    }

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
    inited = true;
    return 0;
}

int binlog_local_consumer_init()
{
    int result;
    pthread_t tid;

    if ((result=binlog_write_thread_init()) != 0) {
        return result;
    }

    return fc_create_thread(&tid, binlog_write_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}

int binlog_local_consumer_replication_start()
{
    int result;
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;

    if ((result=init_binlog_local_consumer_array()) != 0) {
        return result;
    }

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

    if (g_writer_queue != NULL) {
        common_blocked_queue_terminate(g_writer_queue);
    }

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++) {
        iovent_notify_thread(replication->task->thread_data);
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
        iovent_notify_thread(replication->task->thread_data);
    }
}

int binlog_local_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer)
{
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;
    struct fast_task_info *task;
    int result;

    __sync_add_and_fetch(&rbuffer->reffer_count,
            slave_replication_array.count + 1);

     if ((result=push_to_binlog_write_queue(rbuffer)) != 0) {
        logCrit("file: "__FILE__", line: %d, "
                "push_to_binlog_write_queue fail, program exit!",
                __LINE__);
        SF_G_CONTINUE_FLAG = false;
        return result;
    }

    if (slave_replication_array.count == 0) {
        return 0;
    }

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

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
#include "binlog_consumer.h"

static FDIRSlaveReplicationArray slave_replication_array;

static int init_binlog_consumer_array()
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
        malloc(bytes);
    if (slave_replication_array.replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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
        if ((result=init_pthread_lock(&replication->queue.lock)) != 0) {
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

static int binlog_write_thread_start()
{
    int result;
    pthread_t tid;
    pthread_attr_t thread_attr;

    if ((result=init_pthread_attr(&thread_attr, SF_G_THREAD_STACK_SIZE)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_attr fail, program exit!", __LINE__);
        return result;
    }
    if ((result=pthread_create(&tid, &thread_attr,
                    binlog_write_thread_func, NULL)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "create thread failed, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }
    pthread_attr_destroy(&thread_attr);
    return 0;
}

int binlog_consumer_init()
{
    int result;

    if ((result=binlog_write_thread_init()) != 0) {
        return result;
    }

    return binlog_write_thread_start();
}

int binlog_consumer_replication_start()
{
    int result;
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;

    if ((result=init_binlog_consumer_array()) != 0) {
        return result;
    }

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        if ((result=binlog_replication_add_to_thread(replication)) != 0) {
            return result;
        }
    }

    return 0;
}

void binlog_consumer_destroy()
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
        pthread_mutex_destroy(&replication->queue.lock);
        //TODO remove from nio task
    }
    free(slave_replication_array.replications);
    slave_replication_array.replications = NULL;
}

void binlog_consumer_terminate()
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
    pthread_mutex_lock(&replication->queue.lock);
    if (replication->queue.tail == NULL) {
        replication->queue.head = rbuffer;
        notify = true;
    } else {
        replication->queue.tail->nexts[replication->index] = rbuffer;
        notify = false;
    }

    replication->queue.tail = rbuffer;
    pthread_mutex_unlock(&replication->queue.lock);

    if (notify) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

int binlog_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer)
{
    FDIRSlaveReplication *replication;
    FDIRSlaveReplication *end;
    int result;

    __sync_add_and_fetch(&rbuffer->reffer_count,
            slave_replication_array.count + 1);
    __sync_add_and_fetch(&((FDIRServerTaskArg *)rbuffer->task->arg)->context.
            service.waiting_rpc_count, slave_replication_array.count);

    if ((result=common_blocked_queue_push(g_writer_queue, rbuffer)) != 0) {
        logCrit("file: "__FILE__", line: %d, "
                "common_blocked_queue_push fail, program exit!",
                __LINE__);
        SF_G_CONTINUE_FLAG = false;
        return result;
    }

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++) {
        push_to_slave_replica_queues(replication, rbuffer);
    }

    return 0;
}

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
#include "binlog_write_thread.h"
#include "binlog_sync_thread.h"
#include "binlog_consumer.h"

ServerBinlogConsumerArray g_binlog_consumer_array;

static int init_binlog_consumer_array()
{
    int result;
    int count;
    int bytes;
    ServerBinlogConsumerContext *context;
    ServerBinlogConsumerContext *end;

    count = CLUSTER_SERVER_ARRAY.count;
    bytes = sizeof(ServerBinlogConsumerContext) * count;
    g_binlog_consumer_array.contexts = (ServerBinlogConsumerContext *)
        malloc(bytes);
    if (g_binlog_consumer_array.contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    end = g_binlog_consumer_array.contexts + count;
    for (context=g_binlog_consumer_array.contexts; context<end; context++) {
        if ((result=common_blocked_queue_init_ex(&context->queue, 10240)) != 0) {
            return result;
        }
    }
    g_binlog_consumer_array.count = count;
    return 0;
}

static int binlog_consumer_start()
{
    int result;
    int i;
    pthread_t tid;
    pthread_attr_t thread_attr;
    void *(*thread_func)(void *args);

    if ((result=init_pthread_attr(&thread_attr, SF_G_THREAD_STACK_SIZE)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "init_pthread_attr fail, program exit!", __LINE__);
        return result;
    }

    for (i=0; i<CLUSTER_SERVER_ARRAY.count; i++) {
        if (CLUSTER_SERVER_ARRAY.servers + i == CLUSTER_MYSELF_PTR) {
            thread_func = binlog_write_thread_func;
        } else {
            thread_func = binlog_sync_thread_func;
        }
        g_binlog_consumer_array.contexts[i].server =
            CLUSTER_SERVER_ARRAY.servers + i;
        if ((result=pthread_create(&tid, &thread_attr, thread_func,
                        g_binlog_consumer_array.contexts + i)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "create thread failed, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    pthread_attr_destroy(&thread_attr);
    return 0;
}

int binlog_consumer_init()
{
    int result;

    if ((result=init_binlog_consumer_array()) != 0) {
        return result;
    }

    if ((result=binlog_write_thread_init()) != 0) {
        return result;
    }

    return binlog_consumer_start();
}

void binlog_consumer_destroy()
{
    if (g_binlog_consumer_array.contexts != NULL) {
        ServerBinlogConsumerContext *context;
        ServerBinlogConsumerContext *end;

        end = g_binlog_consumer_array.contexts + g_binlog_consumer_array.count;
        for (context=g_binlog_consumer_array.contexts; context<end; context++) {
            common_blocked_queue_destroy(&context->queue);
        }
        free(g_binlog_consumer_array.contexts);
        g_binlog_consumer_array.contexts = NULL;
    }
}

void binlog_consumer_terminate()
{
    ServerBinlogConsumerContext *context;
    ServerBinlogConsumerContext *end;

    end = g_binlog_consumer_array.contexts + g_binlog_consumer_array.count;
    for (context=g_binlog_consumer_array.contexts; context<end; context++) {
        common_blocked_queue_terminate(&context->queue);
    }
}

int binlog_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer)
{
    ServerBinlogConsumerContext *context;
    ServerBinlogConsumerContext *end;

    __sync_add_and_fetch(&rbuffer->reffer_count, g_binlog_consumer_array.count);
    end = g_binlog_consumer_array.contexts + g_binlog_consumer_array.count;
    for (context=g_binlog_consumer_array.contexts; context<end; context++) {
        common_blocked_queue_push(&context->queue, rbuffer);
    }
    return 0;
}

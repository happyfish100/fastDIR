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
#include "binlog_consumer.h"

ServerBinlogConsumerArray g_binlog_consumer_array;

static int init_binlog_consumer_array()
{
    int result;
    int count;
    int bytes;
    ServerBinlogConsumerContext *context;
    ServerBinlogConsumerContext *end;

    count = FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX);
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

int binlog_consumer_init()
{
    int result;

    if ((result=init_binlog_consumer_array()) != 0) {
        return result;
    }

	return 0;
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

int binlog_consumer_push_to_queues(ServerBinlogRecordBuffer *record)
{
    ServerBinlogConsumerContext *context;
    ServerBinlogConsumerContext *end;
    __sync_add_and_fetch(&record->reffer_count, g_binlog_consumer_array.count);

    end = g_binlog_consumer_array.contexts + g_binlog_consumer_array.count;
    for (context=g_binlog_consumer_array.contexts; context<end; context++) {
        common_blocked_queue_push(&context->queue, record);
    }
    return 0;
}

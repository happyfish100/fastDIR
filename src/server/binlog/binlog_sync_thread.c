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
#include "binlog_func.h"
#include "binlog_producer.h"
#include "binlog_sync_thread.h"

typedef struct {
    ServerBinlogBuffer binlog_buffer;
    FCServerInfo *peer_server;
} BinlogSyncContext;

static int binlog_sync_to_server(BinlogSyncContext *sync_context)
{
    if (sync_context->binlog_buffer.length == 0) {
        return 0;
    }

    //TODO
    return 0;
}

static inline int deal_binlog_one_record(BinlogSyncContext *sync_context,
        ServerBinlogRecordBuffer *rb)
{
    int result;
    if (sync_context->binlog_buffer.size - sync_context->binlog_buffer.length
            < rb->record.length)
    {
        if ((result=binlog_sync_to_server(sync_context)) != 0) {
            return result;
        }
    }

    memcpy(sync_context->binlog_buffer.buffer +
            sync_context->binlog_buffer.length,
            rb->record.data, rb->record.length);
    sync_context->binlog_buffer.length += rb->record.length;
    return 0;
}

static int deal_binlog_records(BinlogSyncContext *sync_context,
        struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rb;
    int result;

    do {
        rb = (ServerBinlogRecordBuffer *)node->data;
        if ((result=deal_binlog_one_record(sync_context, rb)) != 0) {
            return result;
        }

        server_binlog_release_rbuffer(rb);
        node = node->next;
    } while (node != NULL);

    return binlog_sync_to_server(sync_context);
}

void *binlog_sync_thread_func(void *arg)
{
    struct common_blocked_queue *queue;
    struct common_blocked_node *node;
    BinlogSyncContext sync_context;

    if (binlog_buffer_init(&sync_context.binlog_buffer) != 0) {
        logCrit("file: "__FILE__", line: %d, "
                "binlog_buffer_init fail, program exit!", __LINE__);

        SF_G_CONTINUE_FLAG = false;
        return NULL;
    }

    sync_context.peer_server = ((ServerBinlogConsumerContext *)arg)->server;
    queue = &((ServerBinlogConsumerContext *)arg)->queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(queue);
        if (node == NULL) {
            continue;
        }

        deal_binlog_records(&sync_context, node);
        common_blocked_queue_free_all_nodes(queue, node);
    }

    return NULL;
}

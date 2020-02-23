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
#include "../server_global.h"
#include "binlog_producer.h"

static struct fast_mblock_man record_buffer_allocator;
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

int record_buffer_alloc_init_func(void *element, void *args)
{
    FastBuffer *buffer;
    int min_bytes;
    int init_capacity;

    buffer = &((ServerBinlogRecordBuffer *)element)->record;

    min_bytes = NAME_MAX + PATH_MAX + 128;
    init_capacity = 512;
    while (init_capacity < min_bytes) {
        init_capacity *= 2;
    }

    logInfo("file: "__FILE__", line: %d, "
            "init_capacity: %d", __LINE__, init_capacity);
    return fast_buffer_init_ex(buffer, init_capacity);
}

int binlog_producer_init()
{
    int result;

    if ((result=fast_mblock_init_ex(&record_buffer_allocator,
                    sizeof(ServerBinlogRecordBuffer),
                    1024, record_buffer_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=init_binlog_consumer_array()) != 0) {
        return result;
    }

	return 0;
}

void binlog_producer_destroy()
{
    fast_mblock_destroy(&record_buffer_allocator);
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

ServerBinlogRecordBuffer *server_binlog_alloc_rbuffer()
{
    ServerBinlogRecordBuffer *record;

    record = (ServerBinlogRecordBuffer *)fast_mblock_alloc_object(
            &record_buffer_allocator);
    if (record == NULL) {
        return NULL;
    }

    record->data_version = __sync_add_and_fetch(&DATA_VERSION, 1);
    return record;
}

void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *buffer)
{
    if (__sync_sub_and_fetch(&buffer->reffer_count, 1) == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, buffer);
        fast_mblock_free_object(&record_buffer_allocator, buffer);
    }
}

int server_binlog_write(ServerBinlogRecordBuffer *record)
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

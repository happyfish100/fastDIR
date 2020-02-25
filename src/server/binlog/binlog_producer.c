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
#include "binlog_producer.h"

#define SLEEP_NANO_SECONDS   (50 * 1000)
#define MAX_SLEEP_COUNT      (20 * 1000)

static struct fast_mblock_man record_buffer_allocator;

static volatile int64_t next_data_version;
static struct timespec sleep_ts;

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

    //TODO: DATA_VERSION must be inited first
    next_data_version = DATA_VERSION + 1;

    sleep_ts.tv_sec = 0;
    sleep_ts.tv_nsec = SLEEP_NANO_SECONDS;
	return 0;
}

void binlog_producer_destroy()
{
    fast_mblock_destroy(&record_buffer_allocator);
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

int server_binlog_produce(ServerBinlogRecordBuffer *record)
{
    int count;
    int result;

    count = 0;
    while ((record->data_version != __sync_fetch_and_add(
                &next_data_version, 0)) && (++count < MAX_SLEEP_COUNT))
    {
        nanosleep(&sleep_ts, NULL);
    }

    if (count >= 1) {
        if (count == MAX_SLEEP_COUNT) {
            logError("file: "__FILE__", line: %d, "
                    "waiting for next data version: %"PRId64" timeout, "
                    "maybe some mistakes happened", __LINE__,
                    record->data_version);
        } else {
            logWarning("file: "__FILE__", line: %d, "
                    "waiting for next data version: %"PRId64" count: %d",
                    __LINE__, record->data_version, count);
        }
    }

    result = binlog_consumer_push_to_queues(record);
    if (count < MAX_SLEEP_COUNT) {  //normal
        __sync_add_and_fetch(&next_data_version, 1);
    } else {  //on exception
        int64_t old_data_version;
        old_data_version = __sync_fetch_and_add(&next_data_version, 0);
        if (old_data_version <= record->data_version) {
            __sync_bool_compare_and_swap(&next_data_version, old_data_version,
                    record->data_version + 1);
        }
    }
    return result;
}

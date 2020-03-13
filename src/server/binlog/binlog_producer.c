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
#include "binlog_reader.h"
#include "binlog_local_consumer.h"
#include "binlog_producer.h"

#define SLEEP_NANO_SECONDS   (50 * 1000)
#define MAX_SLEEP_COUNT      (20 * 1000)

static struct fast_mblock_man record_buffer_allocator;

static volatile int64_t next_data_version = 0;
static struct timespec sleep_ts;

static void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *
        rbuffer, void *args);

int record_buffer_alloc_init_func(void *element, void *args)
{
    FastBuffer *buffer;
    int min_bytes;
    int init_capacity;

    buffer = &((ServerBinlogRecordBuffer *)element)->buffer;

    min_bytes = NAME_MAX + PATH_MAX + 128;
    init_capacity = 512;
    while (init_capacity < min_bytes) {
        init_capacity *= 2;
    }

    ((ServerBinlogRecordBuffer *)element)->release_func =
        server_binlog_release_rbuffer;
    return fast_buffer_init_ex(buffer, init_capacity);
}

int binlog_producer_init()
{
    int result;
    int element_size;

    element_size = sizeof(ServerBinlogRecordBuffer) +
        sizeof(struct server_binlog_record_buffer *) *
        CLUSTER_SERVER_ARRAY.count;
    if ((result=fast_mblock_init_ex(&record_buffer_allocator, element_size,
                    1024, record_buffer_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=binlog_get_max_record_version((int64_t *)
                    &DATA_CURRENT_VERSION)) != 0)
    {
        return result;
    }

    logInfo("DATA_CURRENT_VERSION == %"PRId64, DATA_CURRENT_VERSION);

    next_data_version = DATA_CURRENT_VERSION + 1;
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
    ServerBinlogRecordBuffer *rbuffer;

    rbuffer = (ServerBinlogRecordBuffer *)fast_mblock_alloc_object(
            &record_buffer_allocator);
    if (rbuffer == NULL) {
        return NULL;
    }

    return rbuffer;
}

static void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *
        rbuffer, void *args)
{
    if (__sync_sub_and_fetch(&rbuffer->reffer_count, 1) == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rbuffer);
        fast_mblock_free_object(&record_buffer_allocator, rbuffer);
    }
}

int server_binlog_dispatch(ServerBinlogRecordBuffer *rbuffer)
{
    int count;
    int result;
    int64_t current_version;

    count = 0;
    while ((rbuffer->data_version != (current_version=__sync_fetch_and_add(
                &next_data_version, 0))) && (++count < MAX_SLEEP_COUNT))
    {
        nanosleep(&sleep_ts, NULL);
    }

    if (count >= 1) {
        if (count == MAX_SLEEP_COUNT) {
            logError("file: "__FILE__", line: %d, "
                    "waiting for my turn timeout, my data version: %"PRId64", "
                    "maybe some mistakes happened, next data version: %"PRId64,
                    __LINE__, rbuffer->data_version, current_version);
        } else {
            logWarning("file: "__FILE__", line: %d, "
                    "waiting for my turn count: %d, my data version: %"PRId64,
                    __LINE__, count, rbuffer->data_version);
        }
    }

    result = binlog_local_consumer_push_to_queues(rbuffer);
    if (count < MAX_SLEEP_COUNT) {  //normal
        __sync_add_and_fetch(&next_data_version, 1);
    } else {  //on exception
        int64_t old_data_version;
        old_data_version = __sync_fetch_and_add(&next_data_version, 0);
        if (old_data_version <= rbuffer->data_version) {
            __sync_bool_compare_and_swap(&next_data_version, old_data_version,
                    rbuffer->data_version + 1);
        }
    }

    /*
    current_version=__sync_fetch_and_add(&next_data_version, 0);
    logInfo("file: "__FILE__", line: %d, "
            "=======my data version: %"PRId64", next: %"PRId64", current: %"PRId64"=====",
            __LINE__, rbuffer->data_version, current_version, DATA_CURRENT_VERSION);
            */
    return result;
}

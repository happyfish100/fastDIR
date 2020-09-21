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

typedef struct producer_record_buffer_queue {
    struct server_binlog_record_buffer *head;
    struct server_binlog_record_buffer *tail;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} ProducerRecordBufferQueue;

typedef struct binlog_producer_context {
    struct {
        ServerBinlogRecordBuffer **entries;
        ServerBinlogRecordBuffer **start; //for consumer
        ServerBinlogRecordBuffer **end;   //for producer
        int count;
        int size;
    } ring;

    ProducerRecordBufferQueue queue;

    struct fast_mblock_man rb_allocator;
} BinlogProducerContext;

static BinlogProducerContext proceduer_ctx = {
    {NULL, NULL, NULL, 0, 0}, {NULL, NULL}
};

static uint64_t next_data_version = 0;
static bool running = false;

static void server_binlog_release_rbuffer(ServerBinlogRecordBuffer * rbuffer);
static void *producer_thread_func(void *arg);

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

static int binlog_producer_init_queue()
{
    int result;

    if ((result=init_pthread_lock(&(proceduer_ctx.queue.lock))) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((result=pthread_cond_init(&(proceduer_ctx.queue.cond), NULL)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "pthread_cond_init fail, "
                "errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    return 0;
}

static int binlog_producer_init_ring()
{
    int bytes;

    proceduer_ctx.ring.size = 4096;
    bytes = sizeof(ServerBinlogRecordBuffer *) * proceduer_ctx.ring.size;
    proceduer_ctx.ring.entries = (ServerBinlogRecordBuffer **)fc_malloc(bytes);
    if (proceduer_ctx.ring.entries == NULL) {
        return ENOMEM;
    }
    memset(proceduer_ctx.ring.entries, 0, bytes);

    proceduer_ctx.ring.start = proceduer_ctx.ring.end =
        proceduer_ctx.ring.entries;
    return 0;
}

int binlog_producer_init()
{
    pthread_t tid;
    int result;
    int element_size;

    element_size = sizeof(ServerBinlogRecordBuffer) +
        sizeof(struct server_binlog_record_buffer *) *
        CLUSTER_SERVER_ARRAY.count;
    if ((result=fast_mblock_init_ex1(&proceduer_ctx.rb_allocator,
                    "record_buffer", element_size, 1024, 0,
                    record_buffer_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=binlog_producer_init_queue()) != 0) {
        return result;
    }

    if ((result=binlog_producer_init_ring()) != 0) {
        return result;
    }

    return fc_create_thread(&tid, producer_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}

void binlog_producer_destroy()
{
    int count;

    pthread_cond_signal(&proceduer_ctx.queue.cond);
    count = 0;
    while (running && count++ < 100) {
        fc_sleep_ms(1);
    }

    pthread_cond_destroy(&proceduer_ctx.queue.cond);
    pthread_mutex_destroy(&proceduer_ctx.queue.lock);
    fast_mblock_destroy(&proceduer_ctx.rb_allocator);

    //TODO  notify task in entryes
    free(proceduer_ctx.ring.entries);
    proceduer_ctx.ring.entries = NULL;

    proceduer_ctx.queue.head = proceduer_ctx.queue.tail = NULL;
}

ServerBinlogRecordBuffer *server_binlog_alloc_rbuffer()
{
    ServerBinlogRecordBuffer *rbuffer;

    rbuffer = (ServerBinlogRecordBuffer *)fast_mblock_alloc_object(
            &proceduer_ctx.rb_allocator);
    if (rbuffer == NULL) {
        return NULL;
    }

    return rbuffer;
}

static void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *rbuffer)
{
    if (__sync_sub_and_fetch(&rbuffer->reffer_count, 1) == 0) {
        /*
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rbuffer);
                */
        fast_mblock_free_object(&proceduer_ctx.rb_allocator, rbuffer);
    }
}

void server_binlog_free_rbuffer(ServerBinlogRecordBuffer *rbuffer)
{
    fast_mblock_free_object(&proceduer_ctx.rb_allocator, rbuffer);
}

void binlog_push_to_producer_queue(ServerBinlogRecordBuffer *rbuffer)
{
    bool notify;

    rbuffer->next = NULL;
    PTHREAD_MUTEX_LOCK(&proceduer_ctx.queue.lock);
    if (proceduer_ctx.queue.tail == NULL) {
        proceduer_ctx.queue.head = rbuffer;
        notify = true;
    } else {
        proceduer_ctx.queue.tail->next = rbuffer;
        notify = false;
    }

    proceduer_ctx.queue.tail = rbuffer;
    PTHREAD_MUTEX_UNLOCK(&proceduer_ctx.queue.lock);

    if (notify) {
        pthread_cond_signal(&proceduer_ctx.queue.cond);
    }
}

static void repush_to_queue(ServerBinlogRecordBuffer *rb)
{
    ServerBinlogRecordBuffer *previous;
    ServerBinlogRecordBuffer *current;

    PTHREAD_MUTEX_LOCK(&proceduer_ctx.queue.lock);
    if (proceduer_ctx.queue.head == NULL) {
        rb->next = NULL;
        proceduer_ctx.queue.head = proceduer_ctx.queue.tail = rb;
    } else if (rb->data_version <= proceduer_ctx.queue.head->data_version) {
        rb->next = proceduer_ctx.queue.head;
        proceduer_ctx.queue.head = rb;
    } else if (rb->data_version > proceduer_ctx.queue.tail->data_version) {
        rb->next = NULL;
        proceduer_ctx.queue.tail->next = rb;
        proceduer_ctx.queue.tail = rb;
    } else {
        previous = proceduer_ctx.queue.head;
        current = proceduer_ctx.queue.head->next;
        while (current != NULL && rb->data_version > current->data_version) {
            previous = current;
            current = current->next;
        }

        rb->next = previous->next;
        previous->next = rb;
    }
    PTHREAD_MUTEX_UNLOCK(&proceduer_ctx.queue.lock);
}

#define PUSH_TO_CONSUMER_QUEQUES(rb) \
    do { \
        binlog_local_consumer_push_to_queues(rb); \
        ++next_data_version;  \
    } while (0)

static void deal_record(ServerBinlogRecordBuffer *rb)
{
    int64_t distance;
    int index;
    bool expand;
    ServerBinlogRecordBuffer **current;

    distance = rb->data_version - next_data_version;
    if (distance >= (proceduer_ctx.ring.size - 1)) {
        logWarning("file: "__FILE__", line: %d, "
                "data_version: %"PRId64", is too large, "
                "exceeds %"PRId64" + %d", __LINE__,
                rb->data_version, next_data_version,
                proceduer_ctx.ring.size - 1);
        repush_to_queue(rb);
        return;
    }

    current = proceduer_ctx.ring.entries + rb->data_version %
        proceduer_ctx.ring.size;
    if (current == proceduer_ctx.ring.start) {
        PUSH_TO_CONSUMER_QUEQUES(rb);

        index = proceduer_ctx.ring.start - proceduer_ctx.ring.entries;
        if (proceduer_ctx.ring.start == proceduer_ctx.ring.end) {
            proceduer_ctx.ring.start = proceduer_ctx.ring.end =
                proceduer_ctx.ring.entries +
                (++index) % proceduer_ctx.ring.size;
            return;
        }

        proceduer_ctx.ring.start = proceduer_ctx.ring.entries +
            (++index) % proceduer_ctx.ring.size;
        while (proceduer_ctx.ring.start != proceduer_ctx.ring.end &&
                *(proceduer_ctx.ring.start) != NULL)
        {
            PUSH_TO_CONSUMER_QUEQUES(*(proceduer_ctx.ring.start));
            *(proceduer_ctx.ring.start) = NULL;

            proceduer_ctx.ring.start = proceduer_ctx.ring.entries +
                (++index) % proceduer_ctx.ring.size;
            proceduer_ctx.ring.count--;
        }
        return;
    }

    *current = rb;
    proceduer_ctx.ring.count++;
    if (proceduer_ctx.ring.start == proceduer_ctx.ring.end) { //empty
        expand = true;
    } else if (proceduer_ctx.ring.end > proceduer_ctx.ring.start) {
        expand = !(current > proceduer_ctx.ring.start &&
                current < proceduer_ctx.ring.end);
    } else {
        expand = (current >= proceduer_ctx.ring.end &&
                current < proceduer_ctx.ring.start);
    }

    if (expand) {
        proceduer_ctx.ring.end = proceduer_ctx.ring.entries +
            (rb->data_version + 1) % proceduer_ctx.ring.size;
    }
}

static void deal_queue()
{
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *head;
    static int max_ring_count = 0;

    if (proceduer_ctx.ring.count > max_ring_count) {
        max_ring_count = proceduer_ctx.ring.count;
        //logInfo("max proceduer_ctx.ring.count ==== %d", proceduer_ctx.ring.count);
    }

    PTHREAD_MUTEX_LOCK(&proceduer_ctx.queue.lock);
    if (proceduer_ctx.queue.head == NULL) {
        pthread_cond_wait(&proceduer_ctx.queue.cond,
                &proceduer_ctx.queue.lock);
    }

    head = proceduer_ctx.queue.head;
    proceduer_ctx.queue.head = proceduer_ctx.queue.tail = NULL;
    PTHREAD_MUTEX_UNLOCK(&proceduer_ctx.queue.lock);

    if (head == NULL) {
        return;
    }

    while (head != NULL) {
        rb = head;
        head = head->next;

        deal_record(rb);
    }
}

static void *producer_thread_func(void *arg)
{
    logInfo("file: "__FILE__", line: %d, "
            "producer_thread_func start", __LINE__);

    running = true;

    next_data_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 0) + 1;
    proceduer_ctx.ring.start = proceduer_ctx.ring.end =
        proceduer_ctx.ring.entries + next_data_version %
        proceduer_ctx.ring.size;

    while (SF_G_CONTINUE_FLAG && CLUSTER_MYSELF_PTR == CLUSTER_MASTER_PTR) {
        deal_queue();
    }
    running = false;

    logInfo("file: "__FILE__", line: %d, "
            "producer_thread_func exit", __LINE__);
    return NULL;
}

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
    int rb_init_capacity;
} BinlogProducerContext;

static BinlogProducerContext proceduer_ctx = {
    {NULL, NULL, NULL, 0, 0}, {NULL, NULL}
};

static uint64_t next_data_version = 0;
static volatile char running = 0;

static void *producer_thread_func(void *arg);

int record_buffer_alloc_init_func(void *element, void *args)
{
    FastBuffer *buffer;

    buffer = &((ServerBinlogRecordBuffer *)element)->buffer;
    ((ServerBinlogRecordBuffer *)element)->release_func =
        server_binlog_release_rbuffer;
    return fast_buffer_init_ex(buffer, proceduer_ctx.rb_init_capacity);
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
    int result;
    int element_size;

    proceduer_ctx.rb_init_capacity = 4 * 1024;
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

    return 0;
}

int binlog_producer_start()
{
    pthread_t tid;
    return fc_create_thread(&tid, producer_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}

void binlog_producer_destroy()
{
    int count;

    pthread_cond_signal(&proceduer_ctx.queue.cond);
    count = 0;
    while (FC_ATOMIC_GET(running) && count++ < 300) {
        if (count % 10 == 0) {
            pthread_cond_signal(&proceduer_ctx.queue.cond);
        }
        fc_sleep_ms(1);
    }

    pthread_cond_destroy(&proceduer_ctx.queue.cond);
    pthread_mutex_destroy(&proceduer_ctx.queue.lock);
    fast_mblock_destroy(&proceduer_ctx.rb_allocator);

    //TODO  notify task in entries
    free(proceduer_ctx.ring.entries);
    proceduer_ctx.ring.entries = NULL;

    proceduer_ctx.queue.head = proceduer_ctx.queue.tail = NULL;
}

ServerBinlogRecordBuffer *server_binlog_alloc_hold_rbuffer()
{
    ServerBinlogRecordBuffer *rbuffer;
    int old_value;

    rbuffer = (ServerBinlogRecordBuffer *)fast_mblock_alloc_object(
            &proceduer_ctx.rb_allocator);
    if (rbuffer == NULL) {
        return NULL;
    }

    old_value = 0;  //for hint
    FC_ATOMIC_CAS(rbuffer->reffer_count, old_value, 1);
    rbuffer->buffer.length = 0;
    return rbuffer;
}

void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *rbuffer)
{
    if (__sync_sub_and_fetch(&rbuffer->reffer_count, 1) == 0) {
        /*
           logInfo("file: "__FILE__", line: %d, "
           "free record buffer: %p", __LINE__, rbuffer);
         */
        fast_buffer_reset(&rbuffer->buffer);
        if (rbuffer->buffer.alloc_size >= 2 * proceduer_ctx.rb_init_capacity) {
            fast_buffer_set_capacity(&rbuffer->buffer,
                    proceduer_ctx.rb_init_capacity);
        }
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
    } else if (rb->data_version.first <= proceduer_ctx.
            queue.head->data_version.first)
    {
        rb->next = proceduer_ctx.queue.head;
        proceduer_ctx.queue.head = rb;
    } else if (rb->data_version.first > proceduer_ctx.
            queue.tail->data_version.last)
    {
        rb->next = NULL;
        proceduer_ctx.queue.tail->next = rb;
        proceduer_ctx.queue.tail = rb;
    } else {
        previous = proceduer_ctx.queue.head;
        current = proceduer_ctx.queue.head->next;
        while (current != NULL && rb->data_version.first >
                current->data_version.last)
        {
            previous = current;
            current = current->next;
        }

        rb->next = previous->next;
        previous->next = rb;
    }
    PTHREAD_MUTEX_UNLOCK(&proceduer_ctx.queue.lock);
}

#define PUSH_TO_CONSUMER_QUEQUES(rb, version_count) \
    do { \
        binlog_local_consumer_push_to_queues(rb); \
        next_data_version += version_count;       \
    } while (0)

#define GET_RBUFFER_VERSION_COUNT(rb)  \
        (((rb)->data_version.last - (rb)->data_version.first) + 1)

static void deal_record(ServerBinlogRecordBuffer *rb)
{
    int64_t distance;
    int version_count;
    bool expand;
    ServerBinlogRecordBuffer **current;

    distance = (int64_t)rb->data_version.first - (int64_t)next_data_version;
    if (distance >= (proceduer_ctx.ring.size - 1)) {
        logWarning("file: "__FILE__", line: %d, "
                "data_version: %"PRId64" is too large, "
                "exceeds %"PRId64" + %d", __LINE__,
                rb->data_version.first, next_data_version,
                proceduer_ctx.ring.size - 1);
        repush_to_queue(rb);
        return;
    } else if (distance < 0) {
        logError("file: "__FILE__", line: %d, "
                "data_version: %"PRId64" is too small, "
                "less than %"PRId64, __LINE__,
                rb->data_version.first, next_data_version);
        return;
    }

    current = proceduer_ctx.ring.entries + rb->data_version.first %
        proceduer_ctx.ring.size;
    if (current == proceduer_ctx.ring.start) {
        version_count = GET_RBUFFER_VERSION_COUNT(rb);
        PUSH_TO_CONSUMER_QUEQUES(rb, version_count);

        if (proceduer_ctx.ring.start == proceduer_ctx.ring.end) {
            proceduer_ctx.ring.start = proceduer_ctx.ring.end =
                proceduer_ctx.ring.entries + next_data_version %
                proceduer_ctx.ring.size;
            return;
        }

        proceduer_ctx.ring.start = proceduer_ctx.ring.entries +
            next_data_version % proceduer_ctx.ring.size;
        while (proceduer_ctx.ring.start != proceduer_ctx.ring.end &&
                *(proceduer_ctx.ring.start) != NULL)
        {
            current = proceduer_ctx.ring.start;
            version_count = GET_RBUFFER_VERSION_COUNT(*current);
            PUSH_TO_CONSUMER_QUEQUES(*current, version_count);
            *current = NULL;

            proceduer_ctx.ring.start = proceduer_ctx.ring.entries +
                next_data_version % proceduer_ctx.ring.size;
            proceduer_ctx.ring.count--;
        }
        return;
    }

    distance = (int64_t)rb->data_version.last - (int64_t)next_data_version;
    if (distance >= (proceduer_ctx.ring.size - 1)) {
        logWarning("file: "__FILE__", line: %d, "
                "data_version: %"PRId64", is too large, "
                "exceeds %"PRId64" + %d", __LINE__,
                rb->data_version.last, next_data_version,
                proceduer_ctx.ring.size - 1);
        repush_to_queue(rb);
        return;
    }

    *current = rb;
    proceduer_ctx.ring.count++;
    if (proceduer_ctx.ring.start == proceduer_ctx.ring.end) { //empty
        expand = true;
    } else if (proceduer_ctx.ring.end > proceduer_ctx.ring.start) {
        ServerBinlogRecordBuffer **last;
        last = proceduer_ctx.ring.entries + rb->data_version.last %
            proceduer_ctx.ring.size;
        expand = !(current > proceduer_ctx.ring.start &&
                last < proceduer_ctx.ring.end);
    } else {
        expand = (current >= proceduer_ctx.ring.end &&
                current < proceduer_ctx.ring.start);
    }

    if (expand) {
        proceduer_ctx.ring.end = proceduer_ctx.ring.entries +
            (rb->data_version.last + 1) % proceduer_ctx.ring.size;
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
    logDebug("file: "__FILE__", line: %d, "
            "producer_thread_func start", __LINE__);

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "replica-producer");
#endif

    FC_ATOMIC_SET(running, 1);

    next_data_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 0) + 1;
    proceduer_ctx.ring.start = proceduer_ctx.ring.end =
        proceduer_ctx.ring.entries + next_data_version %
        proceduer_ctx.ring.size;

    while (SF_G_CONTINUE_FLAG && CLUSTER_MYSELF_PTR == CLUSTER_MASTER_PTR) {
        deal_queue();
    }
    FC_ATOMIC_SET(running, 0);

    logDebug("file: "__FILE__", line: %d, "
            "producer_thread_func exit", __LINE__);
    return NULL;
}

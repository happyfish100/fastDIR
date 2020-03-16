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
#include "fastcommon/shared_func.h"
#include "sf/sf_nio.h"
#include "push_result_ring.h"

int push_result_ring_check_init(FDIRBinlogPushResultContext *ctx,
        const int alloc_size)
{
    int bytes;

    if (ctx->ring.entries != NULL) {
        return 0;
    }

    bytes = sizeof(FDIRBinlogPushResultEntry) * alloc_size;
    ctx->ring.entries = (FDIRBinlogPushResultEntry *)malloc(bytes);
    if (ctx->ring.entries == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(ctx->ring.entries, 0, bytes);

    ctx->ring.start = ctx->ring.end = ctx->ring.entries;
    ctx->ring.size = alloc_size;

    ctx->queue.head = ctx->queue.tail = NULL;
    return fast_mblock_init_ex(&ctx->queue.rentry_allocator,
        sizeof(FDIRBinlogPushResultEntry), 4096, NULL, NULL, false);
}

#define DESC_TASK_WAITING_RPC_COUNT(task) \
    do { \
        if (task != NULL) { \
            if (__sync_sub_and_fetch(&((FDIRServerTaskArg *) \
                            task->arg)->context.service.     \
                        waiting_rpc_count, 1) == 0) \
            { \
                sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);  \
            } \
        }  \
    } while (0)

void push_result_ring_clear(FDIRBinlogPushResultContext *ctx)
{
    int index;

    if (ctx->ring.start == ctx->ring.end) {
        return;
    }

    index = ctx->ring.start - ctx->ring.entries;
    while (ctx->ring.start != ctx->ring.end) {
        DESC_TASK_WAITING_RPC_COUNT(ctx->ring.start->waiting_task);

        ctx->ring.start = ctx->ring.entries +
            (++index % ctx->ring.size);
    }
}

void push_result_ring_destroy(FDIRBinlogPushResultContext *ctx)
{
    if (ctx->ring.entries != NULL) {
        free(ctx->ring.entries);
        ctx->ring.start = ctx->ring.end = ctx->ring.entries = NULL;
        ctx->ring.size = 0;
    }

    fast_mblock_destroy(&ctx->queue.rentry_allocator);
}

static int add_to_queue(FDIRBinlogPushResultContext *ctx,
            const uint64_t data_version, struct fast_task_info *waiting_task)
{
    FDIRBinlogPushResultEntry *entry;
    FDIRBinlogPushResultEntry *previous;
    FDIRBinlogPushResultEntry *current;

    entry = (FDIRBinlogPushResultEntry *)fast_mblock_alloc_object(
            &ctx->queue.rentry_allocator);
    if (entry == NULL) {
        return ENOMEM;
    }

    entry->data_version = data_version;
    entry->waiting_task = waiting_task;

    if (ctx->queue.tail == NULL) {  //empty queue
        entry->next = NULL;
        ctx->queue.head = ctx->queue.tail = entry;
        return 0;
    }

    if (data_version > ctx->queue.tail->data_version) {
        entry->next = NULL;
        ctx->queue.tail->next = entry;
        ctx->queue.tail = entry;
        return 0;
    }

    if (data_version < ctx->queue.head->data_version) {
        entry->next = ctx->queue.head;
        ctx->queue.head = entry;
        return 0;
    }

    previous = ctx->queue.head;
    current = ctx->queue.head->next;
    while (current != NULL && data_version > current->data_version) {
        previous = current;
        current = current->next;
    }

    entry->next = previous->next;
    previous->next = entry;
    return 0;
}

int push_result_ring_add(FDIRBinlogPushResultContext *ctx,
        const uint64_t data_version, struct fast_task_info *waiting_task)
{
    FDIRBinlogPushResultEntry *entry;
    FDIRBinlogPushResultEntry *previous;
    FDIRBinlogPushResultEntry *next;
    int index;
    bool matched;

    matched = false;
    index = data_version % ctx->ring.size;
    entry = ctx->ring.entries + index;
    if (ctx->ring.end == ctx->ring.start) {  //empty
        ctx->ring.start = entry;
        ctx->ring.end = ctx->ring.entries + (index + 1) % ctx->ring.size;
        matched = true;
    } else if (entry == ctx->ring.end) {
        previous = ctx->ring.entries + (index + ctx->ring.size - 1) %
            ctx->ring.size;
        next = ctx->ring.entries + (index + 1) % ctx->ring.size;
        if ((next != ctx->ring.start) &&
                data_version == previous->data_version + 1)
        {
            ctx->ring.end = next;
            matched = true;
        }
    }

    if (matched) {
        entry->data_version = data_version;
        entry->waiting_task = waiting_task;
        return 0;
    }

    return add_to_queue(ctx, data_version, waiting_task);
}

static int remove_from_queue(FDIRBinlogPushResultContext *ctx,
        const uint64_t data_version)
{
    FDIRBinlogPushResultEntry *entry;
    FDIRBinlogPushResultEntry *previous;
    FDIRBinlogPushResultEntry *current;

    if (ctx->queue.head == NULL) {  //empty queue
        return ENOENT;
    }

    if (data_version == ctx->queue.head->data_version) {
        entry = ctx->queue.head;
        ctx->queue.head = entry->next;
        if (ctx->queue.head == NULL) {
            ctx->queue.tail = NULL;
        }
    } else {
        previous = ctx->queue.head;
        current = ctx->queue.head->next;
        while (current != NULL && data_version > current->data_version) {
            previous = current;
            current = current->next;
        }

        if (current == NULL || data_version != current->data_version) {
            return ENOENT;
        }

        entry = current;
        previous->next = current->next;
        if (ctx->queue.tail == current) {
            ctx->queue.tail = previous;
        }
    }

    DESC_TASK_WAITING_RPC_COUNT(entry->waiting_task);
    fast_mblock_free_object(&ctx->queue.rentry_allocator, entry);
    return 0;
}

int push_result_ring_remove(FDIRBinlogPushResultContext *ctx,
        const uint64_t data_version)
{
    FDIRBinlogPushResultEntry *entry;
    int index;

    if (ctx->ring.end != ctx->ring.start) {
        index = data_version % ctx->ring.size;
        entry = ctx->ring.entries + index;

        if (entry->data_version == data_version) {
            if (ctx->ring.start == entry) {
                ctx->ring.start = ctx->ring.entries +
                    (++index % ctx->ring.size);
                while (ctx->ring.start != ctx->ring.end &&
                        ctx->ring.start->data_version == 0)
                {
                    ctx->ring.start = ctx->ring.entries +
                        (++index % ctx->ring.size);
                }
            }

            DESC_TASK_WAITING_RPC_COUNT(entry->waiting_task);
            entry->data_version = 0;
            entry->waiting_task = NULL;
            return 0;
        }
    }

    return remove_from_queue(ctx, data_version);
}

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

#include <limits.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "server_global.h"
#include "ns_manager.h"
#include "ns_subscribe.h"

typedef struct fdir_ns_subscribers_allocator {
    FDIRNSSubscriber *objects;
    FDIRNSSubscriber *freelist;
} FDIRNSSubscribersAllocator;

typedef struct fdir_ns_subscribe_context {
    FDIRNSSubscribersAllocator allocator;
    struct {
        struct fc_list_head head;   //element: FDIRNSSubscriber
    } subscribers;
    pthread_mutex_t lock;
} FDIRNSSubscribeContext;

static FDIRNSSubscribeContext subscribe_ctx;

static int init_subscriber_freelist(FDIRNSSubscribersAllocator *allocator)
{
    int result;
    int bytes;
    int i;
    int next_ptr_offsets[2];
    FDIRNSSubscribeEntry dummy;
    FDIRNSSubscriber *previous;
    FDIRNSSubscriber *subs;
    FDIRNSSubscriber *end;

    bytes = sizeof(FDIRNSSubscriber) * FDIR_MAX_NS_SUBSCRIBERS;
    allocator->objects = (FDIRNSSubscriber *)fc_malloc(bytes);
    if (allocator->objects == NULL) {
        return ENOMEM;
    }

    for (i=0; i<2; i++) {
        next_ptr_offsets[i] = (char *)&dummy.
            entries[i].next - (char *)&dummy;
    }

    memset(allocator->objects, 0, bytes);
    end = allocator->objects + FDIR_MAX_NS_SUBSCRIBERS;
    for (subs=allocator->objects; subs<end; subs++) {
        subs->index = subs - allocator->objects;
        for (i=0; i<2; i++) {
            if ((result=fc_queue_init(subs->queues + i,
                            next_ptr_offsets[i])) != 0)
            {
                return result;
            }
        }
    }

    subscribe_ctx.allocator.freelist = allocator->objects;
    previous = allocator->objects;
    for (subs=allocator->objects + 1; subs<end; subs++) {
        previous->next = subs;
        previous = subs;
    }
    previous->next = NULL;

    return 0;
}

int ns_subscribe_init()
{
    int result;

    if ((result=init_subscriber_freelist(&subscribe_ctx.allocator)) != 0) {
        return result;
    }

    if ((result=init_pthread_lock(&subscribe_ctx.lock)) != 0) {
        return result;
    }

    FC_INIT_LIST_HEAD(&subscribe_ctx.subscribers.head);
    return 0;
}

void ns_subscribe_destroy()
{
}

FDIRNSSubscriber *ns_subscribe_register()
{
    FDIRNSSubscriber *subscriber;

    PTHREAD_MUTEX_LOCK(&subscribe_ctx.lock);
    if (subscribe_ctx.allocator.freelist != NULL) {
        subscriber = subscribe_ctx.allocator.freelist;
        subscribe_ctx.allocator.freelist = subscriber->next;

        fc_list_add_tail(&subscriber->dlink,
                &subscribe_ctx.subscribers.head);
    } else {
        subscriber = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&subscribe_ctx.lock);

    return subscriber;
}

void ns_subscribe_unregister(FDIRNSSubscriber *subscriber)
{
    int i;
    FDIRNSSubscribeEntry *entry;

    PTHREAD_MUTEX_LOCK(&subscribe_ctx.lock);
    fc_list_del_init(&subscriber->dlink);
    PTHREAD_MUTEX_UNLOCK(&subscribe_ctx.lock);

    for (i=0; i<2; i++) {
        entry = (FDIRNSSubscribeEntry *)fc_queue_try_pop_all(
                subscriber->queues + i);
        while (entry != NULL) {
            __sync_bool_compare_and_swap(&entry->
                    entries[i].in_queue, 1, 0);
            entry = entry->entries[i].next;
        }
    }

    PTHREAD_MUTEX_LOCK(&subscribe_ctx.lock);
    subscriber->next = subscribe_ctx.allocator.freelist;
    subscribe_ctx.allocator.freelist = subscriber;
    PTHREAD_MUTEX_UNLOCK(&subscribe_ctx.lock);
}

void ns_subscribe_notify_all(struct fdir_namespace_entry *ns_entry)
{
    FDIRNSSubscriber *subscriber;
    FDIRNSSubscribeEntry *entry;

    PTHREAD_MUTEX_LOCK(&subscribe_ctx.lock);
    fc_list_for_each_entry(subscriber, &subscribe_ctx.
            subscribers.head, dlink)
    {
        entry = ns_entry->subs_entries + subscriber->index;
        if (__sync_bool_compare_and_swap(&entry->entries[
                    FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING].
                    in_queue, 0, 1))
        {
            fc_queue_push_silence(subscriber->queues +
                    FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING, entry);
        }
    }
    PTHREAD_MUTEX_UNLOCK(&subscribe_ctx.lock);
}

void ns_subscribe_holding_to_sending_queue(FDIRNSSubscriber *subscriber)
{
    FDIRNSSubscribeEntry *entry;
    FDIRNSSubscribeEntry *current;
    FDIRNSSubscribeEntry *head;
    FDIRNSSubscribeEntry *tail;

    head = tail = NULL;
    entry = (FDIRNSSubscribeEntry *)fc_queue_try_pop_all(subscriber->
            queues + FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING);
    while (entry != NULL) {
        current = entry;
        entry = entry->entries[FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING].next;

        if (__sync_bool_compare_and_swap(&current->entries[
                    FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING].
                    in_queue, 0, 1))
        {
            if (head == NULL) {
                head = current;
            } else {
                tail->entries[FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING].
                    next = current;
            }
            tail = current;
        }

        __sync_bool_compare_and_swap(&current->entries[
                FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING].
                in_queue, 1, 0);
    }

    if (head != NULL) {
        struct fc_queue_info qinfo;

        tail->entries[FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING].next = NULL;
        qinfo.head = head;
        qinfo.tail = tail;
        fc_queue_push_queue_to_tail_silence(subscriber->queues +
                FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING, &qinfo);
    }
}

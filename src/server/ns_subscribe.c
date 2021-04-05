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
    int bytes;
    FDIRNSSubscriber *previous;
    FDIRNSSubscriber *subs;
    FDIRNSSubscriber *end;

    bytes = sizeof(FDIRNSSubscriber) * FDIR_MAX_NS_SUBSCRIBERS;
    allocator->objects = (FDIRNSSubscriber *)fc_malloc(bytes);
    if (allocator->objects == NULL) {
        return ENOMEM;
    }

    memset(allocator->objects, 0, bytes);
    end = allocator->objects + FDIR_MAX_NS_SUBSCRIBERS;
    for (subs=allocator->objects; subs<end; subs++) {
        subs->index = subs - allocator->objects;
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
    PTHREAD_MUTEX_LOCK(&subscribe_ctx.lock);
    fc_list_del_init(&subscriber->dlink);

    subscriber->next = subscribe_ctx.allocator.freelist;
    subscribe_ctx.allocator.freelist = subscriber;
    PTHREAD_MUTEX_UNLOCK(&subscribe_ctx.lock);
}

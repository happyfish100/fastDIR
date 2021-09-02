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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sorted_queue.h"
#include "server_global.h"
#include "change_notify.h"

typedef struct fdir_change_notify_context {
    struct sorted_queue queue;
} FDIRchangeNotifyContext;

static FDIRchangeNotifyContext change_notify_ctx;

static void *change_notify_func(void *arg)
{

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "chg-notify");
#endif

    while (SF_G_CONTINUE_FLAG) {
        //TODO
    }

    return NULL;
}

static int notify_event_compare(const FDIRChangeNotifyEvent *event1,
        const FDIRChangeNotifyEvent *event2)
{
    return fc_compare_int64(event1->version, event2->version);
}

int change_notify_init()
{
    int result;
    pthread_t tid;

    if ((result=sorted_queue_init(&change_notify_ctx.queue, (long)
                    (&((FDIRChangeNotifyEvent *)NULL)->next),
                    (int (*)(const void *, const void *))
                    notify_event_compare)) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, change_notify_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void change_notify_destroy()
{
}

void change_notify_push_to_queue(FDIRChangeNotifyEvent *event)
{
    sorted_queue_push(&change_notify_ctx.queue, event);
}

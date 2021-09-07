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
#include "../data_thread.h"
#include "../binlog/binlog_write.h"
#include "db_updater.h"
#include "change_notify.h"

typedef struct fdir_change_notify_context {
    struct sorted_queue queue;
} FDIRchangeNotifyContext;

static FDIRchangeNotifyContext change_notify_ctx;

static void deal_events(FDIRChangeNotifyEvent *head)
{
    FDIRChangeNotifyEvent *event;

    do {
        event = head;
        head = head->next;

        db_updater_push_to_queue(event);
        fast_mblock_free_object(&event->marray.messages[0].
                dentry->context->event_allocator, event);
    } while (head != NULL);
}

static void *change_notify_func(void *arg)
{
    struct {
        int64_t data_thread;
        int64_t binlog_writer;
    } last_data_versions;
    FDIRChangeNotifyEvent less_than;
    FDIRChangeNotifyEvent *head;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "chg-notify");
#endif

    while (SF_G_CONTINUE_FLAG) {
        last_data_versions.data_thread = data_thread_get_last_data_version();
        last_data_versions.binlog_writer = binlog_writer_get_last_version();
        less_than.version = FC_MIN(last_data_versions.data_thread,
                last_data_versions.binlog_writer);
        if ((head=sorted_queue_pop_all(&change_notify_ctx.
                        queue, &less_than)) != NULL)
        {
            logInfo("file: "__FILE__", line: %d, "
                    "last_data_versions: {data_thread: %"PRId64", "
                    "binlog_writer: %"PRId64"}", __LINE__,
                    last_data_versions.data_thread,
                    last_data_versions.binlog_writer);
            deal_events(head);
        }

        fc_sleep_ms(10);
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

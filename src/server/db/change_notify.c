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
#include "sf/sf_func.h"
#include "../data_thread.h"
#include "../binlog/binlog_write.h"
#include "db_updater.h"
#include "change_notify.h"

typedef struct fdir_change_notify_context {
    volatile int waiting_count;
    struct sorted_queue queue;
} FDIRchangeNotifyContext;

static FDIRchangeNotifyContext change_notify_ctx;

static int deal_events(struct fc_queue_info *qinfo)
{
    int result;
    int count;

    if ((result=db_updater_deal_events(qinfo->head, &count)) != 0) {
        return result;
    }

    sorted_queue_free_chain(&change_notify_ctx.queue,
            &NOTIFY_EVENT_ALLOCATOR, qinfo);
    __sync_sub_and_fetch(&change_notify_ctx.
            waiting_count, count);
    return 0;
}

static void *change_notify_func(void *arg)
{
    struct {
        int64_t data_thread;
        int64_t binlog_writer;
    } last_data_versions;
    FDIRChangeNotifyEvent less_than;
    struct fc_queue_info qinfo;
    time_t last_time;
    int wait_seconds;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "chg-notify");
#endif

    last_time = g_current_time;
    while (SF_G_CONTINUE_FLAG) {
        wait_seconds = (last_time + BATCH_STORE_INTERVAL) - g_current_time;
        last_time = g_current_time;
        if (wait_seconds > 0 && __sync_add_and_fetch(&change_notify_ctx.
                    waiting_count, 0) < BATCH_STORE_ON_MODIFIES)
        {
            if (sorted_queue_timedpeek_sec(&change_notify_ctx.
                        queue, wait_seconds) == NULL)
            {
                continue;
            }
        }

        last_data_versions.data_thread = data_thread_get_last_data_version();
        last_data_versions.binlog_writer = binlog_writer_get_last_version();
        less_than.version = FC_MIN(last_data_versions.data_thread,
                last_data_versions.binlog_writer);
        sorted_queue_try_pop_to_queue(&change_notify_ctx.
                queue, &less_than, &qinfo);
        if (qinfo.head != NULL) {
            logInfo("file: "__FILE__", line: %d, "
                    "last_data_versions: {data_thread: %"PRId64", "
                    "binlog_writer: %"PRId64"}", __LINE__,
                    last_data_versions.data_thread,
                    last_data_versions.binlog_writer);
            if (deal_events(&qinfo) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "deal notify events fail, "
                        "program exit!", __LINE__);
                sf_terminate_myself();
            }
        }
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
    bool notify;

    notify = __sync_add_and_fetch(&change_notify_ctx.
            waiting_count, 1) == BATCH_STORE_ON_MODIFIES;
    sorted_queue_push_silence(&change_notify_ctx.queue, event);
    if (notify) {
        pthread_cond_signal(&change_notify_ctx.queue.queue.lc_pair.cond);
    }
}

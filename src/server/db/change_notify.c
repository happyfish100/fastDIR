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
#include "event_dealer.h"
#include "change_notify.h"

typedef struct fdir_change_notify_context {
    volatile int waiting_count;
    struct sorted_queue queue;
} FDIRchangeNotifyContext;

static FDIRchangeNotifyContext change_notify_ctx;

static void free_events(struct fc_queue_info *qinfo)
{
    FDIRDataThreadContext *head;
    FDIRDataThreadContext *tail;
    FDIRDataThreadContext *thread_ctx;
    FDIRChangeNotifyEvent *event;
    struct fast_mblock_node *node;

    head = tail = NULL;
    event = qinfo->head;
    while (event != NULL) {
        node = fast_mblock_to_node_ptr(event);
        if (event->thread_ctx->event.chain.head == NULL) {
            if (head == NULL) {
                head = event->thread_ctx;
            } else {
                tail->event.next = event->thread_ctx;
            }
            tail = event->thread_ctx;

            event->thread_ctx->event.chain.head = node;
        } else {
            event->thread_ctx->event.chain.tail->next = node;
        }
        event->thread_ctx->event.chain.tail = node;

        event = event->next;
    }

    tail->event.next = NULL;
    thread_ctx = head;
    while (thread_ctx != NULL) {
        thread_ctx->event.chain.tail->next = NULL;
        fast_mblock_batch_free(&thread_ctx->event.allocator,
                &thread_ctx->event.chain);
        thread_ctx->event.chain.head = NULL;
        thread_ctx->event.chain.tail = NULL;

        thread_ctx = thread_ctx->event.next;
    }
}

static int deal_events(struct fc_queue_info *qinfo)
{
    int result;
    int count;

    if ((result=event_dealer_do(qinfo->head, &count)) != 0) {
        return result;
    }

    free_events(qinfo);
    __sync_sub_and_fetch(&change_notify_ctx.
            waiting_count, count);
    return 0;
}

static void *change_notify_func(void *arg)
{
    FDIRChangeNotifyEvent less_equal;
    struct fc_queue_info qinfo;
    time_t last_time;
    int wait_seconds;
    int waiting_count;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "chg-notify");
#endif

    last_time = g_current_time;
    while (SF_G_CONTINUE_FLAG) {
        wait_seconds = (last_time + BATCH_STORE_INTERVAL + 1) - g_current_time;
        waiting_count = FC_ATOMIC_GET(change_notify_ctx.waiting_count);
        if (wait_seconds > 0 && waiting_count < BATCH_STORE_ON_MODIFIES) {
            lcp_timedwait_sec(&change_notify_ctx.queue.
                    queue.lc_pair, wait_seconds);
        }

        last_time = g_current_time;
        if (waiting_count == 0) {
            waiting_count = FC_ATOMIC_GET(change_notify_ctx.waiting_count);
            if (waiting_count == 0) {
                continue;
            }
        }

        if (DATA_LOAD_DONE) {
            less_equal.version = binlog_writer_get_last_version();
        } else {
            less_equal.version = data_thread_get_last_data_version();
        }

        sorted_queue_try_pop_to_queue(&change_notify_ctx.
                queue, &less_equal, &qinfo);
        if (qinfo.head != NULL) {
            /*
            logInfo("file: "__FILE__", line: %d, "
                    "less than version: %"PRId64,
                    __LINE__, less_equal.version);
                    */
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

void change_notify_load_done_signal()
{
    while (FC_ATOMIC_GET(change_notify_ctx.waiting_count) > 0 &&
            SF_G_CONTINUE_FLAG)
    {
        pthread_cond_signal(&change_notify_ctx.queue.queue.lc_pair.cond);
        fc_sleep_ms(1);
    }
    DATA_LOAD_DONE = true;
}

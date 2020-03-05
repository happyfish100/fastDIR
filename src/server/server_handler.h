//server_handler.h

#ifndef FDIR_SERVER_HANDLER_H
#define FDIR_SERVER_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#define TASK_ARG ((FDIRServerTaskArg *)task->arg)
#define REQUEST  TASK_ARG->context.request
#define RESPONSE TASK_ARG->context.response
#define RESPONSE_STATUS RESPONSE.header.status
#define RECORD   TASK_ARG->context.record

#ifdef __cplusplus
extern "C" {
#endif

int server_handler_init();
int server_handler_destroy();
int server_deal_task(struct fast_task_info *task);
void server_task_finish_cleanup(struct fast_task_info *task);
void *server_alloc_thread_extra_data(const int thread_index);
//int server_thread_loop(struct nio_thread_data *thread_data);

static inline int server_expect_body_length(
        struct fast_task_info *task,
        const int expect_body_length)
{
    if (REQUEST.header.body_len != expect_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d != %d",
                REQUEST.header.body_len, expect_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_min_body_length(
        struct fast_task_info *task,
        const int min_body_length)
{
    if (REQUEST.header.body_len < min_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d < %d",
                REQUEST.header.body_len, min_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_max_body_length(
        struct fast_task_info *task,
        const int max_body_length)
{
    if (REQUEST.header.body_len > max_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d > %d",
                REQUEST.header.body_len, max_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_body_length(
        struct fast_task_info *task,
        const int min_body_length, const int max_body_length)
{
    int result;
    if ((result=server_check_min_body_length(task, min_body_length)) != 0) {
        return result;
    }
    return server_check_max_body_length(task, max_body_length);
}

#ifdef __cplusplus
}
#endif

#endif

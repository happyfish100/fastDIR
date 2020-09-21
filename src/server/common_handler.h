//common_handler.h

#ifndef FDIR_COMMON_HANDLER_H
#define FDIR_COMMON_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus

extern "C" {
#endif

static inline void handler_init_task_context(struct fast_task_info *task)
{
    TASK_ARG->req_start_time = get_current_time_us();
    RESPONSE.header.cmd = SF_PROTO_ACK;
    RESPONSE.header.body_len = 0;
    RESPONSE.header.status = 0;
    RESPONSE.error.length = 0;
    RESPONSE.error.message[0] = '\0';
    TASK_ARG->context.log_level = LOG_ERR;
    TASK_ARG->context.response_done = false;
    TASK_ARG->context.need_response = true;

    REQUEST.header.cmd = ((FDIRProtoHeader *)task->data)->cmd;
    REQUEST.header.body_len = task->length - sizeof(FDIRProtoHeader);
    REQUEST.header.status = buff2short(((FDIRProtoHeader *)task->data)->status);
    REQUEST.body = task->data + sizeof(FDIRProtoHeader);
}

int handler_deal_task_done(struct fast_task_info *task);

#ifdef __cplusplus
}
#endif

#endif

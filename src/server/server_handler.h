//server_handler.h

#ifndef FDIR_SERVER_HANDLER_H
#define FDIR_SERVER_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_handler_init();
int server_handler_destroy();
int server_deal_task(struct fast_task_info *task);
void server_task_finish_cleanup(struct fast_task_info *task);
void *server_alloc_thread_extra_data(const int thread_index);
//int server_thread_loop(struct nio_thread_data *thread_data);

#ifdef __cplusplus
}
#endif

#endif

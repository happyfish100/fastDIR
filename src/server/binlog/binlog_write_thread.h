//binlog_write_thread.h

#ifndef _BINLOG_WRITE_THREAD_H_
#define _BINLOG_WRITE_THREAD_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct common_blocked_queue *g_writer_queue;

int binlog_write_thread_init();
void binlog_write_thread_finish();

void *binlog_write_thread_func(void *arg);

int binlog_get_current_write_index();
void binlog_get_current_write_position(FDIRBinlogFilePosition *position);

#define push_to_binlog_write_queue(rbuffer)  \
    return common_blocked_queue_push(&g_writer_queue, rbuffer)

#ifdef __cplusplus
}
#endif

#endif

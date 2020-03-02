//binlog_write_thread.h

#ifndef _BINLOG_WRITE_THREAD_H_
#define _BINLOG_WRITE_THREAD_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_write_thread_init();
void binlog_write_thread_finish();

void *binlog_write_thread_func(void *arg);

int binlog_get_current_write_index();

#ifdef __cplusplus
}
#endif

#endif

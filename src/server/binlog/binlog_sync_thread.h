//binlog_sync_thread.h

#ifndef _BINLOG_SYNC_THREAD_H_
#define _BINLOG_SYNC_THREAD_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

void *binlog_sync_thread_func(void *arg);

#ifdef __cplusplus
}
#endif

#endif

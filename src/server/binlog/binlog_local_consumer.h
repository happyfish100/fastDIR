//binlog_local_consumer.h

#ifndef _BINLOG_LOCAL_CONSUMER_H_
#define _BINLOG_LOCAL_CONSUMER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_local_consumer_init();
void binlog_local_consumer_destroy();
void binlog_local_consumer_terminate();

int binlog_local_consumer_replication_start();
int binlog_local_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer);

#ifdef __cplusplus
}
#endif

#endif

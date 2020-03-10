//binlog_consumer.h

#ifndef _BINLOG_CONSUMER_H_
#define _BINLOG_CONSUMER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_consumer_init();
void binlog_consumer_destroy();
void binlog_consumer_terminate();

int binlog_consumer_replication_start();
int binlog_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer);

#ifdef __cplusplus
}
#endif

#endif

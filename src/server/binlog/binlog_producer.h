//binlog_producer.h

#ifndef _BINLOG_PRODUCER_H_
#define _BINLOG_PRODUCER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_producer_init();
int binlog_producer_start();
void binlog_producer_destroy();

ServerBinlogRecordBuffer *server_binlog_alloc_hold_rbuffer();

void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *rbuffer);

void server_binlog_free_rbuffer(ServerBinlogRecordBuffer *rbuffer);

//int server_binlog_dispatch(ServerBinlogRecordBuffer *rbuffer);
void binlog_push_to_producer_queue(ServerBinlogRecordBuffer *rbuffer);

#ifdef __cplusplus
}
#endif

#endif

//binlog_producer.h

#ifndef _BINLOG_PRODUCER_H_
#define _BINLOG_PRODUCER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_producer_init();
void binlog_producer_destroy();

ServerBinlogRecordBuffer *server_binlog_alloc_rbuffer();

void binlog_producer_init_next_data_version();

int server_binlog_dispatch(ServerBinlogRecordBuffer *rbuffer);

#ifdef __cplusplus
}
#endif

#endif

//server_binlog.h

#ifndef _SERVER_BINLOG_H_
#define _SERVER_BINLOG_H_

#include "binlog/binlog_types.h"
#include "binlog/binlog_producer.h"
#include "binlog/binlog_local_consumer.h"
#include "binlog/binlog_write_thread.h"
#include "binlog/binlog_read_thread.h"
#include "binlog/replica_consumer_thread.h"
#include "binlog/binlog_pack.h"
#include "binlog/binlog_replay.h"
#include "binlog/push_result_ring.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_binlog_init();
void server_binlog_destroy();
void server_binlog_terminate();

#ifdef __cplusplus
}
#endif

#endif

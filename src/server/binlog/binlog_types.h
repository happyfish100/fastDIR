//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

typedef struct server_binlog_buffer {
    char *buffer;  //the buffer pointer
    int length;    //the content length
    int size;      //the buffer size (capacity)
} ServerBinlogBuffer;

typedef struct server_binlog_consumer_context {
    struct common_blocked_queue queue;
    FDIRClusterServerInfo *server;
} ServerBinlogConsumerContext;

typedef struct server_binlog_record_buffer {
    int64_t data_version;
    unsigned int hash_code;
    volatile int reffer_count;
    FastBuffer record;
} ServerBinlogRecordBuffer;

typedef struct server_binlog_consumer_array {
    ServerBinlogConsumerContext *contexts;
    int count;
} ServerBinlogConsumerArray;

#endif

//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

typedef struct server_binlog_consumer_context {
    struct common_blocked_queue queue;
} ServerBinlogConsumerContext;

typedef struct server_binlog_record_buffer {
    int64_t data_version;
    FastBuffer record;
    volatile int reffer_count;
} ServerBinlogRecordBuffer;

typedef struct server_binlog_consumer_array {
    ServerBinlogConsumerContext *contexts;
    int count;
} ServerBinlogConsumerArray;

#endif

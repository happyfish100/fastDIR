//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

typedef struct fdir_binlog_path_info {
    FDIRDEntryFullName fullname;
    int hash_code;
} FDIRBinlogPathInfo;

typedef struct fdir_binlog_record {
    int64_t data_version;
    int64_t inode;
    int operation;
    union {
        int64_t flags;
        struct {
            union {
                int flags: 3;
                struct {
                    bool ns: 1;  //namespace
                    bool pt: 1;  //path
                    bool hc: 1;  //hash code
                };
            } path_info;
            bool user_data : 1;
            bool extra_data: 1;
            bool mode : 1;
            bool ctime: 1;
            bool mtime: 1;
            bool size : 1;
        };
    } options;
    FDIRBinlogPathInfo path;
    FDIRDEntryStatus stat;
    string_t user_data;
    string_t extra_data;
} FDIRBinlogRecord;

typedef struct server_binlog_buffer {
    char *buff;    //the buffer pointer
    int length;    //the content length
    int size;      //the buffer size (capacity)
} ServerBinlogBuffer;

typedef struct server_binlog_consumer_context {
    struct common_blocked_queue queue;
    FDIRClusterServerInfo *server;
} ServerBinlogConsumerContext;

typedef struct server_binlog_record_buffer {
    int64_t data_version;
    volatile int reffer_count;
    FastBuffer record;
} ServerBinlogRecordBuffer;

typedef struct server_binlog_consumer_array {
    ServerBinlogConsumerContext *contexts;
    int count;
} ServerBinlogConsumerArray;

#endif

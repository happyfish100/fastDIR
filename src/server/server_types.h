/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef _FDIR_SERVER_TYPES_H
#define _FDIR_SERVER_TYPES_H

#include <time.h>
#include <pthread.h>
#include "fastcommon/common_define.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fast_allocator.h"
#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/server_id_func.h"
#include "fastcommon/fc_list.h"
#include "fastcommon/fc_queue.h"
#include "fastcommon/fc_atomic.h"
#include "fastcommon/array_allocator.h"
#include "fastcommon/sorted_array.h"
#include "sf/sf_types.h"
#include "sf/idempotency/server/server_types.h"
#include "diskallocator/binlog/common/binlog_types.h"
#include "common/fdir_types.h"
#include "common/fdir_server_types.h"

#define FDIR_MAX_NS_SUBSCRIBERS                8

#define FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING  0
#define FDIR_NS_SUBSCRIBE_QUEUE_INDEX_SENDING  1

#define FDIR_CLUSTER_ID_BITS                 10
#define FDIR_CLUSTER_ID_MAX                  ((1 << FDIR_CLUSTER_ID_BITS) - 1)

#define FDIR_SERVER_DEFAULT_RELOAD_INTERVAL       500
#define FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL  300
#define FDIR_NAMESPACE_HASHTABLE_DEFAULT_CAPACITY 1361
#define FDIR_INODE_HASHTABLE_DEFAULT_CAPACITY     11229331
#define FDIR_INODE_SHARED_LOCKS_DEFAULT_COUNT     163
#define FDIR_DEFAULT_DATA_THREAD_COUNT              1
#define FDIR_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS      64
#define FDIR_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS   3

#define FDIR_SERVER_TASK_TYPE_RELATIONSHIP       1   //slave  -> master
#define FDIR_SERVER_TASK_TYPE_REPLICA_MASTER     2   //[Master] -> slave
#define FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE      3   //master -> [Slave]
#define FDIR_SERVER_TASK_TYPE_NSS_SUBSCRIBE      4   //auth server -> master

#define FDIR_REPLICATION_STAGE_NONE               0
#define FDIR_REPLICATION_STAGE_CONNECTING         1
#define FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP  2
#define FDIR_REPLICATION_STAGE_SYNC_FROM_DISK     3
#define FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE    4

#define TASK_STATUS_CONTINUE           12345
#define TASK_UPDATE_FLAG_OUTPUT_DENTRY     1

#define FDIR_BINLOG_SUBDIR_NAME      "binlog"

#define FDIR_FORCE_ELECTION_LONG_OPTION_STR  "force-master-election"
#define FDIR_FORCE_ELECTION_LONG_OPTION_LEN  (sizeof(  \
            FDIR_FORCE_ELECTION_LONG_OPTION_STR) - 1)

#define TASK_ARG          ((FDIRServerTaskArg *)task->arg)
#define TASK_CTX          TASK_ARG->context
#define REQUEST           TASK_CTX.common.request
#define RESPONSE          TASK_CTX.common.response
#define RESPONSE_STATUS   RESPONSE.header.status
#define REQUEST_STATUS    REQUEST.header.status
#define RECORD            TASK_CTX.service.record
#define RBUFFER           TASK_CTX.service.rbuffer
#define FTASK_HEAD_PTR    &TASK_CTX.service.ftasks
#define SYS_LOCK_TASK     TASK_CTX.service.sys_lock_task
#define WAITING_RPC_COUNT TASK_CTX.service.waiting_rpc_count
#define DENTRY_LIST_CACHE TASK_CTX.service.dentry_list_cache

#define SERVER_TASK_TYPE     TASK_CTX.task_type
#define CLUSTER_PEER         TASK_CTX.shared.cluster.peer
#define CLUSTER_REPLICA      TASK_CTX.shared.cluster.replica
#define CLUSTER_CONSUMER_CTX TASK_CTX.shared.cluster.consumer_ctx
#define IDEMPOTENCY_CHANNEL  TASK_CTX.shared.service.idempotency_channel
#define IDEMPOTENCY_REQUEST  TASK_CTX.service.idempotency_request
#define NS_SUBSCRIBER        TASK_CTX.subscriber

#define SERVER_CTX        ((FDIRServerContext *)task->thread_data->arg)


typedef void (*server_free_func)(void *ptr);
typedef void (*server_free_func_ex)(void *ctx, void *ptr);

typedef struct fdir_path_info {
    string_t paths[FDIR_MAX_PATH_COUNT];   //splited path parts
    int count;
} FDIRPathInfo;

struct fdir_namespace_entry;
struct fdir_dentry_context;
struct fdir_server_dentry;
struct flock_entry;

#define DENTRY_SKIPLIST_INIT_LEVEL_COUNT   2
#define FDIR_DENTRY_LOADED_FLAGS_BASIC    (1 << 0)
#define FDIR_DENTRY_LOADED_FLAGS_CHILDREN (1 << 1)
#define FDIR_DENTRY_LOADED_FLAGS_XATTR    (1 << 2)
#define FDIR_DENTRY_LOADED_FLAGS_CLIST    (1 << 3) /* child list for serialization */
#define FDIR_DENTRY_LOADED_FLAGS_ALL      (FDIR_DENTRY_LOADED_FLAGS_BASIC | \
        FDIR_DENTRY_LOADED_FLAGS_CHILDREN | FDIR_DENTRY_LOADED_FLAGS_XATTR |\
        FDIR_DENTRY_LOADED_FLAGS_CLIST)

typedef struct fdir_server_dentry_db_args {
    IdNameArray *children;         //children inodes for update event dealer
    struct fc_list_head lru_dlink; //for dentry LRU elimination
    int loaded_count;              //children loaded count
    short loaded_flags;
    bool add_to_clist;  //if add to child list for serialization (just a temp variable)
} FDIRServerDentryDBArgs;

typedef struct fdir_server_dentry {
    int64_t inode;
    string_t name;
    volatile int reffer_count;

    FDIRDEntryStat stat;

    union {
        string_t link;    //for symlink
        struct fdir_server_dentry *src_dentry;  //for hard link
    };

    SFKeyValueArray *kv_array;   //for x-attributes
    struct fdir_dentry_context *context;
    UniqSkiplist *children;
    struct fdir_server_dentry *parent;
    struct fdir_namespace_entry *ns_entry;
    struct flock_entry *flock_entry;
    struct fdir_server_dentry *free_next; //for delay free dentry
    struct fdir_server_dentry *ht_next;   //for inode index hash table
    FDIRServerDentryDBArgs db_args[0];    //for data persistency, since V3.0
} FDIRServerDentry;

typedef struct fdir_cluster_server_info {
    FCServerInfo *server;
    char key[FDIR_REPLICA_KEY_SIZE];  //for slave server
    volatile char status;          //the slave status
    volatile char is_master;       //if I am master
    SFBinlogFilePosition binlog_pos_hint;  //for replication
    volatile int64_t last_data_version;  //for replication
    volatile int last_change_version;    //for push server status to the slave
} FDIRClusterServerInfo;

typedef struct fdir_cluster_server_array {
    FDIRClusterServerInfo *servers;
    int count;
    volatile int alives;
    volatile int change_version;
} FDIRClusterServerArray;

struct server_binlog_record_buffer;

typedef struct fdir_record_buffer_queue {
    struct server_binlog_record_buffer *head;
    struct server_binlog_record_buffer *tail;
    pthread_mutex_t lock;
} FDIRRecordBufferQueue;

typedef struct fdir_binlog_push_result_entry {
    uint64_t data_version;
    time_t expires;
    struct fast_task_info *waiting_task;
    struct fdir_binlog_push_result_entry *next;
} FDIRBinlogPushResultEntry;

typedef struct fdir_binlog_push_result_context {
    struct {
        FDIRBinlogPushResultEntry *entries;
        FDIRBinlogPushResultEntry *start; //for consumer
        FDIRBinlogPushResultEntry *end;   //for producer
        int size;
    } ring;

    struct {
        FDIRBinlogPushResultEntry *head;
        FDIRBinlogPushResultEntry *tail;
        struct fast_mblock_man rentry_allocator;
    } queue;   //for overflow exceptions

    time_t last_check_timeout_time;
} FDIRBinlogPushResultContext;

struct binlog_read_thread_context;
typedef struct fdir_replication_context {
    FDIRRecordBufferQueue queue;  //push to the slave
    struct binlog_read_thread_context *reader_ctx; //read from binlog file
    FDIRBinlogPushResultContext push_result_ctx;   //push result recv from the slave
    struct {
        int64_t by_queue;
        struct {
            int64_t previous;
            int64_t current;
        } by_disk;
        int64_t by_resp;  //for flow control
    } last_data_versions;

    struct {
        int64_t start_time_ms;
        int64_t binlog_size;
        int64_t record_count;
    } sync_by_disk_stat;
} FDIRReplicationContext;

typedef struct fdir_slave_replication {
    struct fast_task_info *task;
    FDIRClusterServerInfo *slave;
    int stage;
    int index;  //for next links
    struct {
        int start_time;
        int next_connect_time;
        int last_errno;
        int fail_count;
        ConnectionInfo conn;
    } connection_info;

    FDIRReplicationContext context;
} FDIRSlaveReplication;

typedef struct fdir_slave_replication_array {
    FDIRSlaveReplication *replications;
    int count;
} FDIRSlaveReplicationArray;

typedef struct fdir_slave_replication_ptr_array {
    int count;
    FDIRSlaveReplication **replications;
} FDIRSlaveReplicationPtrArray;

struct fdir_binlog_record;
struct flock_task;
struct sys_lock_task;

typedef struct fdir_ns_subscriber {
    int index;  //for allocating FDIRNSSubscribeEntry
    struct fc_queue queues[2];  //element: FDIRNSSubscribeEntry
    struct fc_list_head dlink;  //for subscriber's chain
    struct fdir_ns_subscriber *next;  //for freelist
} FDIRNSSubscriber;

typedef struct server_task_arg {
    struct {
        SFCommonTaskContext common;
        int task_type;

        union {
            struct {
                struct idempotency_channel *idempotency_channel;
            } service;

            union {
                FDIRClusterServerInfo *peer;   //the peer server in the cluster
                FDIRSlaveReplication *replica; //master side
                struct replica_consumer_thread_context *consumer_ctx;//slave side
            } cluster;
        } shared;

        union {
            struct {
                struct {
                    PointerArray *array;
                    int64_t token;
                    int offset;
                    int release_start;
                    bool compact_output;
                    time_t expires;  //expire time
                } dentry_list_cache; //for dentry_list

                struct fc_list_head ftasks;  //for flock
                struct sys_lock_task *sys_lock_task; //for append and ftruncate

                struct idempotency_request *idempotency_request;
                struct fdir_binlog_record *record;
                struct server_binlog_record_buffer *rbuffer;
                volatile int waiting_rpc_count;
            } service;

            FDIRNSSubscriber *subscriber;
        };

    } context;

} FDIRServerTaskArg;


typedef struct fdir_server_context {
    union {
        struct {
            struct fast_mblock_man record_allocator;
            struct fast_mblock_man record_parray_allocator;
            struct fast_mblock_man request_allocator; //for idempotency_request
        } service;

        struct {
            bool clean_connected_replicas;   //for cleanup connected array
            FDIRSlaveReplicationPtrArray connectings;  //master side
            FDIRSlaveReplicationPtrArray connected;    //master side

            struct replica_consumer_thread_context *consumer_ctx;//slave side
        } cluster;
    };
} FDIRServerContext;

#endif

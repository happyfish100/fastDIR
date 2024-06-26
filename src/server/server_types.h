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
#define FDIR_NODE_HASHTABLE_DEFAULT_CAPACITY      1361
#define FDIR_INODE_HASHTABLE_DEFAULT_CAPACITY     11229331
#define FDIR_INODE_SHARED_LOCKS_DEFAULT_COUNT     163
#define FDIR_DEFAULT_DATA_THREAD_COUNT              1
#define FDIR_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS      64
#define FDIR_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS   3

#define FDIR_SERVER_TASK_TYPE_RELATIONSHIP       1   //slave  -> master
#define FDIR_SERVER_TASK_TYPE_REPLICA_MASTER     2   //[Master] -> slave
#define FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE      3   //master -> [Slave]
#define FDIR_SERVER_TASK_TYPE_SYNC_BINLOG        4   //slave  -> master
#define FDIR_SERVER_TASK_TYPE_NSS_SUBSCRIBE      5   //auth server -> master

#define FDIR_REPLICATION_STAGE_NONE               0
#define FDIR_REPLICATION_STAGE_IN_QUEUE           1  //for adding to conecting array
#define FDIR_REPLICATION_STAGE_BEFORE_CONNECT     2
#define FDIR_REPLICATION_STAGE_CONNECTING         3
#define FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP  4
#define FDIR_REPLICATION_STAGE_SYNC_FROM_DISK     5
#define FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE    6

#define TASK_STATUS_CONTINUE           12345
#define TASK_UPDATE_FLAG_OUTPUT_DENTRY     1

#define FDIR_DATA_DUMP_DUMMY_INODE      1000001234367890LL

#define FDIR_BINLOG_SUBDIR_NAME         "binlog"
#define FDIR_DATA_DUMP_SUBDIR_NAME      "binlog/dump"
#define FDIR_RECOVERY_SUBDIR_NAME       "recovery"
#define FDIR_RECOVERY_DUMP_SUBDIR_NAME  "recovery/dump"

#define FDIR_FORCE_ELECTION_LONG_OPTION_STR  "force-master-election"
#define FDIR_FORCE_ELECTION_LONG_OPTION_LEN  (sizeof(  \
            FDIR_FORCE_ELECTION_LONG_OPTION_STR) - 1)

#define TASK_PENDING_SEND_COUNT task->pending_send_count
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
#define SERVICE_FTYPE     TASK_CTX.service.flock_type
#define SERVICE_FLOCK     TASK_CTX.service.flock
#define SERVICE_FTASK     SERVICE_FLOCK.ftask
#define SERVICE_STASK     SERVICE_FLOCK.stask
#define DENTRY_LIST_CACHE  TASK_CTX.service.dentry_list_cache
#define SERVICE_FRONT_SIZE TASK_CTX.service.front_size

#define SERVER_TASK_TYPE     TASK_CTX.task_type
#define SERVER_TASK_VERSION  TASK_CTX.task_version
#define CLUSTER_PEER         TASK_CTX.shared.cluster.peer
#define CLUSTER_REPLICA      TASK_CTX.shared.cluster.replica
#define TASK_CONSUMER_CTX    TASK_CTX.shared.cluster.consumer_ctx
#define REPLICA_READER       TASK_CTX.shared.cluster.reader
#define IDEMPOTENCY_CHANNEL  TASK_CTX.shared.service.idempotency_channel
#define NS_SUBSCRIBER        TASK_CTX.shared.service.subscriber
#define IDEMPOTENCY_REQUEST  TASK_CTX.service.idempotency_request

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
struct fdir_flock_entry;

#define DENTRY_SKIPLIST_INIT_LEVEL_COUNT   2
#define FDIR_DENTRY_LOADED_FLAGS_BASIC    (1 << 0)
#define FDIR_DENTRY_LOADED_FLAGS_CHILDREN (1 << 1)
#define FDIR_DENTRY_LOADED_FLAGS_XATTR    (1 << 2)
#define FDIR_DENTRY_LOADED_FLAGS_CLIST    (1 << 3) /* child list for serialization */
#define FDIR_DENTRY_LOADED_FLAGS_ALL      (FDIR_DENTRY_LOADED_FLAGS_BASIC | \
        FDIR_DENTRY_LOADED_FLAGS_CHILDREN | FDIR_DENTRY_LOADED_FLAGS_XATTR |\
        FDIR_DENTRY_LOADED_FLAGS_CLIST)

typedef enum {
    fdir_posix_acl_none,
    fdir_posix_acl_strict
} FDIRPosixACLPolicy;

typedef enum {
    fdir_children_container_sortedarray,
    fdir_children_container_skiplist
} FDIRChildrenContainer;

typedef struct fdir_server_dentry_db_args {
    union {
        IdNameArray *sa;
        UniqSkiplist *sl;
        void *ptr;
    } children;         //children inodes for update event dealer
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
    struct fdir_flock_entry *flock_entry;
    struct fdir_server_dentry *free_next; //for delay free dentry
    struct fdir_server_dentry *ht_next;   //for inode index hash table
    FDIRServerDentryDBArgs db_args[0];    //for data persistency, since V3.0
} FDIRServerDentry;

struct fdir_slave_replication;

typedef struct fdir_cluster_server_info {
    FCServerInfo *server;
    char key[FDIR_REPLICA_KEY_SIZE]; //for slave server
    volatile char status;            //the slave status
    volatile char is_master;         //if I am the master
    volatile char is_old_master;     //if I am the old master
    volatile bool recovering;        //if data recovering
    int check_fail_count;
    SFBinlogFilePosition binlog_pos_hint;    //for replication
    volatile int64_t last_data_version;      //for replication
    volatile int64_t confirmed_data_version; //for replication quorum majority
    volatile int last_change_version;    //for push server status to the slave
    struct fdir_slave_replication *replica;
} FDIRClusterServerInfo;

typedef struct fdir_cluster_server_array {
    FDIRClusterServerInfo *servers;
    int count;
    volatile int active_count;
    volatile int change_version;
} FDIRClusterServerArray;

typedef struct fdir_cluster_server_ptr_array {
    FDIRClusterServerInfo **servers;
    int count;
} FDIRClusterServerPtrArray;

struct server_binlog_record_buffer;

typedef struct fdir_record_buffer_queue {
    struct server_binlog_record_buffer *head;
    struct server_binlog_record_buffer *tail;
    pthread_mutex_t lock;
} FDIRRecordBufferQueue;

typedef struct fdir_replica_rpc_result_entry {
    uint64_t data_version;
    struct fast_task_info *waiting_task;
} FDIRReplicaRPCResultEntry;

typedef struct fdir_replica_rpc_result_array {
    FDIRReplicaRPCResultEntry *results;
    int alloc;
    int count;
} FDIRReplicaRPCResultArray;

struct binlog_read_thread_context;
typedef struct fdir_replication_context {
    FDIRRecordBufferQueue queue;  //push to the slave
    struct binlog_read_thread_context *reader_ctx; //read from binlog file
    FDIRReplicaRPCResultArray rpc_result_array;
    struct {
        int64_t by_queue;
        struct {
            int64_t previous;
            int64_t current;
        } by_disk;
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
    volatile int stage;
    int index;  //for next links
    int join_fail_count;  //join slave successive fail count
    struct {
        int start_time;
        int next_connect_time;
        time_t last_net_comm_time;
        int last_errno;
        int fail_count;
    } connection_info;

    SFRequestMetadataArray req_meta_array;

    FDIRReplicationContext context;
    struct fdir_slave_replication *next;
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

typedef struct fdir_ns_subscriber {
    int index;  //for allocating FDIRNSSubscribeEntry
    struct fc_queue queues[2];  //element: FDIRNSSubscribeEntry
    struct fc_list_head dlink;  //for subscriber's chain
    struct fdir_ns_subscriber *next;  //for freelist
} FDIRNSSubscriber;

struct fdir_flock_region;
struct fdir_flock_context;

typedef struct fdir_flock_params {
    short type;
    FDIRFlockOwner owner;
    int64_t offset;
    int64_t length;
} FDIRFlockParams;

typedef struct fdir_flock_task {
    /* LOCK_SH for shared read lock, LOCK_EX for exclusive write lock  */
    short type;
    volatile short which_queue;
    volatile int reffer_count;
    FDIRFlockOwner owner;
    struct fdir_flock_region *region;
    struct fdir_flock_context *flock_ctx;
    struct fast_task_info *task;
    FDIRServerDentry *dentry;
    struct fc_list_head clink;  //for connection double link chain
    struct fc_list_head flink;  //for flock chain and queue
} FDIRFLockTask;

typedef struct fdir_sys_lock_task {
    short status;
    struct fast_task_info *task;
    FDIRServerDentry *dentry;
    struct fc_list_head dlink;
} FDIRSysLockTask;

typedef union {
    FDIRFLockTask *ftask;   //for flock apply
    FDIRSysLockTask *stask; //for sys lock apply
} FDIRFLockTaskInfo;

typedef struct fdir_ftask_change_event {
    int type;
    uint32_t task_version;  //for ABA check
    FDIRFLockTask *ftask;
    struct fdir_ftask_change_event *next;
} FDIRFTaskChangeEvent;

typedef struct {
    FDIRDEntryInfo dentry;
    int64_t data_version;
} FDIRIdempotencyResponse;

typedef struct {
    SFCommonTaskContext common;
    int task_type;
    uint32_t task_version;  //for ABA check

    union {
        union {
            struct idempotency_channel *idempotency_channel;
            FDIRNSSubscriber *subscriber;
        } service;

        struct {
            union {
                FDIRClusterServerInfo *peer;   //the peer server in the cluster
                FDIRSlaveReplication *replica; //master side
                struct replica_consumer_thread_context *consumer_ctx;//slave side
                struct server_binlog_reader *reader; //for fetch/sync binlog
            };
        } cluster;
    } shared;

    struct {
        struct {
            PointerArray *array;
            int64_t token;
            int offset;
            int release_start;
            bool compact_output;
            bool output_special;
            time_t expires;  //expire time
        } dentry_list_cache; //for dentry_list

        struct {
            int flock_type;  //LOCK_EX, LOCK_SH, LOCK_UN
            struct fc_list_head ftasks;
            FDIRSysLockTask *sys_lock_task; //for sys lock apply
            FDIRFLockTaskInfo flock;
        };

        struct idempotency_request *idempotency_request;
        struct fdir_binlog_record *record;
        struct server_binlog_record_buffer *rbuffer;
        struct {
            volatile short waiting_count;
            volatile short success_count;
        } rpc;
        short front_size;
    } service;

} FSServerTaskContext;

typedef struct server_task_arg {
    FSServerTaskContext context;
} FDIRServerTaskArg;

typedef struct fdir_server_context {
    int thread_index;
    union {
        struct {
            struct fast_mblock_man record_allocator;
            struct fast_mblock_man record_parray_allocator;
            struct fast_mblock_man request_allocator; //for idempotency_request
            struct fast_mblock_man event_allocator; //element: FDIRFTaskChangeEvent
            struct fc_queue queue; //for flock unlock, element: FDIRFTaskChangeEvent
        } service;

        struct {
            volatile char clean_replications; //for cleanup connecting & connected array
            struct {
                FDIRSlaveReplication *head;
                pthread_mutex_t lock;
            } queue;  //waiting to add to connecting array
            FDIRSlaveReplicationPtrArray connectings;  //master side
            FDIRSlaveReplicationPtrArray connected;    //master side
        } cluster;
    };
} FDIRServerContext;

#endif

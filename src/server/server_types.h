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
#include "sf/idempotency/server/server_types.h"
#include "common/fdir_types.h"

#define FDIR_CLUSTER_ID_BITS                 10
#define FDIR_CLUSTER_ID_MAX                  ((1 << FDIR_CLUSTER_ID_BITS) - 1)

#define FDIR_SERVER_DEFAULT_RELOAD_INTERVAL       500
#define FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL  300
#define FDIR_NAMESPACE_HASHTABLE_DEFAULT_CAPACITY 1361
#define FDIR_INODE_HASHTABLE_DEFAULT_CAPACITY     1403641
#define FDIR_INODE_SHARED_LOCKS_DEFAULT_COUNT     163
#define FDIR_DEFAULT_DATA_THREAD_COUNT              1

#define FDIR_SERVER_TASK_TYPE_RELATIONSHIP       1   //slave  -> master
#define FDIR_SERVER_TASK_TYPE_REPLICA_MASTER     2   //[Master] -> slave
#define FDIR_SERVER_TASK_TYPE_REPLICA_SLAVE      3   //master -> [Slave]

#define FDIR_REPLICATION_STAGE_NONE               0
#define FDIR_REPLICATION_STAGE_CONNECTING         1
#define FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP  2
#define FDIR_REPLICATION_STAGE_SYNC_FROM_DISK     3
#define FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE    4

#define TASK_STATUS_CONTINUE           12345
#define TASK_UPDATE_FLAG_OUTPUT_DENTRY     1

#define TASK_ARG          ((FDIRServerTaskArg *)task->arg)
#define REQUEST           TASK_ARG->context.request
#define RESPONSE          TASK_ARG->context.response
#define RESPONSE_STATUS   RESPONSE.header.status
#define REQUEST_STATUS    REQUEST.header.status
#define RECORD            TASK_ARG->context.service.record
#define FTASK_HEAD_PTR    &TASK_ARG->context.service.ftasks
#define SYS_LOCK_TASK     TASK_ARG->context.service.sys_lock_task
#define WAITING_RPC_COUNT TASK_ARG->context.service.waiting_rpc_count
#define DENTRY_LIST_CACHE TASK_ARG->context.service.dentry_list_cache

#define SERVER_TASK_TYPE  TASK_ARG->context.task_type
#define CLUSTER_PEER      TASK_ARG->context.shared.cluster.peer
#define CLUSTER_REPLICA   TASK_ARG->context.shared.cluster.replica
#define CLUSTER_CONSUMER_CTX  TASK_ARG->context.shared.cluster.consumer_ctx
#define IDEMPOTENCY_CHANNEL   TASK_ARG->context.shared.service.idempotency_channel
#define IDEMPOTENCY_REQUEST   TASK_ARG->context.service.idempotency_request

#define SERVER_CTX        ((FDIRServerContext *)task->thread_data->arg)

typedef void (*server_free_func)(void *ptr);
typedef void (*server_free_func_ex)(void *ctx, void *ptr);

typedef struct fdir_path_info {
    string_t paths[FDIR_MAX_PATH_COUNT];   //splited path parts
    int count;
} FDIRPathInfo;

struct fdir_dentry_context;
struct flock_entry;
typedef struct fdir_server_dentry {
    int64_t inode;
    unsigned int hash_code;   //data thread dispach & mutex lock
    string_t name;
    FDIRDEntryStatus stat;

    union {
        string_t link;    //for symlink
        struct fdir_server_dentry *src_dentry;  //for hard link
    };

    struct fdir_dentry_context *context;
    UniqSkiplist *children;
    struct fdir_server_dentry *parent;
    struct flock_entry *flock_entry;
    struct fdir_server_dentry *ht_next;  //for inode hash table;
} FDIRServerDentry;

typedef struct fdir_server_dentry_array {
    int alloc;
    int count;
    struct fdir_server_dentry **entries;
} FDIRServerDentryArray;  //for list entry

typedef struct fdir_binlog_file_position {
    int index;      //current binlog file
    int64_t offset; //current file offset
} FDIRBinlogFilePosition;

typedef struct fdir_cluster_server_info {
    FCServerInfo *server;
    char key[FDIR_REPLICA_KEY_SIZE];  //for slave server
    volatile char status;          //the slave status
    volatile char is_master;       //if I am master
    FDIRBinlogFilePosition binlog_pos_hint;  //for replication
    volatile int64_t last_data_version;  //for replication
    volatile int last_change_version;    //for push server status to the slave
} FDIRClusterServerInfo;

typedef struct fdir_cluster_server_array {
    FDIRClusterServerInfo *servers;
    int count;
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
    int64_t task_version;
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

typedef struct server_task_arg {
    volatile int64_t task_version;
    int64_t req_start_time;

    struct {
        SFRequestInfo request;
        SFResponseInfo response;
        int (*deal_func)(struct fast_task_info *task);
        bool response_done;
        bool log_error;
        bool need_response;
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

        struct {
            struct {
                FDIRServerDentryArray array;
                int64_t token;
                int offset;
                time_t expires;  //expire time
            } dentry_list_cache; //for dentry_list

            struct fc_list_head ftasks;  //for flock
            struct sys_lock_task *sys_lock_task; //for append and ftruncate

            struct idempotency_request *idempotency_request;
            struct fdir_binlog_record *record;
            volatile int waiting_rpc_count;
        } service;

    } context;

} FDIRServerTaskArg;


typedef struct fdir_server_context {
    union {
        struct {
            struct fast_mblock_man record_allocator;
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

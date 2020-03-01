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
#include "common/fdir_types.h"

#define FDIR_CLUSTER_ID_BITS                 10
#define FDIR_CLUSTER_ID_MAX                  ((1 << FDIR_CLUSTER_ID_BITS) - 1)

#define FDIR_SERVER_DEFAULT_RELOAD_INTERVAL       500
#define FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL  300
#define FDIR_NAMESPACE_HASHTABLE_CAPACITY        1361

typedef void (*server_free_func)(void *ptr);
typedef void (*server_free_func_ex)(void *ctx, void *ptr);

struct fdir_server_context;

typedef struct fdir_dentry_status {
    int mode;
    int ctime;  /* create time */
    int mtime;  /* modify time */
    int64_t size;   /* file size in bytes */
} FDIRDEntryStatus;

typedef struct fdir_dentry_context {
    UniqSkiplistFactory factory;
    struct fast_mblock_man dentry_allocator;
    struct fast_allocator_context name_acontext;
    struct fdir_server_context *server_context;
} FDIRDentryContext;

typedef struct server_delay_free_node {
    int expires;
    void *ctx;     //the context
    void *ptr;     //ptr to free
    server_free_func free_func;
    server_free_func_ex free_func_ex;
    struct server_delay_free_node *next;
} ServerDelayFreeNode;

typedef struct server_delay_free_queue {
    ServerDelayFreeNode *head;
    ServerDelayFreeNode *tail;
} ServerDelayFreeQueue;

typedef struct server_delay_free_context {
    time_t last_check_time;
    ServerDelayFreeQueue queue;
    struct fast_mblock_man allocator;
} ServerDelayFreeContext;

typedef struct fdir_server_context {
    FDIRDentryContext dentry_context;
    ServerDelayFreeContext delay_free_context;
    int thread_index;
} FDIRServerContext;

typedef struct fdir_path_info {
    FDIRDEntryFullName fullname;

    string_t paths[FDIR_MAX_PATH_COUNT];   //splited path parts
    int count;
    unsigned int hash_code;
} FDIRPathInfo;

struct fdir_server_dentry;
typedef struct fdir_server_dentry_array {
    int alloc;
    int count;
    struct fdir_server_dentry **entries;
} FDIRServerDentryArray;

typedef struct fdir_cluster_server_info {
    FCServerInfo *server;
    char key[FDIR_REPLICA_KEY_SIZE];   //for slave server
} FDIRClusterServerInfo;

typedef struct fdir_cluster_server_array {
    int count;
    FDIRClusterServerInfo *servers;
} FDIRClusterServerArray;

typedef struct server_task_arg {
    volatile int64_t task_version;
    int64_t req_start_time;
    FDIRPathInfo path_info;
    struct {
        FDIRServerDentryArray array;
        int64_t token;
        int offset;
        time_t expires;  //expire time
    } dentry_list_cache; //for dentry_list

    FDIRClusterServerInfo *cluster_peer;  //the peer server in the cluster
} FDIRServerTaskArg;

typedef struct {
    struct fast_task_info *task;
    FDIRServerContext *server_context;
    FDIRServerTaskArg *task_arg;

    FDIRRequestInfo request;
    FDIRResponseInfo response;

    bool response_done;
    bool log_error;
} ServerTaskContext;

typedef struct fdir_slave_array {
    int count;
    FDIRClusterServerInfo **servers;
} FDIRServerSlaveArray;

typedef struct fdir_server_cluster {
    int64_t version;
    struct {
        FDIRServerSlaveArray inactives;
        FDIRServerSlaveArray actives;
    } slaves;
    FDIRClusterServerInfo *master;
} FDIRServerCluster;

#endif

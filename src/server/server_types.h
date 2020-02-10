#ifndef _FDIR_SERVER_TYPES_H
#define _FDIR_SERVER_TYPES_H

#include <time.h>
#include <pthread.h>
#include "fastcommon/common_define.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fast_allocator.h"
#include "fastcommon/uniq_skiplist.h"
#include "common/fdir_types.h"

#define FDIR_SERVER_DEFAULT_RELOAD_INTERVAL       500
#define FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL  300
#define FDIR_NAMESPACE_HASHTABLE_CAPACITY        1361

typedef struct fdir_dentry_context {
    UniqSkiplistFactory factory;
    struct fast_mblock_man dentry_allocator;
} FDIRDentryContext;

typedef struct skiplist_delay_free_node {
    int expires;
    UniqSkiplist *skiplist;
    struct skiplist_delay_free_node *next;
} SkiplistDelayFreeNode;

typedef struct skiplist_delay_free_queue {
    SkiplistDelayFreeNode *head;
    SkiplistDelayFreeNode *tail;
} SkiplistDelayFreeQueue;

typedef struct skiplist_delay_free_context {
    time_t last_check_time;
    SkiplistDelayFreeQueue queue;
    struct fast_mblock_man allocator;
} SkiplistDelayFreeContext;

typedef struct fdir_server_context {
    FDIRDentryContext dentry_context;
    SkiplistDelayFreeContext delay_free_context;
    int thread_index;
} FDIRServerContext;

typedef struct fdir_path_info {
    string_t ns;    //namespace
    string_t path;  //origin path
    string_t paths[FDIR_MAX_PATH_COUNT];   //splited path parts
    int count;
} FDIRPathInfo;

struct fdir_server_dentry;
typedef struct fdir_server_dentry_array {
    int alloc;
    int count;
    struct fdir_server_dentry **entries;
} FDIRServerDentryArray;

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

#endif

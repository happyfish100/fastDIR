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
} FDIRServerContext;

typedef struct server_task_arg {
    volatile int64_t task_version;
} FDIRServerTaskArg;

#endif

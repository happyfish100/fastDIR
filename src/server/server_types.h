#ifndef _FDIR_SERVER_TYPES_H
#define _FDIR_SERVER_TYPES_H

#include <time.h>
#include <pthread.h>
#include "fastcommon/common_define.h"
#include "fastcommon/common_blocked_queue.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fc_list.h"
#include "common/fdir_types.h"

#define FDIR_SERVER_DEFAULT_RELOAD_INTERVAL       500
#define FDIR_SERVER_DEFAULT_CHECK_ALIVE_INTERVAL  300

#define FDIR_SERVER_EVENT_TYPE_PUSH_CONFIG   1
#define FDIR_SERVER_EVENT_TYPE_ACTIVE_TEST   2

#define FDIR_SERVER_TASK_WAITING_REQUEST          0
#define FDIR_SERVER_TASK_WAITING_PUSH_RESP        1
#define FDIR_SERVER_TASK_WAITING_ACTIVE_TEST_RESP 2

#define FDIR_SERVER_TASK_WAITING_RESP (FDIR_SERVER_TASK_WAITING_PUSH_RESP | \
        FDIR_SERVER_TASK_WAITING_ACTIVE_TEST_RESP)

typedef struct server_context {
    struct common_blocked_queue push_queue;
} FDIRServerContext;

typedef struct fdir_env_publisher {
    char *env;
    int64_t current_version;
    struct fc_list_head head;   //subscribe task double chain
    pthread_mutex_t lock;

    struct {
        time_t last_reload_all_time;
        struct {
            int64_t total_count;
            int64_t last_count;
        } version_changed;
        bool reload_all;
    } config_stat;
} FDIREnvPublisher;

typedef struct fdir_config_message_queue {
    int64_t agent_cfg_version;
    int offset;
} FDIRConfigMessageQueue;

typedef struct server_task_arg {
    volatile int64_t task_version;

    struct fc_list_head subscribe;

    FDIRConfigMessageQueue msg_queue;

    int last_recv_pkg_time;
    short waiting_type;
    bool joined;
} FDIRServerTaskArg;

typedef struct server_push_event {
    struct fast_task_info *task;
    int64_t task_version;
    int type;
} FDIRServerPushEvent;

#endif

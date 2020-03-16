#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/process_ctrl.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "sf/sf_service.h"
#include "sf/sf_util.h"
#include "common/fdir_proto.h"
#include "common/fdir_types.h"
#include "server_types.h"
#include "server_func.h"
#include "dentry.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"
#include "inode_generator.h"
#include "server_binlog.h"
#include "data_thread.h"
#include "data_loader.h"
#include "service_handler.h"
#include "cluster_handler.h"

static bool daemon_mode = true;
static int setup_server_env(const char *config_filename);
static int setup_mblock_stat_task();

int main(int argc, char *argv[])
{
    char *config_filename;
    char *action;
    char g_pid_filename[MAX_PATH_SIZE];
    pthread_t schedule_tid;
    int wait_count;
    bool stop;
    int r;
    failvars;

    stop = false;
    if (argc < 2) {
        sf_usage(argv[0]);
        return 1;
    }
    config_filename = argv[1];
    log_init2();

    r = get_base_path_from_conf_file(config_filename, g_sf_global_vars.base_path,
                                     sizeof(g_sf_global_vars.base_path));
    gofailif(r, "base path error");

    snprintf(g_pid_filename, sizeof(g_pid_filename), 
             "%s/serverd.pid", g_sf_global_vars.base_path);

    sf_parse_daemon_mode_and_action(argc, argv, &daemon_mode, &action);
    r = process_action(g_pid_filename, action, &stop);
    if (r == EINVAL) {
        sf_usage(argv[0]);
        log_destroy();
        return 1;
    }
    gofailif(r, "process arg error");

    if (stop) {
        log_destroy();
        return 0;
    }

    srand(time(NULL));
    //fast_mblock_manager_init();

    //sched_set_delay_params(300, 1024);
    r = setup_server_env(config_filename);
    gofailif(r, "");

    r = sf_startup_schedule(&schedule_tid);
    gofailif(r, "");

    r = inode_generator_init();
    gofailif(r, "inode generator init error");

    r = sf_socket_server();
    gofailif(r, "service socket server error");

    r = sf_socket_server_ex(&CLUSTER_SF_CTX);
    gofailif(r, "cluster socket server error");

    r = write_to_pid_file(g_pid_filename);
    gofailif(r, "write pid error");

    r = dentry_init();
    gofailif(r, "dentry init error");

    r = service_handler_init();
    gofailif(r, "server handler init error");

    r = server_binlog_init();
    gofailif(r, "server binlog init error");

    r = data_thread_init();
    gofailif(r, "data thread init error");

    r = server_load_data();
    gofailif(r, "load data error");

    fdir_proto_init();

    r = cluster_top_init();
    gofailif(r, "cluster topology init error");

    r = cluster_relationship_init();
    gofailif(r, "cluster relationship init error");

    r = sf_service_init_ex(&CLUSTER_SF_CTX, cluster_alloc_thread_extra_data,
            cluster_thread_loop_callback, NULL, fdir_proto_set_body_length,
            cluster_deal_task, cluster_task_finish_cleanup, NULL,
            1000, sizeof(FDIRProtoHeader), sizeof(FDIRServerTaskArg));
    gofailif(r, "cluster service init error");
    sf_enable_thread_notify_ex(&CLUSTER_SF_CTX, true);
    sf_set_remove_from_ready_list_ex(&CLUSTER_SF_CTX, false);
    sf_enable_realloc_task_buffer_ex(&CLUSTER_SF_CTX, false);

    r = sf_service_init(server_alloc_thread_extra_data, NULL,
            NULL, fdir_proto_set_body_length, server_deal_task,
            server_task_finish_cleanup, NULL,
            1000, sizeof(FDIRProtoHeader), sizeof(FDIRServerTaskArg));
    gofailif(r, "server service init error");
    sf_set_remove_from_ready_list(false);

    setup_mblock_stat_task();

    sf_accept_loop_ex(&CLUSTER_SF_CTX, false);
    sf_accept_loop();
    if (g_schedule_flag) {
        pthread_kill(schedule_tid, SIGINT);
    }

    wait_count = 0;
    while ((SF_G_ALIVE_THREAD_COUNT != 0 || SF_ALIVE_THREAD_COUNT(
                    CLUSTER_SF_CTX) != 0) || g_schedule_flag)
    {
        usleep(10000);
        if (++wait_count > 1000) {
            lwarning("waiting timeout, exit!");
            break;
        }
    }

    inode_generator_destroy();
    server_binlog_terminate();
    sf_service_destroy();
    delete_pid_file(g_pid_filename);
    logInfo("file: "__FILE__", line: %d, "
            "program exit normally.\n", __LINE__);
    log_destroy();
    return 0;

FAIL_:
    logfail();
    lcrit("program exit abnomally");
    log_destroy();
    return eres;
}

static int mblock_stat_task_func(void *args)
{
    fast_mblock_manager_stat_print_ex(false, FAST_MBLOCK_ORDER_BY_ELEMENT_SIZE);
    return 0;
}

static int setup_mblock_stat_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    return 0;   //disabled for DEBUG

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 60,  mblock_stat_task_func, NULL);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

static int setup_server_env(const char *config_filename)
{
    int result;

    sf_set_current_time();

    if ((result=server_load_config(config_filename)) != 0) {
        return result;
    }

    if (daemon_mode) {
        daemon_init(false);
    }
    umask(0);

    result = sf_setup_signal_handler();

    log_set_cache(true);
    return result;
}

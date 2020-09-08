//cluster_info.h

#ifndef _CLUSTER_INFO_H_
#define _CLUSTER_INFO_H_

#include <time.h>
#include <pthread.h>
#include "server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_info_init(const char *cluster_config_filename);
int cluster_info_destroy();

FDIRClusterServerInfo *fdir_get_server_by_id(const int server_id);

int cluster_info_setup_sync_to_file_task();

static inline void cluster_info_set_status(FDIRClusterServerInfo *cs,
        const int new_status)
{
    int old_status;

    old_status = __sync_fetch_and_add(&cs->status, 0);
    if (old_status != new_status) {
        do  {
            if (__sync_bool_compare_and_swap(&cs->status,
                        old_status, new_status))
            {
                break;
            }
            old_status = __sync_add_and_fetch(&cs->status, 0);
        } while (old_status != new_status);
        __sync_add_and_fetch(&CLUSTER_SERVER_ARRAY.change_version, 1);
    }
}

#ifdef __cplusplus
}
#endif

#endif

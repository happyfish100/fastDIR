//cluster_info.h

#ifndef _CLUSTER_INFO_H_
#define _CLUSTER_INFO_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_info_init(const char *cluster_config_filename);
int cluster_info_destroy();

FDIRClusterServerInfo *fdir_get_server_by_id(const int server_id);

int cluster_info_write_to_file();

#ifdef __cplusplus
}
#endif

#endif

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

//cluster_relationship.h

#ifndef _CLUSTER_RELATIONSHIP_H_
#define _CLUSTER_RELATIONSHIP_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"
#include "cluster_info.h"

typedef struct fdir_cluster_server_status {
    FDIRClusterServerInfo *cs;
    char is_master;
    char master_hint;
    char status;
    char force_election;
    int server_id;
    int last_heartbeat_time;
    int up_time;
    int last_shutdown_time;
    int64_t data_version;
} FDIRClusterServerStatus;

#ifdef __cplusplus
extern "C" {
#endif

int cluster_relationship_init();
void cluster_relationship_destroy();

int cluster_relationship_pre_set_master(FDIRClusterServerInfo *master);

int cluster_relationship_commit_master(FDIRClusterServerInfo *master);

void cluster_relationship_trigger_reselect_master();

int cluster_relationship_master_quorum_check();

int cluster_proto_get_server_status(ConnectionInfo *conn,
        const int network_timeout,
        FDIRClusterServerStatus *server_status);

int cluster_add_to_detect_server_array(FDIRClusterServerInfo *cs);

int cluster_remove_from_detect_server_array(FDIRClusterServerInfo *cs);

#ifdef __cplusplus
}
#endif

#endif

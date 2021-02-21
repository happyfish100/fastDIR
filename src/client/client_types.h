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

#ifndef _FDIR_CLIENT_TYPES_H
#define _FDIR_CLIENT_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/connection_pool.h"
#include "sf/sf_configs.h"
#include "sf/sf_connection_manager.h"
#include "sf/idempotency/client/client_types.h"
#include "fdir_types.h"

struct fdir_client_context;

typedef struct fdir_client_server_entry {
    int server_id;
    ConnectionInfo conn;
    char status;
} FDIRClientServerEntry;

typedef struct fdir_server_group {
    int alloc_size;
    int count;
    ConnectionInfo *servers;
} FDIRServerGroup;

typedef struct fdir_client_session {
    struct fdir_client_context *ctx;
    ConnectionInfo *mconn;  //master connection
} FDIRClientSession;

typedef enum {
    conn_manager_type_simple = 1,
    conn_manager_type_pooled,
    conn_manager_type_other
} FDIRClientConnManagerType;

typedef struct fdir_client_context {
    FCServerConfig server_cfg;
    int cluster_group_index;
    int service_group_index;
    SFConnectionManager cm;
    FDIRClientConnManagerType conn_manager_type;
    bool cloned;
    bool idempotency_enabled;
    SFClientCommonConfig common_cfg;
} FDIRClientContext;

#endif

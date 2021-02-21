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

#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/idempotency/client/client_channel.h"
#include "fdir_proto.h"
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

static int connect_done_callback(ConnectionInfo *conn, void *args)
{
    SFConnectionParameters *params;
    int result;

    params = (SFConnectionParameters *)conn->args;
    if (((FDIRClientContext *)args)->idempotency_enabled) {
        params->channel = idempotency_client_channel_get(conn->ip_addr,
                conn->port, ((FDIRClientContext *)args)->common_cfg.
                connect_timeout, &result);
        if (params->channel == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "server %s:%u, idempotency channel get fail, "
                    "result: %d, error info: %s", __LINE__, conn->ip_addr,
                    conn->port, result, STRERROR(result));
            return result;
        }
    } else {
        params->channel = NULL;
    }

    result = fdir_client_proto_join_server((FDIRClientContext *)args, conn, params);
    if (result == SF_RETRIABLE_ERROR_NO_CHANNEL && params->channel != NULL) {
        idempotency_client_channel_check_reconnect(params->channel);
    }
    return result;
}

int fdir_pooled_connection_manager_init(FDIRClientContext *client_ctx,
        SFConnectionManager *cm, const int max_count_per_entry,
        const int max_idle_time)
{
    const int group_count = 1;
    int server_count;
    int result;

    server_count = FC_SID_SERVER_COUNT(client_ctx->server_cfg);
    if ((result=sf_connection_manager_init(cm, &client_ctx->common_cfg,
                    group_count, client_ctx->service_group_index,
                    server_count, max_count_per_entry, max_idle_time,
                    connect_done_callback, client_ctx)) != 0)
    {
        return result;
    }

    return 0;
}

void fdir_pooled_connection_manager_destroy(SFConnectionManager *cm)
{
}

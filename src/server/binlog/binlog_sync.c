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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/fc_atomic.h"
#include "sf/sf_func.h"
#include "../../common/fdir_proto.h"
#include "../server_global.h"
#include "../cluster_relationship.h"
#include "binlog_dump.h"
#include "binlog_sync.h"

typedef struct {
    int fd;
    int wait_count;

    struct {
        int start_index;
        int last_index;
    } dump_data;

    struct {
        int start_index;
        int last_index;
    } binlog;

    BufferInfo buffer;  //for network
} BinlogSyncContext;

static int query_binlog_info(ConnectionInfo *conn,
        BinlogSyncContext *sync_ctx)
{
    int result;
    FDIRProtoHeader *header;
    FDIRProtoReplicaQueryBinlogInfoReq *req;
    FDIRProtoReplicaQueryBinlogInfoResp resp;
    SFResponseInfo response;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoReplicaQueryBinlogInfoReq)];

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoReplicaQueryBinlogInfoReq *)
        (out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_MYSELF_PTR->server->id, req->server_id);
    SF_PROTO_SET_HEADER(header, FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
            sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
            FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP,
            (char *)&resp, sizeof(resp))) != 0)
    {
        fdir_log_network_error_ex(&response, conn, result, LOG_ERR);
        return result;
    }

    sync_ctx->dump_data.start_index = buff2int(resp.dump_data.start_index);
    sync_ctx->dump_data.last_index = buff2int(resp.dump_data.last_index);
    sync_ctx->binlog.start_index = buff2int(resp.binlog.start_index);
    sync_ctx->binlog.last_index = buff2int(resp.binlog.last_index);
    return 0;
}

static int sync_binlog_to_local(ConnectionInfo *conn,
        BinlogSyncContext *sync_ctx, const unsigned char req_cmd,
        char *out_buff, const int out_bytes, bool *is_last)
{
    int result;
    FDIRProtoHeader *header;
    SFResponseInfo response;

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, req_cmd, out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
            out_bytes, &response, SF_G_NETWORK_TIMEOUT,
            FDIR_REPLICA_PROTO_SYNC_BINLOG_RESP)) != 0)
    {
        fdir_log_network_error_ex(&response, conn, result, LOG_ERR);
        return result;
    }

    if (response.header.body_len == 0) {
        *is_last = true;
        return 0;
    }

    if (response.header.body_len > sync_ctx->buffer.alloc_size) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%u, response body length: %d is too large, "
                "the max body length is %d", __LINE__, conn->ip_addr,
                conn->port, response.header.body_len,
                sync_ctx->buffer.alloc_size);
        return EOVERFLOW;
    }

    if ((result=tcprecvdata_nb(conn->sock, sync_ctx->buffer.buff,
                    response.header.body_len, SF_G_NETWORK_TIMEOUT)) != 0)
    {
        response.error.length = snprintf(response.error.message,
                sizeof(response.error.message),
                "recv data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        fdir_log_network_error(&response, conn, result);
        return result;
    }

    if (fc_safe_write(sync_ctx->fd, sync_ctx->buffer.buff, response.
                header.body_len) != response.header.body_len)
    {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "write to file fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    return 0;
}

static inline int sync_binlog_first_to_local(ConnectionInfo *conn,
        BinlogSyncContext *sync_ctx, const char file_type,
        const int binlog_index, bool *is_last)
{
    FDIRProtoReplicaSyncBinlogFirstReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoReplicaSyncBinlogFirstReq)];

    req = (FDIRProtoReplicaSyncBinlogFirstReq *)
        (out_buff + sizeof(FDIRProtoHeader));
    req->file_type = file_type;
    int2buff(binlog_index, req->binlog_index);
    return sync_binlog_to_local(conn, sync_ctx,
            FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ,
            out_buff, sizeof(out_buff), is_last);
}

static inline int sync_binlog_next_to_local(ConnectionInfo *conn,
        BinlogSyncContext *sync_ctx, bool *is_last)
{
    char out_buff[sizeof(FDIRProtoHeader)];
    return sync_binlog_to_local(conn, sync_ctx,
            FDIR_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ,
            out_buff, sizeof(out_buff), is_last);
}

static int proto_sync_binlog(ConnectionInfo *conn,
        BinlogSyncContext *sync_ctx, const char file_type,
        const int binlog_index)
{
    int result;
    bool is_last;
    char tmp_filename[PATH_MAX];
    char full_filename[PATH_MAX];

    if (file_type == FDIR_PROTO_FILE_TYPE_DUMP) {
        fdir_get_dump_data_filename_ex(FDIR_RECOVERY_DUMP_SUBDIR_NAME,
                full_filename, sizeof(full_filename));
    } else {
        sf_binlog_writer_get_filename(DATA_PATH_STR,
                FDIR_RECOVERY_SUBDIR_NAME, binlog_index,
                full_filename, sizeof(full_filename));
    }

    snprintf(tmp_filename, sizeof(tmp_filename), "%s.tmp", full_filename);
    if ((sync_ctx->fd=open(tmp_filename, O_WRONLY |
                    O_CREAT | O_TRUNC, 0644)) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "open binlog file %s fail, errno: %d, error info: %s",
                __LINE__, tmp_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    is_last = false;
    if ((result=sync_binlog_first_to_local(conn, sync_ctx,
                    file_type, binlog_index, &is_last)) != 0)
    {
        close(sync_ctx->fd);
        return result;
    }

    while (!is_last) {
        if ((result=sync_binlog_next_to_local(conn,
                        sync_ctx, &is_last)) != 0)
        {
            break;
        }
    }

    close(sync_ctx->fd);
    if (result != 0) {
        return result;
    }

    if (rename(tmp_filename, full_filename) != 0) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, rename file "
                "%s to %s fail, errno: %d, error info: %s",
                __LINE__, tmp_filename, full_filename,
                result, STRERROR(result));
    }
    return result;
}

static int cmp_server_status(const void *p1, const void *p2)
{
    FDIRClusterServerStatus *status1;
    FDIRClusterServerStatus *status2;
    int sub;

    status1 = (FDIRClusterServerStatus *)p1;
    status2 = (FDIRClusterServerStatus *)p2;

    sub = (int)status1->is_master - (int)status2->is_master;
    if (sub != 0) {
        return sub;
    }

    if (status1->data_version < status2->data_version) {
        return -1;
    } else if (status1->data_version > status2->data_version) {
        return 1;
    }

    return 0;
}

static int get_master_connection(ConnectionInfo *conn, bool *need_rebuild)
{
#define FIXED_SERVER_COUNT  8
    int result;
    int count;
    FDIRClusterServerInfo *cs;
    FDIRClusterServerInfo *send;
    FDIRClusterServerStatus fixed[FIXED_SERVER_COUNT];
    FDIRClusterServerStatus *server_status;
    FDIRClusterServerStatus *ps;
    FDIRClusterServerStatus *last;

    if (CLUSTER_SERVER_ARRAY.count <= FIXED_SERVER_COUNT) {
        server_status = fixed;
    } else {
        server_status = fc_malloc(sizeof(FDIRClusterServerStatus) *
                CLUSTER_SERVER_ARRAY.count);
        if (server_status == NULL) {
            return ENOMEM;
        }
    }

    ps = server_status;
    result = ENOENT;
    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<send; cs++) {
        if (cs == CLUSTER_MYSELF_PTR) {
            continue;
        }

        if ((result=fc_server_make_connection_ex(
                        &CLUSTER_GROUP_ADDRESS_ARRAY(cs->server), conn,
                        "fdir", SF_G_CONNECT_TIMEOUT, NULL, true)) != 0)
        {
            continue;
        }

        ps->cs = cs;
        if ((result=cluster_proto_get_server_status(conn,
                        SF_G_NETWORK_TIMEOUT, ps)) == 0)
        {
            ps++;
        }
        conn_pool_disconnect_server(conn);
    }

    count = ps - server_status;
    do {
        if (count == 0) {
            result = ENOENT;
            break;
        }

        if (count > 1) {
            qsort(server_status, count,
                    sizeof(FDIRClusterServerStatus),
                    cmp_server_status);
        }

        last = ps - 1;
        if (last->data_version > 0) {
            if (last->is_master) {
                *need_rebuild = true;
                result = fc_server_make_connection_ex(
                        &CLUSTER_GROUP_ADDRESS_ARRAY(last->cs->server),
                        conn, "fdir", SF_G_CONNECT_TIMEOUT, NULL, true);
            } else {
                result = EAGAIN;
            }
            break;
        }

        if (count == CLUSTER_SERVER_ARRAY.count - 1) {
            *need_rebuild = false;
            result = 0;
        } else {
            result = EAGAIN;
        }
    } while (0);

    if (server_status != fixed) {
        free(server_status);
    }
    return result;
}

static int do_sync_binlogs(BinlogSyncContext *sync_ctx)
{
    int result;
    int sleep_seconds;
    bool need_rebuild;
    char file_type;
    int binlog_index;
    ConnectionInfo conn;

    conn.sock = -1;
    sleep_seconds = 1;
    while (SF_G_CONTINUE_FLAG) {
        if ((result=get_master_connection(&conn, &need_rebuild)) == 0) {
            break;
        }

        sleep(sleep_seconds);
        if (sleep_seconds < 30) {
            sleep_seconds *= 2;
        }
    }

    if (!SF_G_CONTINUE_FLAG) {
        return EINTR;
    }

    if (!need_rebuild) {
        return 0;
    }

    if ((result=query_binlog_info(&conn, sync_ctx)) != 0) {
        conn_pool_disconnect_server(&conn);
        return result;
    }

    /*
    for (binlog_index=start_index; binlog_index<=last_index; binlog_index++) {
        if ((result=proto_sync_binlog(&conn, sync_ctx, binlog_index)) != 0)
        {
            break;
        }
    }
    */

    if (result == 0) {
        /*
        if ((result=replica_binlog_set_binlog_start_index(
                        ctx->ds->dg->id, start_index)) == 0)
        {
            result = replica_binlog_writer_change_write_index(
                    ctx->ds->dg->id, last_index);
        }
        */
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

int data_recovery_sync_binlog()
{
    int result;
    BinlogSyncContext sync_ctx;

    if (CLUSTER_SERVER_ARRAY.count == 1) {
        return 0;
    }

    memset(&sync_ctx, 0, sizeof(sync_ctx));
    if ((result=fc_init_buffer(&sync_ctx.buffer, 256 * 1024)) != 0) {
        return result;
    }

    result = do_sync_binlogs(&sync_ctx);
    fc_free_buffer(&sync_ctx.buffer);
    return result;
}

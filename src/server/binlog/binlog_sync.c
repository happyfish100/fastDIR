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
#include "../server_func.h"
#include "../cluster_relationship.h"
#include "binlog_dump.h"
#include "binlog_write.h"
#include "binlog_sync.h"

typedef struct {
    bool remove_dump_data;
    char file_type;
    int binlog_index;
    int fd;
    ConnectionInfo conn;

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

static int query_binlog_info(BinlogSyncContext *sync_ctx)
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
    if ((result=sf_send_and_recv_response(&sync_ctx->conn, out_buff,
            sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
            FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP,
            (char *)&resp, sizeof(resp))) != 0)
    {
        fdir_log_network_error_ex(&response,
                &sync_ctx->conn, result, LOG_ERR);
        return result;
    }

    sync_ctx->dump_data.start_index = buff2int(resp.dump_data.start_index);
    sync_ctx->dump_data.last_index = buff2int(resp.dump_data.last_index);
    sync_ctx->binlog.start_index = buff2int(resp.binlog.start_index);
    sync_ctx->binlog.last_index = buff2int(resp.binlog.last_index);
    sync_ctx->remove_dump_data = resp.remove_dump_data;
    return 0;
}

static int sync_binlog_report(BinlogSyncContext *sync_ctx, const char stage)
{
    int result;
    FDIRProtoHeader *header;
    FDIRProtoReplicaSyncBinlogReportReq *req;
    SFResponseInfo response;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoReplicaSyncBinlogReportReq)];

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoReplicaSyncBinlogReportReq *)
        (out_buff + sizeof(FDIRProtoHeader));
    SF_PROTO_SET_HEADER(header, FDIR_REPLICA_PROTO_SYNC_BINLOG_REPORT,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    SERVER_PROTO_PACK_IDENTITY(req->si);
    req->remove_dump_data = sync_ctx->remove_dump_data;
    req->stage = stage;
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(&sync_ctx->conn,
                    out_buff, sizeof(out_buff), &response,
                    SF_G_NETWORK_TIMEOUT, SF_PROTO_ACK)) != 0)
    {
        fdir_log_network_error_ex(&response,
                &sync_ctx->conn, result, LOG_ERR);
    }

    return result;
}

static int sync_binlog_to_local(BinlogSyncContext *sync_ctx,
        const unsigned char req_cmd, char *out_buff,
        const int out_bytes, bool *is_last)
{
    int result;
    int count;
    int sleep_seconds;
    int64_t start_time_ms;
    char time_buff[32];
    FDIRProtoHeader *header;
    SFResponseInfo response;

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, req_cmd, out_bytes - sizeof(FDIRProtoHeader));
    count = 0;
    start_time_ms = get_current_time_ms();
    sleep_seconds = 1;
    while (SF_G_CONTINUE_FLAG) {
        response.error.length = 0;
        result = sf_send_and_check_response_header(&sync_ctx->conn,
                out_buff, out_bytes, &response, SF_G_NETWORK_TIMEOUT,
                FDIR_REPLICA_PROTO_SYNC_BINLOG_RESP);
        if (result != 0) {
            if (req_cmd == FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ
                    && sync_ctx->file_type == FDIR_PROTO_FILE_TYPE_DUMP
                    && result == EAGAIN)
            {
                if (++count == 1) {
                    logInfo("file: "__FILE__", line: %d, "
                            "waiting for full dump data done ...",
                            __LINE__);
                }
                sleep(sleep_seconds);
                if (sleep_seconds < 60) {
                    sleep_seconds *= 2;
                }
                continue;
            }

            fdir_log_network_error_ex(&response,
                    &sync_ctx->conn, result, LOG_ERR);
            return result;
        }

        break;
    }

    if (!SF_G_CONTINUE_FLAG) {
        return EINTR;
    }

    if (count > 0) {
        long_to_comma_str(get_current_time_ms() - start_time_ms, time_buff);
        logInfo("file: "__FILE__", line: %d, "
                "wait full dump data done, time used: %s ms.",
                __LINE__, time_buff);
    }

    if (response.header.body_len == 0) {
        *is_last = true;
        return 0;
    }

    if (response.header.body_len > sync_ctx->buffer.alloc_size) {
        if ((result=fc_realloc_buffer(&sync_ctx->buffer, 1024,
                        response.header.body_len)) != 0)
        {
            return result;
        }
    }

    if ((result=tcprecvdata_nb(sync_ctx->conn.sock, sync_ctx->buffer.buff,
                    response.header.body_len, SF_G_NETWORK_TIMEOUT)) != 0)
    {
        response.error.length = snprintf(response.error.message,
                sizeof(response.error.message),
                "recv data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        fdir_log_network_error(&response, &sync_ctx->conn, result);
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

static inline int sync_binlog_first_to_local(
        BinlogSyncContext *sync_ctx, bool *is_last)
{
    FDIRProtoReplicaSyncBinlogFirstReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoReplicaSyncBinlogFirstReq)];

    req = (FDIRProtoReplicaSyncBinlogFirstReq *)
        (out_buff + sizeof(FDIRProtoHeader));
    req->file_type = sync_ctx->file_type;
    int2buff(sync_ctx->binlog_index, req->binlog_index);
    return sync_binlog_to_local(sync_ctx,
            FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ,
            out_buff, sizeof(out_buff), is_last);
}

static inline int sync_binlog_next_to_local(
        BinlogSyncContext *sync_ctx, bool *is_last)
{
    char out_buff[sizeof(FDIRProtoHeader)];
    return sync_binlog_to_local(sync_ctx,
            FDIR_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ,
            out_buff, sizeof(out_buff), is_last);
}

static int proto_sync_binlog(BinlogSyncContext *sync_ctx)
{
    int result;
    bool is_last;
    char full_filename[PATH_MAX];

    if (sync_ctx->file_type == FDIR_PROTO_FILE_TYPE_DUMP) {
        fdir_get_dump_data_filename_ex(FDIR_RECOVERY_DUMP_SUBDIR_NAME,
                full_filename, sizeof(full_filename));
    } else {
        sf_binlog_writer_get_filename(DATA_PATH_STR,
                FDIR_RECOVERY_SUBDIR_NAME, sync_ctx->binlog_index,
                full_filename, sizeof(full_filename));
    }

    if ((sync_ctx->fd=open(full_filename, O_WRONLY |
                    O_CREAT | O_TRUNC, 0644)) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "open binlog file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    is_last = false;
    if ((result=sync_binlog_first_to_local(sync_ctx, &is_last)) != 0) {
        close(sync_ctx->fd);
        return result;
    }

    while (!is_last && SF_G_CONTINUE_FLAG) {
        if ((result=sync_binlog_next_to_local(sync_ctx, &is_last)) != 0) {
            break;
        }
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    close(sync_ctx->fd);
    return result;
}

static int proto_sync_mark_file(BinlogSyncContext *sync_ctx)
{
    int result;
    char out_buff[sizeof(FDIRProtoHeader)];
    char mark_filename[PATH_MAX];
    FDIRProtoHeader *header;
    SFResponseInfo response;

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    result = sf_send_and_check_response_header(&sync_ctx->conn, out_buff,
            sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
            FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_RESP);
    if (result != 0) {
        fdir_log_network_error_ex(&response,
                &sync_ctx->conn, result, LOG_ERR);
        return result;
    }

    if (response.header.body_len > sync_ctx->buffer.alloc_size) {
        if ((result=fc_realloc_buffer(&sync_ctx->buffer, 1024,
                        response.header.body_len)) != 0)
        {
            return result;
        }
    }

    if ((result=tcprecvdata_nb(sync_ctx->conn.sock, sync_ctx->buffer.buff,
                    response.header.body_len, SF_G_NETWORK_TIMEOUT)) != 0)
    {
        response.error.length = snprintf(response.error.message,
                sizeof(response.error.message),
                "recv data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        fdir_log_network_error(&response, &sync_ctx->conn, result);
        return result;
    }

    fdir_get_dump_mark_filename_ex(FDIR_RECOVERY_DUMP_SUBDIR_NAME,
            mark_filename, sizeof(mark_filename));
    if ((result=safeWriteToFile(mark_filename, sync_ctx->buffer.buff,
                    response.header.body_len)) != 0)
    {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "write to file %s fail, errno: %d, error info: %s",
                __LINE__, mark_filename, result, STRERROR(result));
        return result;
    }

    return 0;
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

static int get_master_connection(ConnectionInfo *conn, int *master_id)
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

    *master_id = 0;
    if (CLUSTER_SERVER_ARRAY.count <= FIXED_SERVER_COUNT) {
        server_status = fixed;
    } else {
        server_status = fc_malloc(sizeof(FDIRClusterServerStatus) *
                CLUSTER_SERVER_ARRAY.count);
        if (server_status == NULL) {
            return ENOMEM;
        }
    }

    result = ENOENT;
    ps = server_status;
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
                *master_id = last->cs->server->id;
                result = fc_server_make_connection_ex(
                        &CLUSTER_GROUP_ADDRESS_ARRAY(last->cs->server),
                        conn, "fdir", SF_G_CONNECT_TIMEOUT, NULL, true);
            } else {
                result = EAGAIN;
            }
            break;
        } else if (count == CLUSTER_SERVER_ARRAY.count - 1) {
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

static int check_mkdirs()
{
    int result;
    char filepath[PATH_MAX];

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_RECOVERY_SUBDIR_NAME,
            filepath, sizeof(filepath));
    if ((result=fc_check_mkdir(filepath, 0775)) != 0) {
        return result;
    }

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_RECOVERY_DUMP_SUBDIR_NAME,
            filepath, sizeof(filepath));
    return fc_check_mkdir(filepath, 0775);
}

static int clean_binlog_path()
{
    const int binlog_index = 0;
    int result;
    char filename[PATH_MAX];
    char filepath[PATH_MAX];

    sf_file_writer_get_filename(DATA_PATH_STR, FDIR_BINLOG_SUBDIR_NAME,
            binlog_index, filename, sizeof(filename));
    if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
        return result;
    }

    sf_file_writer_get_index_filename(DATA_PATH_STR,
            FDIR_BINLOG_SUBDIR_NAME, filename, sizeof(filename));
    if ((result=fc_delete_file_ex(filename, "index")) != 0) {
        return result;
    }

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_BINLOG_SUBDIR_NAME,
            filepath, sizeof(filepath));
    if (fc_get_path_child_count(filepath) == 0) {
        return 0;
    } else {
        logError("file: "__FILE__", line: %d, "
                "binlog path %s not emtpy!",
                __LINE__, filepath);
        return ENOTEMPTY;
    }
}

static int sync_finish(BinlogSyncContext *sync_ctx)
{
    int result;
    char index_filename[PATH_MAX];
    char recovery_filename[PATH_MAX];
    char recovery_path[PATH_MAX];
    char binlog_path[PATH_MAX];

    if ((result=binlog_writer_set_indexes(sync_ctx->binlog.start_index,
                    sync_ctx->binlog.last_index)) != 0)
    {
        return result;
    }

    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            FDIR_BINLOG_SUBDIR_NAME, index_filename,
            sizeof(index_filename));
    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            FDIR_RECOVERY_SUBDIR_NAME, recovery_filename,
            sizeof(recovery_filename));
    if (rename(index_filename, recovery_filename) != 0) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "rename file %s to %s fail, errno: %d, error info: %s",
                __LINE__, index_filename, recovery_filename,
                result, STRERROR(result));
        return result;
    }

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_RECOVERY_SUBDIR_NAME,
            recovery_path, sizeof(recovery_path));
    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_BINLOG_SUBDIR_NAME,
            binlog_path, sizeof(binlog_path));
    if (rename(recovery_path, binlog_path) != 0) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "rename path %s to %s fail, errno: %d, error info: %s",
                __LINE__, recovery_path, binlog_path,
                result, STRERROR(result));
        return result;
    }

    return binlog_writer_change_write_index(sync_ctx->binlog.last_index);
}

static int do_sync_binlogs(BinlogSyncContext *sync_ctx)
{
    int result;
    int sleep_seconds;
    int master_id;
    int64_t start_time_ms;
    char time_buff[32];

    master_id = -1;
    sync_ctx->conn.sock = -1;
    sleep_seconds = 1;
    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "try to get master connection to "
            "fetch binlog ...", __LINE__);
    while (SF_G_CONTINUE_FLAG) {
        if ((result=get_master_connection(&sync_ctx->
                        conn, &master_id)) == 0)
        {
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

    if (master_id == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "do NOT need fetch binlog from "
                "other fdir server.", __LINE__);
        return 0;
    }

    if ((result=clean_binlog_path()) != 0) {
        return result;
    }
    if ((result=check_mkdirs()) != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "fetch binlogs from master server id: %d, %s:%u ...",
            __LINE__, master_id, sync_ctx->conn.ip_addr,
            sync_ctx->conn.port);

    if ((result=query_binlog_info(sync_ctx)) != 0) {
        conn_pool_disconnect_server(&sync_ctx->conn);
        return result;
    }

    result = sync_binlog_report(sync_ctx, FDIR_PROTO_SYNC_BINLOG_STAGE_START);
    if (result != 0) {
        conn_pool_disconnect_server(&sync_ctx->conn);
        return result;
    }

    if (sync_ctx->dump_data.start_index <= sync_ctx->dump_data.last_index) {
        sync_ctx->file_type = FDIR_PROTO_FILE_TYPE_DUMP;
        sync_ctx->binlog_index = 0;
        if ((result=proto_sync_binlog(sync_ctx)) != 0) {
            return result;
        }

        if ((result=proto_sync_mark_file(sync_ctx)) != 0) {
            return result;
        }
    }

    for (sync_ctx->binlog_index=sync_ctx->binlog.start_index;
            sync_ctx->binlog_index<= sync_ctx->binlog.last_index;
            sync_ctx->binlog_index++)
    {
        sync_ctx->file_type = FDIR_PROTO_FILE_TYPE_BINLOG;
        if ((result=proto_sync_binlog(sync_ctx)) != 0) {
            return result;
        }
    }

    result = sync_binlog_report(sync_ctx, FDIR_PROTO_SYNC_BINLOG_STAGE_END);
    conn_pool_disconnect_server(&sync_ctx->conn);
    if (result != 0) {
        return result;
    }

    long_to_comma_str(get_current_time_ms() - start_time_ms, time_buff);
    logInfo("file: "__FILE__", line: %d, "
            "fetch binlogs from master server id: %d done, "
            "time used: %s ms.", __LINE__, master_id, time_buff);

    return sync_finish(sync_ctx);
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

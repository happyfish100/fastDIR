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
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_func.h"
#include "common/fdir_proto.h"
#include "server_global.h"
#include "server_binlog.h"
#include "data_thread.h"
#include "inode_generator.h"
#include "cluster_relationship.h"

FDIRClusterServerInfo *g_next_master = NULL;

typedef struct fdir_cluster_server_status {
    FDIRClusterServerInfo *cs;
    bool is_master;
    char status;
    int server_id;
    int64_t data_version;
} FDIRClusterServerStatus;

static int proto_get_server_status(ConnectionInfo *conn,
        const int network_timeout,
        FDIRClusterServerStatus *server_status)
{
	int result;
	FDIRProtoHeader *header;
    FDIRProtoGetServerStatusReq *req;
    FDIRProtoGetServerStatusResp *resp;
    SFResponseInfo response;
	char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoGetServerStatusReq)];
	char in_body[sizeof(FDIRProtoGetServerStatusResp)];

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoGetServerStatusReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->config_sign, CLUSTER_CONFIG_SIGN_BUF, SF_CLUSTER_CONFIG_SIGN_LEN);

    response.error.length = 0;
	if ((result=sf_send_and_check_response_header(conn, out_buff,
			sizeof(out_buff), &response, network_timeout,
            FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP)) != 0)
    {
        sf_log_network_error(&response, conn, result);
        return result;
    }

    if (response.header.body_len != sizeof(FDIRProtoGetServerStatusResp)) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%u, recv body length: %d != %d",
                __LINE__, conn->ip_addr, conn->port,
                response.header.body_len,
                (int)sizeof(FDIRProtoGetServerStatusResp));
        return EINVAL;
    }

    if ((result=tcprecvdata_nb(conn->sock, in_body, response.
                    header.body_len, network_timeout)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "recv from server %s:%u fail, "
                "errno: %d, error info: %s",
                __LINE__, conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    resp = (FDIRProtoGetServerStatusResp *)in_body;
    server_status->is_master = resp->is_master;
    server_status->status = resp->status;
    server_status->server_id = buff2int(resp->server_id);
    server_status->data_version = buff2long(resp->data_version);
    return 0;
}

static inline void generate_replica_key()
{
    static int64_t key_number_sn = 0;
    int64_t n1, n2;

    n1 = (((int64_t)CLUSTER_MY_SERVER_ID) << 48) |
        (((int64_t)getpid()) << 32) | (++key_number_sn);
    n2 = (((int64_t)g_current_time) << 32) | rand();
    long2buff(n1 ^ n2, REPLICA_KEY_BUFF);
}

static int proto_join_master(ConnectionInfo *conn, const int network_timeout)
{
	int result;
	FDIRProtoHeader *header;
    FDIRProtoJoinMasterReq *req;
    SFResponseInfo response;
	char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoJoinMasterReq)];

    generate_replica_key();
    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_CLUSTER_PROTO_JOIN_MASTER,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoJoinMasterReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_ID, req->cluster_id);
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->key, REPLICA_KEY_BUFF, FDIR_REPLICA_KEY_SIZE);
    memcpy(req->config_sign, CLUSTER_CONFIG_SIGN_BUF,
            SF_CLUSTER_CONFIG_SIGN_LEN);
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, network_timeout,
                    SF_PROTO_ACK)) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

static int proto_ping_master(ConnectionInfo *conn, const int network_timeout)
{
    FDIRProtoHeader header;
    SFResponseInfo response;
    char in_buff[8 * 1024];
    FDIRProtoPingMasterRespHeader *body_header;
    FDIRProtoPingMasterRespBodyPart *body_part;
    FDIRProtoPingMasterRespBodyPart *body_end;
    FDIRClusterServerInfo *cs;
    int64_t inode_sn;
    int server_count;
    int server_id;
    int result;

    SF_PROTO_SET_HEADER(&header, FDIR_CLUSTER_PROTO_PING_MASTER_REQ, 0);

    response.error.length = 0;
    if ((result=sf_send_and_check_response_header(conn, (char *)&header,
                    sizeof(header), &response, network_timeout,
                    FDIR_CLUSTER_PROTO_PING_MASTER_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(in_buff)) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d is too large",
                    response.header.body_len);
            result = EOVERFLOW;
        } else {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, network_timeout);
        }
    }

    body_header = (FDIRProtoPingMasterRespHeader *)in_buff;
    if (result == 0) {
        int calc_size;
        server_count = buff2int(body_header->server_count);
        calc_size = sizeof(FDIRProtoPingMasterRespHeader) +
            server_count * sizeof(FDIRProtoPingMasterRespBodyPart);
        if (calc_size != response.header.body_len) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d != calculate size: %d, "
                    "server count: %d", response.header.body_len,
                    calc_size, server_count);
            result = EINVAL;
        }
    } else {
        server_count = 0;
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
        return result;
    }

    inode_sn = buff2long(body_header->inode_sn);
    if (inode_sn > CURRENT_INODE_SN) {
        CURRENT_INODE_SN = inode_sn;
    }
    if (server_count == 0) {
        return 0;
    }

    body_part = (FDIRProtoPingMasterRespBodyPart *)(in_buff +
            sizeof(FDIRProtoPingMasterRespHeader));
    body_end = body_part + server_count;
    for (; body_part < body_end; body_part++) {
        server_id = buff2int(body_part->server_id);
        if ((cs=fdir_get_server_by_id(server_id)) != NULL) {
            cluster_info_set_status(cs, body_part->status);
        }
    }

    return 0;
}

static int cluster_cmp_server_status(const void *p1, const void *p2)
{
    FDIRClusterServerStatus *status1;
    FDIRClusterServerStatus *status2;
    int sub;

    status1 = (FDIRClusterServerStatus *)p1;
    status2 = (FDIRClusterServerStatus *)p2;
    if (status1->data_version < status2->data_version) {
        return -1;
    } else if (status1->data_version > status2->data_version) {
        return 1;
    }

    sub = (int)status1->is_master - (int)status2->is_master;
    if (sub != 0) {
        return sub;
    }

    sub = (int)status1->status - (int)status2->status;
    if (sub != 0) {
        return sub;
    }
    return (int)status1->server_id - (int)status2->server_id;
}

static int cluster_get_server_status(FDIRClusterServerStatus *server_status,
        const bool log_connect_error)
{
    const int connect_timeout = 2;
    const int network_timeout = 2;
    ConnectionInfo conn;
    int result;

    if (server_status->cs == CLUSTER_MYSELF_PTR) {
        server_status->is_master = MYSELF_IS_MASTER;
        server_status->status = __sync_fetch_and_add(
                &CLUSTER_MYSELF_PTR->status, 0);
        server_status->server_id = CLUSTER_MY_SERVER_ID;
        server_status->data_version = DATA_CURRENT_VERSION;
        return 0;
    } else {
        if ((result=fc_server_make_connection_ex(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            server_status->cs->server), &conn,
                        connect_timeout, NULL, log_connect_error)) != 0)
        {
            return result;
        }

        result = proto_get_server_status(&conn,
                network_timeout, server_status);
        conn_pool_disconnect_server(&conn);
        return result;
    }
}

static int cluster_get_master(FDIRClusterServerStatus *server_status,
        const bool log_connect_error, int *active_count)
{
#define STATUS_ARRAY_FIXED_COUNT  8
	FDIRClusterServerInfo *server;
	FDIRClusterServerInfo *end;
	FDIRClusterServerStatus *current_status;
	FDIRClusterServerStatus *cs_status;
	FDIRClusterServerStatus status_array[STATUS_ARRAY_FIXED_COUNT];
	int result;
	int r;
	int i;

	memset(server_status, 0, sizeof(FDIRClusterServerStatus));
    if (CLUSTER_SERVER_ARRAY.count < STATUS_ARRAY_FIXED_COUNT) {
        cs_status = status_array;
    } else {
        int bytes;
        bytes = sizeof(FDIRClusterServerStatus) * CLUSTER_SERVER_ARRAY.count;
        cs_status = (FDIRClusterServerStatus *)fc_malloc(bytes);
        if (cs_status == NULL) {
            return ENOMEM;
        }
    }

	current_status = cs_status;
	result = 0;
	end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<end; server++) {
		current_status->cs = server;
        r = cluster_get_server_status(current_status, log_connect_error);
		if (r == 0) {
			current_status++;
		} else if (r != ENOENT) {
			result = r;
		}
	}

	*active_count = current_status - cs_status;
    if (*active_count == 0) {
        logError("file: "__FILE__", line: %d, "
                "get server status fail, "
                "server count: %d", __LINE__,
                CLUSTER_SERVER_ARRAY.count);
        return result == 0 ? ENOENT : result;
    }

	qsort(cs_status, *active_count, sizeof(FDIRClusterServerStatus),
		cluster_cmp_server_status);

	for (i=0; i<*active_count; i++) {
        logDebug("file: "__FILE__", line: %d, "
                "server_id: %d, ip addr %s:%u, is_master: %d, "
                "status: %d(%s), data_version: %"PRId64, __LINE__,
                cs_status[i].server_id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs_status[i].cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs_status[i].cs->server),
                cs_status[i].is_master, cs_status[i].status,
                fdir_get_server_status_caption(cs_status[i].status),
                cs_status[i].data_version);
    }

	memcpy(server_status, cs_status + (*active_count - 1),
			sizeof(FDIRClusterServerStatus));
    if (cs_status != status_array) {
        free(cs_status);
    }
	return 0;
}

static int do_notify_master_changed(FDIRClusterServerInfo *cs,
		FDIRClusterServerInfo *master, const unsigned char cmd,
        bool *bConnectFail)
{
    char out_buff[sizeof(FDIRProtoHeader) + 4];
    ConnectionInfo conn;
    FDIRProtoHeader *header;
    SFResponseInfo response;
    int result;

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        cs->server), &conn, SF_G_CONNECT_TIMEOUT)) != 0)
    {
        *bConnectFail = true;
        return result;
    }
    *bConnectFail = false;

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, cmd, sizeof(out_buff) -
            sizeof(FDIRProtoHeader));
    int2buff(master->server->id, out_buff + sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(&conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    SF_PROTO_ACK)) != 0)
    {
        sf_log_network_error(&response, &conn, result);
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

int cluster_relationship_pre_set_master(FDIRClusterServerInfo *master)
{
    FDIRClusterServerInfo *next_master;

    next_master = g_next_master;
    if (next_master == NULL) {
        g_next_master = master;
    } else if (next_master != master) {
        logError("file: "__FILE__", line: %d, "
                "try to set next master id: %d, "
                "but next master: %d already exist",
                __LINE__, master->server->id, next_master->server->id);
        g_next_master = NULL;
        return EEXIST;
    }

    return 0;
}

static inline void cluster_unset_master()
{
    FDIRClusterServerInfo *old_master;

    old_master = CLUSTER_MASTER_ATOM_PTR;
    if (old_master != NULL) {
        if (__sync_bool_compare_and_swap(&CLUSTER_MASTER_PTR,
                    old_master, NULL))
        {
            __sync_bool_compare_and_swap(&old_master->is_master, 1, 0);
        }
    }
}

static void update_field_is_master(FDIRClusterServerInfo *new_master)
{
    FDIRClusterServerInfo *server;
    FDIRClusterServerInfo *send;
    int old_value;
    int new_value;

    send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (server=CLUSTER_SERVER_ARRAY.servers; server<send; server++) {
        old_value = __sync_add_and_fetch(&server->is_master, 0);
        new_value = (server == new_master ? 1 : 0);
        if (new_value != old_value) {
            __sync_bool_compare_and_swap(&server->is_master,
                    old_value, new_value);
        }
    }
}

static int cluster_relationship_set_master(FDIRClusterServerInfo *new_master,
        const time_t start_time)
{
    int result;
    int old_status;
    FDIRClusterServerInfo *old_master;

    old_master = CLUSTER_MASTER_ATOM_PTR;
    if (new_master == old_master) {
        logDebug("file: "__FILE__", line: %d, "
                "the server id: %d, ip %s:%u already is master",
                __LINE__, new_master->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(new_master->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(new_master->server));
        return 0;
    }

    if (CLUSTER_MYSELF_PTR == new_master) {
        inode_generator_skip();  //skip SN avoid conflict

        if ((result=binlog_producer_init()) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "binlog_producer_init fail, "
                    "program exit!", __LINE__);
            sf_terminate_myself();
            return result;
        }

        g_data_thread_vars.error_mode = FDIR_DATA_ERROR_MODE_STRICT;
        binlog_write_set_order_by(SF_BINLOG_WRITER_TYPE_ORDER_BY_VERSION);
        binlog_write_set_next_version();

        old_status = __sync_add_and_fetch(&new_master->status, 0);
        while (old_status != FDIR_SERVER_STATUS_ACTIVE) {
            if (__sync_bool_compare_and_swap(&new_master->status,
                        old_status, FDIR_SERVER_STATUS_ACTIVE))
            {
                break;
            }
            old_status = __sync_add_and_fetch(&new_master->status, 0);
        }
    } else {
        char time_used[128];
        if (start_time > 0) {
            sprintf(time_used, ", election time used: %ds",
                    (int)(g_current_time - start_time));
        } else {
            *time_used = '\0';
        }

        logInfo("file: "__FILE__", line: %d, "
                "the master server id: %d, ip %s:%u%s",
                __LINE__, new_master->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(new_master->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(new_master->server),
                time_used);
    }

    do {
        if (__sync_bool_compare_and_swap(&CLUSTER_MASTER_PTR,
                    old_master, new_master))
        {
            break;
        }
        old_master = CLUSTER_MASTER_ATOM_PTR;
    } while (old_master != new_master);
    update_field_is_master(new_master);

    if (CLUSTER_MYSELF_PTR == new_master) {
        if ((result=binlog_producer_start()) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "binlog_producer_start fail, "
                    "program exit!", __LINE__);
            sf_terminate_myself();
            return result;
        }

        binlog_local_consumer_replication_start();
    }

    __sync_add_and_fetch(&CLUSTER_SERVER_ARRAY.change_version, 1);
    return 0;
}

int cluster_relationship_commit_master(FDIRClusterServerInfo *master)
{
    const time_t start_time = 0;
    FDIRClusterServerInfo *next_master;
    int result;

    next_master = g_next_master;
    if (next_master == NULL) {
        logError("file: "__FILE__", line: %d, "
                "next master is NULL", __LINE__);
        return EBUSY;
    }
    if (next_master != master) {
        logError("file: "__FILE__", line: %d, "
                "next master server id: %d != expected server id: %d",
                __LINE__, next_master->server->id, master->server->id);
        g_next_master = NULL;
        return EBUSY;
    }

    result = cluster_relationship_set_master(master, start_time);
    g_next_master = NULL;
    return result;
}

void cluster_relationship_trigger_reselect_master()
{
    struct nio_thread_data *thread_data;
    struct nio_thread_data *data_end;

    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR) {
        return;
    }

    g_data_thread_vars.error_mode = FDIR_DATA_ERROR_MODE_LOOSE;
    binlog_write_set_order_by(SF_BINLOG_WRITER_TYPE_ORDER_BY_NONE);

    cluster_unset_master();
    __sync_add_and_fetch(&CLUSTER_SERVER_ARRAY.change_version, 1);

    data_end = CLUSTER_SF_CTX.thread_data + CLUSTER_SF_CTX.work_threads;
    for (thread_data=CLUSTER_SF_CTX.thread_data;
            thread_data<data_end; thread_data++)
    {
        ((FDIRServerContext *)thread_data->arg)->
            cluster.clean_connected_replicas = true;
    }
    binlog_producer_destroy();
}

static int cluster_notify_next_master(FDIRClusterServerInfo *cs,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    FDIRClusterServerInfo *master;
    master = server_status->cs;
    if (cs == CLUSTER_MYSELF_PTR) {
        return cluster_relationship_pre_set_master(master);
    } else {
        return do_notify_master_changed(cs, master,
                FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_commit_next_master(FDIRClusterServerInfo *cs,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    FDIRClusterServerInfo *master;

    master = server_status->cs;
    if (cs == CLUSTER_MYSELF_PTR) {
        return cluster_relationship_commit_master(master);
    } else {
        return do_notify_master_changed(cs, master,
                FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_notify_master_changed(FDIRClusterServerStatus *server_status)
{
	FDIRClusterServerInfo *server;
	FDIRClusterServerInfo *send;
	int result;
	bool bConnectFail;
	int success_count;

	result = ENOENT;
	send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	success_count = 0;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<send; server++) {
		if ((result=cluster_notify_next_master(server,
				server_status, &bConnectFail)) != 0)
		{
			if (!bConnectFail) {
				return result;
			}
		} else {
			success_count++;
		}
	}

	if (success_count == 0) {
		return result;
	}

	result = ENOENT;
	success_count = 0;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<send; server++) {
		if ((result=cluster_commit_next_master(server,
				server_status, &bConnectFail)) != 0)
		{
			if (!bConnectFail) {
				return result;
			}
		} else {
			success_count++;
		}
	}

	if (success_count == 0) {
		return result;
	}

	return 0;
}

static int cluster_select_master()
{
	int result;
    int active_count;
    int i;
    int max_sleep_secs;
    int sleep_secs;
    int remain_time;
    bool need_log;
    bool force_sleep;
    time_t start_time;
    time_t last_log_time;
    char status_prompt[512];
	FDIRClusterServerStatus server_status;
    FDIRClusterServerInfo *next_master;

	logInfo("file: "__FILE__", line: %d, "
		"selecting master...", __LINE__);

    start_time = g_current_time;
    last_log_time = 0;
    sleep_secs = 10;
    max_sleep_secs = 1;
    i = 0;
    while (CLUSTER_MASTER_ATOM_PTR == NULL) {
        if (sleep_secs > 1) {
            need_log = true;
            last_log_time = g_current_time;
        } else if (g_current_time - last_log_time > 8) {
            need_log = ((i + 1) % 10 == 0);
            if (need_log) {
                last_log_time = g_current_time;
            }
        } else {
            need_log = false;
        }

        if ((result=cluster_get_master(&server_status,
                        need_log, &active_count)) != 0)
        {
            return result;
        }

        ++i;
        if (!sf_election_quorum_check(MASTER_ELECTION_QUORUM,
                    CLUSTER_SERVER_ARRAY.count, active_count) &&
                !FORCE_MASTER_ELECTION)
        {
            sleep_secs = 1;
            if (need_log) {
                logWarning("file: "__FILE__", line: %d, "
                        "round %dth select master fail because alive server "
                        "count: %d < half of total server count: %d, "
                        "try again after %d seconds.", __LINE__, i,
                        active_count, CLUSTER_SERVER_ARRAY.count,
                        sleep_secs);
            }
            sleep(sleep_secs);
            continue;
        }

        if ((active_count == CLUSTER_SERVER_ARRAY.count) ||
                (active_count >= 2 && server_status.is_master))
        {
            break;
        }

        if ((server_status.status < FDIR_SERVER_STATUS_OFFLINE) &&
                !FORCE_MASTER_ELECTION)
        {
            sprintf(status_prompt, "the candidate server status: %d (%s) "
                    "does not match the selection rule. you must start "
                    "ALL servers in the first time, or remove the "
                    "deprecated server(s) from the config file, or execute "
                    " fdir_serverd with option --%s", server_status.status,
                    fdir_get_server_status_caption(server_status.status),
                    FDIR_FORCE_ELECTION_LONG_OPTION_STR);
            force_sleep = true;
        } else {
            if (g_current_time - start_time > ELECTION_MAX_WAIT_TIME) {
                break;
            }

            if (FORCE_MASTER_ELECTION) {
                sprintf(status_prompt, "force_master_election: %d, ",
                        FORCE_MASTER_ELECTION);
            } else {
                *status_prompt = '\0';
            }

            force_sleep = false;
        }

        remain_time = ELECTION_MAX_WAIT_TIME - (g_current_time - start_time);
        if (remain_time > 0) {
            sleep_secs = FC_MIN(remain_time, max_sleep_secs);
        } else {
            if (force_sleep) {
                sleep_secs = max_sleep_secs;
            } else {
                sleep_secs = 1;
            }
        }

        if (need_log) {
            logWarning("file: "__FILE__", line: %d, "
                    "round %dth select master, alive server count: %d "
                    "< server count: %d, %stry again after %d seconds.",
                    __LINE__, i, active_count, CLUSTER_SERVER_ARRAY.count,
                    status_prompt, sleep_secs);
        }

        sleep(sleep_secs);
        if (max_sleep_secs < 32) {
            max_sleep_secs *= 2;
        }
    }

    next_master = CLUSTER_MASTER_ATOM_PTR;
    if (next_master != NULL) {
        logInfo("file: "__FILE__", line: %d, "
                "abort election because the master exists, "
                "master id: %d, ip %s:%u, election time used: %ds",
                __LINE__, next_master->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master->server),
                (int)(g_current_time - start_time));
        return 0;
    }

    next_master = server_status.cs;
    if (CLUSTER_MYSELF_PTR == next_master) {
		if ((result=cluster_notify_master_changed(
                        &server_status)) != 0)
		{
			return result;
		}

		logInfo("file: "__FILE__", line: %d, "
			"I am the new master, id: %d, ip %s:%u, election "
            "time used: %ds", __LINE__, next_master->server->id,
            CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master->server),
            CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master->server),
            (int)(g_current_time - start_time));
    } else {
        if (server_status.is_master) {
            cluster_relationship_set_master(next_master, start_time);
        } else if (CLUSTER_MASTER_ATOM_PTR == NULL) {
            logInfo("file: "__FILE__", line: %d, "
                    "election time used: %ds, waiting for the candidate "
                    "master server id: %d, ip %s:%u notify ...", __LINE__,
                    (int)(g_current_time - start_time), next_master->server->id,
                    CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master->server),
                    CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master->server));
            return ENOENT;
        }
    }

	return 0;
}

static int cluster_ping_master(ConnectionInfo *conn, const int timeout)
{
    int result;
    int connect_timeout;
    int network_timeout;
    FDIRClusterServerInfo *master;

    master = CLUSTER_MASTER_ATOM_PTR;
    if (CLUSTER_MYSELF_PTR == master) {
        return 0;  //do not need ping myself
    }

    if (master == NULL) {
        return ENOENT;
    }

    network_timeout = FC_MIN(SF_G_NETWORK_TIMEOUT, timeout);
    if (conn->sock < 0) {
        connect_timeout = FC_MIN(SF_G_CONNECT_TIMEOUT, timeout);
        if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            master->server), conn, connect_timeout)) != 0)
        {
            return result;
        }

        if ((result=proto_join_master(conn, network_timeout)) != 0) {
            conn_pool_disconnect_server(conn);
            return result;
        }
    }

    if ((result=proto_ping_master(conn, network_timeout)) != 0) {
        conn_pool_disconnect_server(conn);
    }

    return result;
}

static void *cluster_thread_entrance(void* arg)
{
#define MAX_SLEEP_SECONDS  10

    int fail_count;
    int sleep_seconds;
    int ping_remain_time;
    time_t ping_start_time;
    FDIRClusterServerInfo *master;
    ConnectionInfo mconn;  //master connection

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "relationship");
#endif

    memset(&mconn, 0, sizeof(mconn));
    mconn.sock = -1;

    fail_count = 0;
    sleep_seconds = 1;
    ping_start_time = g_current_time;
    while (SF_G_CONTINUE_FLAG) {
        master = CLUSTER_MASTER_ATOM_PTR;
        if (master == NULL) {
            if (cluster_select_master() != 0) {
                sleep_seconds = 1 + (int)((double)rand()
                        * (double)MAX_SLEEP_SECONDS / RAND_MAX);
            } else {
                if (mconn.sock >= 0) {
                    conn_pool_disconnect_server(&mconn);
                }
                ping_start_time = g_current_time;
                sleep_seconds = 1;
            }
        } else {
            ping_remain_time = ELECTION_MASTER_LOST_TIMEOUT -
                (g_current_time - ping_start_time);
            if (ping_remain_time < 2) {
                ping_remain_time = 2;
            }
            if (cluster_ping_master(&mconn, ping_remain_time) == 0) {
                fail_count = 0;
                ping_start_time = g_current_time;
                sleep_seconds = 1;
            } else {
                ++fail_count;
                logError("file: "__FILE__", line: %d, "
                        "%dth ping master id: %d, ip %s:%u fail",
                        __LINE__, fail_count, master->server->id,
                        CLUSTER_GROUP_ADDRESS_FIRST_IP(master->server),
                        CLUSTER_GROUP_ADDRESS_FIRST_PORT(master->server));

                if (g_current_time - ping_start_time >
                        ELECTION_MASTER_LOST_TIMEOUT)
                {
                    if (fail_count > 1) {
                        cluster_unset_master();
                        fail_count = 0;
                    }
                    sleep_seconds = 0;
                } else {
                    sleep_seconds = 1;
                }
            }
        }

        if (sleep_seconds > 0) {
            sleep(sleep_seconds);
        }
    }

    return NULL;
}

int cluster_relationship_init()
{
	pthread_t tid;

	return fc_create_thread(&tid, cluster_thread_entrance, NULL,
            SF_G_THREAD_STACK_SIZE);
}

int cluster_relationship_destroy()
{
	return 0;
}

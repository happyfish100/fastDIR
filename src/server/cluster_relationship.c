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
#include "sf/sf_global.h"
#include "common/fdir_proto.h"
#include "server_global.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"

static FDIRClusterServerInfo *g_next_master = NULL;

typedef struct fdir_cluster_server_status {
    FDIRClusterServerInfo *cs;
    bool is_master;
    int server_id;
    int64_t data_version;
} FDIRClusterServerStatus;

void fdir_log_network_error(const ConnectionInfo *conn,
        const FDIRResponseInfo *response,
        const int line, const int result)
{
    if (response->error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "%s", line, response->error.message);
    } else {
        logError("file: "__FILE__", line: %d, "
                "communicate with server %s:%d fail, "
                "errno: %d, error info: %s",
                line, conn->ip_addr, conn->port,
                result, STRERROR(result));
    }
}

static int proto_get_server_status(ConnectionInfo *conn,
        FDIRClusterServerStatus *server_status)
{
	int result;
	FDIRProtoHeader *header;
    FDIRProtoGetServerStatusReq *req;
    FDIRProtoGetServerStatusResp *resp;
    FDIRResponseInfo response;
	char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoGetServerStatusReq)];
	char in_body[sizeof(FDIRProtoGetServerStatusResp)];

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoGetServerStatusReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->config_sign, CLUSTER_CONFIG_SIGN_BUF, CLUSTER_CONFIG_SIGN_LEN);

	if ((result=fdir_send_and_check_response_header(conn, out_buff,
			sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
            FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP)) != 0)
    {
        fdir_log_network_error(conn, &response, __LINE__, result);
        return result;
    }

    if (response.header.body_len != sizeof(FDIRProtoGetServerStatusResp)) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%d, recv body length: %d != %d",
                __LINE__, conn->ip_addr, conn->port,
                response.header.body_len,
                (int)sizeof(FDIRProtoGetServerStatusResp));
        return EINVAL;
    }

    if ((result=tcprecvdata_nb(conn->sock, in_body, response.header.body_len,
                    SF_G_NETWORK_TIMEOUT)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "recv from server %s:%d fail, "
                "errno: %d, error info: %s",
                __LINE__, conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    resp = (FDIRProtoGetServerStatusResp *)in_body;

    server_status->is_master = resp->is_master;
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

static int proto_join_master(ConnectionInfo *conn)
{
	int result;
	FDIRProtoHeader *header;
    FDIRProtoJoinMasterReq *req;
    FDIRResponseInfo response;
	char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoJoinMasterReq)];

    generate_replica_key();
    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_CLUSTER_PROTO_JOIN_MASTER,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoJoinMasterReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->key, REPLICA_KEY_BUFF, FDIR_REPLICA_KEY_SIZE);
    memcpy(req->config_sign, CLUSTER_CONFIG_SIGN_BUF, CLUSTER_CONFIG_SIGN_LEN);
    if ((result=fdir_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FDIR_PROTO_ACK)) != 0)
    {
        fdir_log_network_error(conn, &response, __LINE__, result);
    }

    return result;
}

static int proto_ping_master(ConnectionInfo *conn)
{
    FDIRProtoHeader header;
    FDIRResponseInfo response;
    int result;
    char in_buff[1024];
    char *pInBuff;

    FDIR_PROTO_SET_HEADER(&header, FDIR_CLUSTER_PROTO_PING_MASTER_REQ, 0);
    if ((result=fdir_send_and_check_response_header(conn, (char *)&header,
                    sizeof(header), &response, SF_G_NETWORK_TIMEOUT,
                    FDIR_CLUSTER_PROTO_PING_MASTER_RESP)) != 0)
    {
        fdir_log_network_error(conn, &response, __LINE__, result);
        return result;
    }

    if (response.header.body_len == 0) {
        return 0;
    }

    pInBuff = in_buff;
    /*
    if ((result=fdfs_recv_response(conn, &pInBuff,
                    sizeof(in_buff), &in_bytes)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "fdfs_recv_response from %s:%d fail, result: %d",
                __LINE__, conn->ip_addr,
                conn->port, result);
        return result;
    }
    */

    return 0;
}

static int cluster_cmp_server_status(const void *p1, const void *p2)
{
	FDIRClusterServerStatus *status1;
	FDIRClusterServerStatus *status2;
	int sub;

	status1 = (FDIRClusterServerStatus *)p1;
	status2 = (FDIRClusterServerStatus *)p2;
	sub = status1->is_master - status2->is_master;
	if (sub != 0) {
		return sub;
	}

	if (status1->data_version < status2->data_version) {
        return -1;
    } else if (status1->data_version > status2->data_version) {
        return 1;
	}

	return status1->server_id - status2->server_id;
}

static int cluster_get_server_status(FDIRClusterServerStatus *server_status)
{
    ConnectionInfo *conn;
    int result;

    if (server_status->cs == CLUSTER_MYSELF_PTR) {
        server_status->is_master = MYSELF_IS_MASTER;
        server_status->server_id = CLUSTER_MY_SERVER_ID;
        server_status->data_version = DATA_VERSION;
        return 0;
    } else {
        if ((conn=fc_server_check_connect(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            server_status->cs->server), SF_G_CONNECT_TIMEOUT,
                        &result)) == NULL)
        {
            return result;
        }
        if ((result=proto_get_server_status(conn, server_status)) != 0) {
            conn_pool_disconnect_server(conn);
        }
        return result;
    }
}

static int cluster_get_master(FDIRClusterServerStatus *server_status)
{
	FDIRClusterServerInfo *server;
	FDIRClusterServerInfo *end;
	FDIRClusterServerStatus *current_status;
	FDIRClusterServerStatus status_array[8];  //TODO
	int count;
	int result;
	int r;
	int i;

	memset(server_status, 0, sizeof(FDIRClusterServerStatus));
	current_status = status_array;
	result = 0;
	end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<end; server++) {
		current_status->cs = server;
        r = cluster_get_server_status(current_status);
		if (r == 0) {
			current_status++;
		} else if (r != ENOENT) {
			result = r;
		}
	}

	count = current_status - status_array;
    if (count == 0) {
        logError("file: "__FILE__", line: %d, "
                "get server status fail, "
                "server count: %d", __LINE__,
                CLUSTER_SERVER_ARRAY.count);
        return result == 0 ? ENOENT : result;
    }

	qsort(status_array, count, sizeof(FDIRClusterServerStatus),
		cluster_cmp_server_status);

	for (i=0; i<count; i++) {
        logInfo("file: "__FILE__", line: %d, "
                "server_id: %d, ip addr %s:%d, is_master: %d, "
                "data_version: %"PRId64, __LINE__,
                status_array[i].server_id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(status_array[i].cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(status_array[i].cs->server),
                status_array[i].is_master, status_array[i].data_version);
    }

	memcpy(server_status, status_array + (count - 1),
			sizeof(FDIRClusterServerStatus));
	return 0;
}

static int do_notify_master_changed(FDIRClusterServerInfo *cs,
		FDIRClusterServerInfo *master, const char cmd, bool *bConnectFail)
{
    char out_buff[sizeof(FDIRProtoHeader) + 4];
    ConnectionInfo *conn;
    FDIRProtoHeader *header;
    FDIRResponseInfo response;
    int result;

    if ((conn=fc_server_check_connect(&CLUSTER_GROUP_ADDRESS_ARRAY(cs->server),
                    SF_G_CONNECT_TIMEOUT, &result)) == NULL)
    {
        *bConnectFail = true;
        return result;
    }
    *bConnectFail = false;

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, cmd, sizeof(out_buff) -
            sizeof(FDIRProtoHeader));
    int2buff(master->server->id, out_buff + sizeof(FDIRProtoHeader));
    if ((result=fdir_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FDIR_PROTO_ACK)) != 0)
    {
        fdir_log_network_error(conn, &response, __LINE__, result);
        conn_pool_disconnect_server(conn);
    }

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

int cluster_relationship_commit_master(FDIRClusterServerInfo *master,
        const bool master_self)
{
    FDIRClusterServerInfo *next_master;
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

    CLUSTER_MASTER_PTR = master;
    if (master_self) {
        ct_reset_slave_arrays();

        MYSELF_IS_MASTER = true;
        //TODO
        //g_tracker_master_chg_count++;
    } else {
        logInfo("file: "__FILE__", line: %d, "
                "the master server id: %d, ip %s:%d",
                __LINE__, master->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(master->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(master->server));
    }

    g_next_master = NULL;
    return 0;
}

static int cluster_notify_next_master(FDIRClusterServerInfo *cs,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    if (server_status->cs == cs) {
        return cluster_relationship_pre_set_master(cs); 
    } else {
        FDIRClusterServerInfo *master;
        master = server_status->cs;
        return do_notify_master_changed(cs, master,
                FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_commit_next_master(FDIRClusterServerInfo *cs,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    FDIRClusterServerInfo *master;

    master = server_status->cs;
    if (server_status->cs == cs) {
        return cluster_relationship_commit_master(master, true);
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
			if (!bConnectFail)
			{
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
	FDIRClusterServerStatus server_status;
    FDIRClusterServerInfo *next_master;

	logInfo("file: "__FILE__", line: %d, "
		"selecting master...", __LINE__);

	if ((result=cluster_get_master(&server_status)) != 0) {
		return result;
	}

    next_master = server_status.cs;
    if (CLUSTER_MYSELF_PTR == next_master) {
		if ((result=cluster_notify_master_changed(
                        &server_status)) != 0)
		{
			return result;
		}

		logInfo("file: "__FILE__", line: %d, "
			"I am the new master, id: %d, ip %s:%d",
			__LINE__, next_master->server->id,
            CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master->server),
            CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master->server));
	} else {
        if (server_status.is_master) {
            CLUSTER_MASTER_PTR = next_master;

			logInfo("file: "__FILE__", line: %d, "
				"the master server id: %d, ip %s:%d",
                __LINE__, next_master->server->id,
				CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master->server));
        }
        else
		{
			logInfo("file: "__FILE__", line: %d, "
				"waiting for the candidate master server id: %d, "
                "ip %s:%d notify ...", __LINE__, next_master->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master->server));
			return ENOENT;
		}
	}

	return 0;
}


//CLUSTER_ACTIVE_SLAVES

static int master_check_brain_split()
{
    return 0;
}

static int cluster_ping_master()
{
	int result;
    static bool master_joined = false;
    FDIRClusterServerInfo *master;
    ConnectionInfo *conn;

	if (MYSELF_IS_MASTER) {
		return 0;  //do not need ping myself
	}

    master = CLUSTER_MASTER_PTR;
	if (master == NULL) {
		return ENOENT;
	}

    if ((conn=fc_server_check_connect(&CLUSTER_GROUP_ADDRESS_ARRAY(master->
                        server), SF_G_CONNECT_TIMEOUT, &result)) == NULL)
    {
        return result;
    }

    if (!master_joined) {
        if ((result=proto_join_master(conn)) != 0) {
            conn_pool_disconnect_server(conn);
            return result;
        }
        master_joined = true;
    }

	if ((result=proto_ping_master(conn)) != 0) {
        conn_pool_disconnect_server(conn);
        master_joined = false;
    }
	return result;
}

static void *cluster_thread_entrance(void* arg)
{
#define MAX_SLEEP_SECONDS  10

    int fail_count;
    int sleep_seconds;
    FDIRClusterServerInfo *master;

    fail_count = 0;
    sleep_seconds = 1;
    while (SF_G_CONTINUE_FLAG) {
        master = CLUSTER_MASTER_PTR;
        if (master == NULL) {
            if (cluster_select_master() != 0) {
                sleep_seconds = 1 + (int)((double)rand()
                        * (double)MAX_SLEEP_SECONDS / RAND_MAX);
            } else {
                sleep_seconds = 1;
            }
        } else {
            if (cluster_ping_master() == 0) {
                fail_count = 0;
                sleep_seconds = 1;
            } else {
                ++fail_count;
                logError("file: "__FILE__", line: %d, "
                        "%dth ping master id: %d, ip %s:%d fail",
                        __LINE__, fail_count, master->server->id,
                        CLUSTER_GROUP_ADDRESS_FIRST_IP(master->server),
                        CLUSTER_GROUP_ADDRESS_FIRST_PORT(master->server));

                sleep_seconds *= 2;
                if (fail_count >= 4) {
                    CLUSTER_MASTER_PTR = NULL;
                    fail_count = 0;
                    sleep_seconds = 1;
                }
            }
        }

        sleep(sleep_seconds);
    }

    return NULL;
}

int cluster_relationship_init()
{
	int result;
	pthread_t tid;
	pthread_attr_t thread_attr;

	if ((result=init_pthread_attr(&thread_attr, SF_G_THREAD_STACK_SIZE)) != 0) {
		logError("file: "__FILE__", line: %d, "
			"init_pthread_attr fail, program exit!", __LINE__);
		return result;
	}

	if ((result=pthread_create(&tid, &thread_attr,
			cluster_thread_entrance, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, "
			"create thread failed, errno: %d, error info: %s",
			__LINE__, result, STRERROR(result));
		return result;
	}

	pthread_attr_destroy(&thread_attr);
	return 0;
}

int cluster_relationship_destroy()
{
	return 0;
}

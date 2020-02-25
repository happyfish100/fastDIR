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
#include "sf/sf_global.h"
#include "common/fdir_proto.h"
#include "server_global.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"

static FCServerInfo *g_next_master = NULL;

typedef struct fdir_cluster_server_status {
    FCServerInfo *server;
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
    int2buff(CLUSTER_MYSELF_PTR->id, req->server_id);
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

static int proto_join_master(ConnectionInfo *conn)
{
	int result;
	FDIRProtoHeader *header;
    FDIRProtoJoinMasterReq *req;
    FDIRResponseInfo response;
	char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoJoinMasterReq)];

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_CLUSTER_PROTO_JOIN_MASTER,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoJoinMasterReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_MYSELF_PTR->id, req->server_id);
    long2buff(DATA_VERSION, req->data_version);
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

    if (server_status->server == CLUSTER_MYSELF_PTR) {
        server_status->is_master = MYSELF_IS_MASTER;
        server_status->server_id = CLUSTER_MYSELF_PTR->id;
        server_status->data_version = DATA_VERSION;
        return 0;
    } else {
        if ((conn=fc_server_check_connect(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            server_status->server), SF_G_CONNECT_TIMEOUT,
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
	FCServerInfo *server;
	FCServerInfo *end;
	FDIRClusterServerStatus *current_status;
	FDIRClusterServerStatus status_array[8];  //TODO
	int count;
	int result;
	int r;
	int i;

	memset(server_status, 0, sizeof(FDIRClusterServerStatus));
	current_status = status_array;
	result = 0;
	end = FC_SID_SERVERS(CLUSTER_CONFIG_CTX) +
        FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX);
	for (server=FC_SID_SERVERS(CLUSTER_CONFIG_CTX); server<end; server++) {
		current_status->server = server;
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
                FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX));
        return result == 0 ? ENOENT : result;
    }

	qsort(status_array, count, sizeof(FDIRClusterServerStatus),
		cluster_cmp_server_status);

	for (i=0; i<count; i++) {
        logInfo("file: "__FILE__", line: %d, "
                "server_id: %d, ip addr %s:%d, is_master: %d, "
                "data_version: %"PRId64, __LINE__,
                status_array[i].server_id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(status_array[i].server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(status_array[i].server),
                status_array[i].is_master, status_array[i].data_version);
    }

	memcpy(server_status, status_array + (count - 1),
			sizeof(FDIRClusterServerStatus));
	return 0;
}

static int do_notify_master_changed(FCServerInfo *server,
		FCServerInfo *master, const char cmd, bool *bConnectFail)
{
    char out_buff[sizeof(FDIRProtoHeader) + 4];
    ConnectionInfo *conn;
    FDIRProtoHeader *header;
    FDIRResponseInfo response;
    int result;

    if ((conn=fc_server_check_connect(&CLUSTER_GROUP_ADDRESS_ARRAY(server),
                    SF_G_CONNECT_TIMEOUT, &result)) == NULL)
    {
        *bConnectFail = true;
        return result;
    }
    *bConnectFail = false;

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, cmd, sizeof(out_buff) -
            sizeof(FDIRProtoHeader));
    int2buff(master->id, out_buff + sizeof(FDIRProtoHeader));
    if ((result=fdir_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FDIR_PROTO_ACK)) != 0)
    {
        fdir_log_network_error(conn, &response, __LINE__, result);
        conn_pool_disconnect_server(conn);
    }

    return result;
}

int cluster_relationship_pre_set_master(FCServerInfo *master)
{
    FCServerInfo *next_master;

    next_master = g_next_master;
    if (next_master == NULL) {
        g_next_master = master;
    } else if (next_master != master) {
        logError("file: "__FILE__", line: %d, "
                "try to set next master id: %d, "
                "but next master: %d already exist",
                __LINE__, master->id, next_master->id);
        g_next_master = NULL;
        return EEXIST;
    }

    return 0;
}

int cluster_relationship_commit_master(FCServerInfo *master,
        const bool master_self)
{
    FCServerInfo *next_master;
    next_master = g_next_master;
    if (next_master == NULL) {
        logError("file: "__FILE__", line: %d, "
                "next master is NULL", __LINE__);
        return EBUSY;
    }
    if (next_master != master) {
        logError("file: "__FILE__", line: %d, "
                "next master server id: %d != expected server id: %d",
                __LINE__, next_master->id, master->id);
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
                __LINE__, master->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(master),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(master));
    }

    g_next_master = NULL;
    return 0;
}

static int cluster_notify_next_master(FCServerInfo *server,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    if (server_status->server == server) {
        return cluster_relationship_pre_set_master(server); 
    } else {
        FCServerInfo *master;
        master = server_status->server;
        return do_notify_master_changed(server, master,
                FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_commit_next_master(FCServerInfo *server,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    FCServerInfo *master;

    master = server_status->server;
    if (server_status->server == server) {
        return cluster_relationship_commit_master(master, true);
    } else {
        return do_notify_master_changed(server, master,
                FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_notify_master_changed(FDIRClusterServerStatus *server_status)
{
	FCServerInfo *server;
	FCServerInfo *send;
	int result;
	bool bConnectFail;
	int success_count;

	result = ENOENT;
	send = FC_SID_SERVERS(CLUSTER_CONFIG_CTX) +
    FC_SID_SERVER_COUNT(CLUSTER_CONFIG_CTX);
	success_count = 0;
	for (server=FC_SID_SERVERS(CLUSTER_CONFIG_CTX); server<send; server++) {
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
	for (server=FC_SID_SERVERS(CLUSTER_CONFIG_CTX); server<send; server++) {
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
    FCServerInfo *next_master;

	logInfo("file: "__FILE__", line: %d, "
		"selecting master...", __LINE__);

	if ((result=cluster_get_master(&server_status)) != 0) {
		return result;
	}

    next_master = server_status.server;
    if (CLUSTER_MYSELF_PTR == next_master) {
		if ((result=cluster_notify_master_changed(
                        &server_status)) != 0)
		{
			return result;
		}

		logInfo("file: "__FILE__", line: %d, "
			"I am the new master, id: %d, ip %s:%d",
			__LINE__, next_master->id,
            CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master),
            CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master));
	} else {
        if (server_status.is_master) {
            CLUSTER_MASTER_PTR = next_master;

			logInfo("file: "__FILE__", line: %d, "
				"the master server id: %d, ip %s:%d",
                __LINE__, next_master->id,
				CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master));
        }
        else
		{
			logInfo("file: "__FILE__", line: %d, "
				"waiting for the candidate master server id: %d, "
                "ip %s:%d notify ...", __LINE__, next_master->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(next_master),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_master));
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
    FCServerInfo *master;
    ConnectionInfo *conn;

	if (MYSELF_IS_MASTER) {
		return 0;  //do not need ping myself
	}

    master = CLUSTER_MASTER_PTR;
	if (master == NULL) {
		return ENOENT;
	}

    if ((conn=fc_server_check_connect(&CLUSTER_GROUP_ADDRESS_ARRAY(master),
                    SF_G_CONNECT_TIMEOUT, &result)) == NULL)
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
    FCServerInfo *master;

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
                        __LINE__, fail_count, master->id,
                        CLUSTER_GROUP_ADDRESS_FIRST_IP(master),
                        CLUSTER_GROUP_ADDRESS_FIRST_PORT(master));

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

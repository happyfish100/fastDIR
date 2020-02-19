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
#include "cluster_relationship.h"

typedef struct fdir_cluster_server_status {
    FCServerInfo *server;
    bool is_master;
    int server_id;
    int64_t data_version;
} FDIRClusterServerStatus;

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
            sizeof(out_buff));

    req = (FDIRProtoGetServerStatusReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_MYSELF_PTR->id, req->server_id);
    memcpy(req->config_sign, CLUSTER_CONFIG_SIGN_BUF, CLUSTER_CONFIG_SIGN_LEN);

	if ((result=fdir_send_and_check_response_header(conn, out_buff,
			sizeof(out_buff), &response, g_sf_global_vars.network_timeout,
            FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP)) != 0)
    {
        if (response.error.length > 0) {
            logError("file: "__FILE__", line: %d, "
                    "communicate with server %s:%d fail, "
                    "errno: %d, error info: %s",
                    __LINE__, conn->ip_addr,
                    conn->port, result, response.error.message);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "communicate with server %s:%d fail, "
                    "errno: %d, error info: %s",
                    __LINE__, conn->ip_addr,
                    conn->port, result, STRERROR(result));
        }
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
                    g_sf_global_vars.network_timeout)) != 0)
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

static int proto_ping_master(ConnectionInfo *conn)
{
    FDIRProtoHeader header;
    FDIRResponseInfo response;
    int result;
    char in_buff[1024];
    char *pInBuff;

    FDIR_PROTO_SET_HEADER(&header, FDIR_CLUSTER_PROTO_PING_MASTER,
            sizeof(header));
    if ((result=fdir_send_and_check_response_header(conn, (char *)&header,
                    sizeof(header), &response, g_sf_global_vars.network_timeout,
                    FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP)) != 0)
    {
        if (response.error.length > 0) {
            logError("file: "__FILE__", line: %d, "
                    "communicate with server %s:%d fail, "
                    "errno: %d, error info: %s",
                    __LINE__, conn->ip_addr,
                    conn->port, result, response.error.message);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "communicate with server %s:%d fail, "
                    "errno: %d, error info: %s",
                    __LINE__, conn->ip_addr,
                    conn->port, result, STRERROR(result));
        }
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

static int cluster_get_server_status(ConnectionInfo *conn,
        FDIRClusterServerStatus *server_status)
{
    /*
    if (fdfs_server_contain_local_service(server_status->conn,
                g_server_port))
    {
        server_status->is_master = MYSELF_IS_MASTER;
        return 0;
    }
    else
    {
        return proto_get_server_status(conn, server_status);
    }
    */
    return proto_get_server_status(conn, server_status);
    //return 0;
}

static int cluster_get_master(FDIRClusterServerStatus *server_status)
{
	FCServerInfo *server;
	FCServerInfo *end;
	FDIRClusterServerStatus *current_status;
	FDIRClusterServerStatus status_array[8];
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
        r = cluster_get_server_status(NULL, current_status);
		if (r == 0) {
			current_status++;
		} else if (r != ENOENT) {
			result = r;
		}
	}

	count = current_status - status_array;
	if (count == 0)
	{
        /*
		logError("file: "__FILE__", line: %d, "
                "get tracker status fail, "
                "server count: %d", __LINE__,
                g_tracker_servers.server_count);
                */
		return result == 0 ? ENOENT : result;
	}

	qsort(status_array, count, sizeof(FDIRClusterServerStatus),
		cluster_cmp_server_status);

	for (i=0; i<count; i++)
	{
        /*
		logDebug("file: "__FILE__", line: %d, " \
			"%s:%d is_master: %d, running time: %d, " \
			"restart interval: %d", __LINE__, \
			status_array[i].conn->connections->ip_addr, \
			status_array[i].conn->connections->port, \
			status_array[i].is_master, \
			status_array[i].running_time, \
			status_array[i].restart_interval);
            */
	}

	memcpy(server_status, status_array + (count - 1),
			sizeof(FDIRClusterServerStatus));
	return 0;
}

/*
static int do_notify_master_changed(TrackerServerInfo *conn, \
		ConnectionInfo *pLeader, const char cmd, bool *bConnectFail)
{
	char out_buff[sizeof(FDIRProtoHeader) + FDFS_PROTO_IP_PORT_SIZE];
	char in_buff[1];
	ConnectionInfo *conn;
	FDIRProtoHeader *pHeader;
	char *pInBuff;
	int64_t in_bytes;
	int result;

    fdfs_server_sock_reset(conn);
	if ((conn=tracker_connect_server(conn, &result)) == NULL)
	{
		*bConnectFail = true;
		return result;
	}
	*bConnectFail = false;

	do
	{
	memset(out_buff, 0, sizeof(out_buff));
	pHeader = (FDIRProtoHeader *)out_buff;
	pHeader->cmd = cmd;
	sprintf(out_buff + sizeof(FDIRProtoHeader), "%s:%d", \
			pLeader->ip_addr, pLeader->port);
	long2buff(FDFS_PROTO_IP_PORT_SIZE, pHeader->pkg_len);
	if ((result=tcpsenddata_nb(conn->sock, out_buff, \
			sizeof(out_buff), g_sf_global_vars.network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, "
			"send data to server %s:%d fail, "
			"errno: %d, error info: %s", __LINE__,
			conn->ip_addr, conn->port, result, STRERROR(result));

		result = (result == ENOENT ? EACCES : result);
		break;
	}

	pInBuff = in_buff;
	result = fdfs_recv_response(conn, &pInBuff, \
				0, &in_bytes);
	if (result != 0)
	{
        logError("file: "__FILE__", line: %d, "
                "fdfs_recv_response from server %s:%d fail, "
                "result: %d", __LINE__, conn->ip_addr, conn->port, result);
		break;
	}

	if (in_bytes != 0)
	{
		logError("file: "__FILE__", line: %d, "
			"server %s:%d response data "
			"length: %"PRId64" is invalid, "
			"expect length: %d.", __LINE__,
			conn->ip_addr, conn->port, in_bytes, 0);
		result = EINVAL;
		break;
	}
	} while (0);

	if (conn->port == g_server_port &&
		is_local_host_ip(conn->ip_addr))
	{
		tracker_close_connection_ex(conn, true);
	}
	else
	{
		tracker_close_connection_ex(conn, result != 0);
	}

	return result;
}

void cluster_relationship_set_master(const int server_index,
        ConnectionInfo *pLeader, const bool master_self)
{
    g_tracker_servers.master_index = server_index;
    g_next_master_index = -1;

    if (master_self)
    {
        MYSELF_IS_MASTER = true;
        g_tracker_master_chg_count++;
    }
    else
    {
        logInfo("file: "__FILE__", line: %d, "
            "the tracker master is %s:%d", __LINE__,
            pLeader->ip_addr, pLeader->port);
    }
}

static int cluster_notify_next_master(TrackerServerInfo *conn,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    if (server_status->conn == conn)
    {
        g_next_master_index = conn - g_tracker_servers.servers;
        return 0;
    }
    else
    {
        ConnectionInfo *pLeader;
        pLeader = server_status->conn->connections;
        return do_notify_master_changed(conn, pLeader,
                FDIR_CLUSTER_PROTO_NOTIFY_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_commit_next_master(TrackerServerInfo *conn,
        FDIRClusterServerStatus *server_status, bool *bConnectFail)
{
    ConnectionInfo *pLeader;

    pLeader = server_status->conn->connections;
    if (server_status->conn == conn)
    {
        int server_index;
        int expect_index;
        server_index = g_next_master_index;
        expect_index = conn - g_tracker_servers.servers;
        if (server_index != expect_index)
        {
            logError("file: "__FILE__", line: %d, "
                    "g_next_master_index: %d != expected: %d",
                    __LINE__, server_index, expect_index);
            g_next_master_index = -1;
            return EBUSY;
        }

        cluster_relationship_set_master(server_index, pLeader, true);
        return 0;
    }
    else
    {
        return do_notify_master_changed(conn, pLeader,
                FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER, bConnectFail);
    }
}

static int cluster_notify_master_changed(FDIRClusterServerStatus *server_status)
{
	TrackerServerInfo *conn;
	TrackerServerInfo *pTrackerEnd;
	int result;
	bool bConnectFail;
	int success_count;

	result = ENOENT;
	pTrackerEnd = g_tracker_servers.servers + g_tracker_servers.server_count;
	success_count = 0;
	for (conn=g_tracker_servers.servers;
		conn<pTrackerEnd; conn++)
	{
		if ((result=cluster_notify_next_master(conn,
				server_status, &bConnectFail)) != 0)
		{
			if (!bConnectFail)
			{
				return result;
			}
		}
		else
		{
			success_count++;
		}
	}

	if (success_count == 0)
	{
		return result;
	}

	result = ENOENT;
	success_count = 0;
	for (conn=g_tracker_servers.servers;
		conn<pTrackerEnd; conn++)
	{
		if ((result=cluster_commit_next_master(conn,
				server_status, &bConnectFail)) != 0)
		{
			if (!bConnectFail)
			{
				return result;
			}
		}
		else
		{
			success_count++;
		}
	}
	if (success_count == 0)
	{
		return result;
	}

	return 0;
}

static int cluster_select_master()
{
	int result;
	FDIRClusterServerStatus trackerStatus;
    ConnectionInfo *conn;

	if (g_tracker_servers.server_count <= 0)
	{
		return 0;
	}

	logInfo("file: "__FILE__", line: %d, " \
		"selecting tracker master...", __LINE__);

	if ((result=cluster_get_master(&trackerStatus)) != 0)
	{
		return result;
	}

    conn = trackerStatus.conn->connections;
    if (fdfs_server_contain_local_service(trackerStatus.
                conn, g_server_port))
	{
		if ((result=cluster_notify_master_changed(
                        &trackerStatus)) != 0)
		{
			return result;
		}

		logInfo("file: "__FILE__", line: %d, "
			"I am the new tracker master %s:%d",
			__LINE__, conn->ip_addr, conn->port);
	}
	else
	{
		if (trackerStatus.is_master)
		{
			g_tracker_servers.master_index =
				trackerStatus.conn -
				g_tracker_servers.servers;
			if (g_tracker_servers.master_index < 0 ||
				g_tracker_servers.master_index >=
				g_tracker_servers.server_count)
			{
                logError("file: "__FILE__", line: %d, "
                        "invalid tracker master index: %d",
                        __LINE__, g_tracker_servers.master_index);
				g_tracker_servers.master_index = -1;
				return EINVAL;
			}
		}

        if (g_tracker_servers.master_index >= 0)
        {
			logInfo("file: "__FILE__", line: %d, "
				"the tracker master %s:%d", __LINE__,
				conn->ip_addr, conn->port);
        }
        else
		{
			logInfo("file: "__FILE__", line: %d, "
				"waiting for the candidate tracker master %s:%d notify ...",
                __LINE__, conn->ip_addr, conn->port);
			return ENOENT;
		}
	}

	return 0;
}
*/

static int cluster_ping_master()
{
	int result;
    FCServerInfo *master;
    ConnectionInfo *conn;

	if (MYSELF_IS_MASTER)
	{
		return 0;  //do not need ping myself
	}

    master = CLUSTER_MASTER_PTR;
	if (master == NULL) {
		return EINVAL;
	}

    /*
    if ((conn=tracker_connect_server(conn, &result)) == NULL) {
        return result;
	}
    */
    conn = NULL;

	result = proto_ping_master(conn);
    //tracker_close_connection_ex(conn, result != 0);
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
    while (g_sf_global_vars.continue_flag) {
        master = CLUSTER_MASTER_PTR;
        if (master == NULL) {
            //if (cluster_select_master() != 0) {
            if (false) {
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
                ConnectionInfo *conn;

                conn = NULL;
                //conn = master->connections;

                ++fail_count;
                logError("file: "__FILE__", line: %d, "
                        "%dth ping master %s:%d fail", __LINE__,
                        fail_count, conn->ip_addr, conn->port);

                sleep_seconds *= 2;
                if (fail_count >= 3) {
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

	if ((result=init_pthread_attr(&thread_attr, g_sf_global_vars.
                    thread_stack_size)) != 0)
    {
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


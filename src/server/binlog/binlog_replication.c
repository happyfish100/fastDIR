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
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "../../common/fdir_proto.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_producer.h"
#include "binlog_replication.h"

/*
static int binlog_sync_to_server(BinlogSyncContext *sync_context)
{
    if (sync_context->binlog_buffer.length == 0) {
        return 0;
    }

    //TODO
    //int64_t last_data_version;
    sync_context->binlog_buffer.length = 0;  //reset cache buff

    return 0;
}

static inline int deal_binlog_one_record(BinlogSyncContext *sync_context,
        ServerBinlogRecordBuffer *rb)
{
    int result;
    if (sync_context->binlog_buffer.size - sync_context->binlog_buffer.length
            < rb->buffer.length)
    {
        if ((result=binlog_sync_to_server(sync_context)) != 0) {
            return result;
        }
    }

    //TODO
    if (__sync_sub_and_fetch(&((FDIRServerTaskArg *)rb->task->arg)->context.
            service.waiting_rpc_count, 1) == 0)
    {
        sf_nio_notify(rb->task, SF_NIO_STAGE_CONTINUE);
    }

    memcpy(sync_context->binlog_buffer.buff +
            sync_context->binlog_buffer.length,
            rb->buffer.data, rb->buffer.length);
    sync_context->binlog_buffer.length += rb->buffer.length;
    return 0;
}

static int deal_binlog_records(BinlogSyncContext *sync_context,
        struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rb;
    int result;

    do {
        rb = (ServerBinlogRecordBuffer *)node->data;
        if ((result=deal_binlog_one_record(sync_context, rb)) != 0) {
            return result;
        }

        server_binlog_release_rbuffer(rb);
        node = node->next;
    } while (node != NULL);

    return binlog_sync_to_server(sync_context);
}

void *binlog_replication_func(void *arg)
{
    struct common_blocked_queue *queue;
    struct common_blocked_node *node;
    BinlogSyncContext sync_context;

    if (binlog_buffer_init(&sync_context.binlog_buffer) != 0) {
        logCrit("file: "__FILE__", line: %d, "
                "binlog_buffer_init fail, program exit!", __LINE__);

        SF_G_CONTINUE_FLAG = false;
        return NULL;
    }

    sync_context.peer_server = ((ServerBinlogConsumerContext *)arg)->server;
    queue = &((ServerBinlogConsumerContext *)arg)->queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(queue);
        if (node == NULL) {
            continue;
        }

        deal_binlog_records(&sync_context, node);
        common_blocked_queue_free_all_nodes(queue, node);
    }

    return NULL;
}
*/

static int check_alloc_ptr_array(FDIRSlaveReplicationPtrArray *array)
{
    int bytes;

    if (array->replications != NULL) {
        return 0;
    }

    bytes = sizeof(FDIRSlaveReplication *) * CLUSTER_SERVER_ARRAY.count;
    array->replications = (FDIRSlaveReplication **)malloc(bytes);
    if (array->replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(array->replications, 0, bytes);
    return 0;
}

static void add_to_replication_ptr_array(FDIRSlaveReplicationPtrArray *
        array, FDIRSlaveReplication *replication)
{
    array->replications[array->count++] = replication;
}

static int remove_from_replication_ptr_array(FDIRSlaveReplicationPtrArray *
        array, FDIRSlaveReplication *replication)
{
    int i;

    for (i=0; i<array->count; i++) {
        if (array->replications[i] == replication) {
            break;
        }
    }

    if (i == array->count) {
        return ENOENT;
    }

    for (i=i+1; i<array->count; i++) {
        array->replications[i - 1] = array->replications[i];
    }
    array->count--;
    return 0;
}

int binlog_replication_bind_thread(FDIRSlaveReplication *replication)
{
    int result;
    struct fast_task_info *task;
    FDIRServerContext *server_ctx;

    task = free_queue_pop();
    if (task == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc task buff failed, you should "
                "increase the parameter: max_connections",
                __LINE__);
        return ENOMEM;
    }

    task->canceled = false;
    task->ctx = &CLUSTER_SF_CTX;
    task->event.fd = -1;
    task->thread_data = CLUSTER_SF_CTX.thread_data +
        replication->index % CLUSTER_SF_CTX.work_threads;

    CLUSTER_TASK_TYPE = FDIR_CLUSTER_TASK_TYPE_REPLICATION;
    CLUSTER_REPLICA = replication;
    replication->connection_info.conn.sock = -1;
    replication->task = task;
    server_ctx = (FDIRServerContext *)task->thread_data->arg;
    if ((result=check_alloc_ptr_array(&server_ctx->
                    cluster.connectings)) != 0)
    {
        return result;
    }
    if ((result=check_alloc_ptr_array(&server_ctx->
                    cluster.connected)) != 0)
    {
        return result;
    }

    add_to_replication_ptr_array(&server_ctx->
            cluster.connectings, replication);
    return 0;
}

int binlog_replication_rebind_thread(FDIRSlaveReplication *replication)
{
    int result;
    FDIRServerContext *server_ctx;

    server_ctx = (FDIRServerContext *)replication->task->thread_data->arg;
    if ((result=remove_from_replication_ptr_array(&server_ctx->
                cluster.connected, replication)) == 0)
    {
        result = binlog_replication_bind_thread(replication);
    }

    return result;
}

static inline FDIRSlaveReplication *replication_array_next(
        FDIRSlaveReplicationPtrArray *array)
{
    if (array->index >= array->count) {
        return NULL;
    }

    return array->replications[array->index++];
}

static int async_connect_server(ConnectionInfo *conn)
{
    int result;
    int domain;
    sockaddr_convert_t convert;

    if (conn->socket_domain == AF_INET || conn->socket_domain == AF_INET6) {
        domain = conn->socket_domain;
    } else {
        domain = is_ipv6_addr(conn->ip_addr) ? AF_INET6 : AF_INET;
    }
    conn->sock = socket(domain, SOCK_STREAM, 0);
    if(conn->sock < 0) {
        logError("file: "__FILE__", line: %d, "
                "socket create fail, errno: %d, "
                "error info: %s", __LINE__, errno, STRERROR(errno));
        return errno != 0 ? errno : EPERM;
    }

    SET_SOCKOPT_NOSIGPIPE(conn->sock);
    if ((result=tcpsetnonblockopt(conn->sock)) != 0) {
        close(conn->sock);
        conn->sock = -1;
        return result;
    }

    if ((result=setsockaddrbyip(conn->ip_addr, conn->port, &convert)) != 0) {
        return result;
    }

    if (connect(conn->sock, &convert.sa.addr, convert.len) < 0) {
        result = errno != 0 ? errno : EINPROGRESS;
        if (result != EINPROGRESS) {
            logError("file: "__FILE__", line: %d, "
                    "connect to %s:%d fail, errno: %d, error info: %s",
                    __LINE__, conn->ip_addr, conn->port,
                    result, STRERROR(result));
            close(conn->sock);
            conn->sock = -1;
        }
        return result;
    }

    return 0;
}

static int deal_connecting_replication(FDIRSlaveReplication *replication)
{
    int result;
    int polled;
    socklen_t len;
    struct pollfd pollfds;

    if (replication->connection_info.conn.sock < 0) {
        FCAddressPtrArray *addr_array;
        FCAddressInfo *addr;

        addr_array = &CLUSTER_GROUP_ADDRESS_ARRAY(replication->slave->server);
        addr = addr_array->addrs[addr_array->index++];
        if (addr_array->index >= addr_array->count) {
            addr_array->index = 0;
        }

        replication->connection_info.start_time = g_current_time;
        replication->connection_info.conn = addr->conn;
        if ((result=async_connect_server(&replication->
                        connection_info.conn)) == 0)
        {
            return 0;
        }
        if (result != EINPROGRESS) {
            return result;
        }
    }

    pollfds.fd = replication->connection_info.conn.sock;
    pollfds.events = POLLIN | POLLOUT;
    polled = poll(&pollfds, 1, 0);
    if (polled == 0) {  //timeout
        if (g_current_time - replication->connection_info.start_time >
                SF_G_CONNECT_TIMEOUT)
        {
            result = ETIMEDOUT;
        } else {
            result = EINPROGRESS;
        }
    } else if (polled < 0) {   //error
        result = errno != 0 ? errno : EIO;
    } else {
        len = sizeof(result);
        if (getsockopt(replication->connection_info.conn.sock, SOL_SOCKET,
                    SO_ERROR, &result, &len) < 0)
        {
            result = errno != 0 ? errno : EACCES;
        }
    }

    if (!(result == 0 || result == EINPROGRESS)) {
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    }
    return result;
}

static int send_join_slave_package(FDIRSlaveReplication *replication)
{
	int result;
	FDIRProtoHeader *header;
    FDIRProtoJoinSlaveReq *req;
	char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoJoinSlaveReq)];

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoJoinSlaveReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_ID, req->cluster_id);
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->key, replication->slave->key, FDIR_REPLICA_KEY_SIZE);

    if ((result=tcpsenddata_nb(replication->connection_info.conn.sock,
                    out_buff, sizeof(out_buff), SF_G_NETWORK_TIMEOUT)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "send data to server %s:%d fail, "
                "errno: %d, error info: %s", __LINE__,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port,
                result, STRERROR(result));
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    }

    return result;
}

static int connect_and_send_join_package(FDIRSlaveReplication *replication)
{
    int result;
    result = deal_connecting_replication(replication);
    if (result == 0) {
        result = send_join_slave_package(replication);
    }

    return result;
}

static int deal_replication_connectings(FDIRServerContext *server_ctx)
{
    int result;
    FDIRSlaveReplicationPtrArray *array;
    FDIRSlaveReplication *replication;

    array = &server_ctx->cluster.connectings;
    if (array->count == 0) {
        return 0;
    }

    array->index = 0;
    while ((replication=replication_array_next(array)) != NULL) {
        result = connect_and_send_join_package(replication);
        if (result == 0) {
            if (remove_from_replication_ptr_array(&server_ctx->
                    cluster.connectings, replication) == 0)
            {
                add_to_replication_ptr_array(&server_ctx->
                        cluster.connected, replication);
            }

            replication->task->event.fd = replication->
                connection_info.conn.sock;
            snprintf(replication->task->client_ip,
                    sizeof(replication->task->client_ip), "%s",
                    replication->connection_info.conn.ip_addr);
            if (sf_nio_notify(replication->task, SF_NIO_STAGE_INIT) != 0) {
            }
        } else if (result != EINPROGRESS) {
            logError("file: "__FILE__", line: %d, "
                    "connect to %s:%d fail, time used: %ds, "
                    "errno: %d, error info: %s", __LINE__,
                    replication->connection_info.conn.ip_addr,
                    replication->connection_info.conn.port, (int)
                    (g_current_time - replication->connection_info.start_time),
                    result, STRERROR(result));
        }
    }

    return 0;
}

static int deal_connected_replication(FDIRSlaveReplication *replication)
{
    return 0;
}

static int deal_replication_connected(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplicationPtrArray *array;
    FDIRSlaveReplication *replication;

    array = &server_ctx->cluster.connected;
    if (array->count == 0) {
        return 0;
    }

    array->index = 0;
    while ((replication=replication_array_next(array)) != NULL) {
        if (deal_connected_replication(replication) != 0) {
            //TODO
            //array->index--; //rewind index
        }
    }

    return 0;
}

int binlog_replication_process(FDIRServerContext *server_ctx)
{
    int result;

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}

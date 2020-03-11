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

static void calc_next_connect_time(FDIRSlaveReplication *replication)
{
    int interval;

    switch (replication->connection_info.fail_count) {
        case 0:
            interval = 1;
            break;
        case 1:
            interval = 2;
            break;
        case 2:
            interval = 4;
            break;
        case 3:
            interval = 8;
            break;
        case 4:
            interval = 16;
            break;
        default:
            interval = 32;
            break;
    }

    replication->connection_info.next_connect_time = g_current_time + interval;
}

static int check_and_make_replica_connection(FDIRSlaveReplication *replication)
{
    int result;
    int polled;
    socklen_t len;
    struct pollfd pollfds;

    if (replication->connection_info.conn.sock < 0) {
        FCAddressPtrArray *addr_array;
        FCAddressInfo *addr;

        if (replication->connection_info.next_connect_time > g_current_time) {
            return EAGAIN;
        }

        addr_array = &CLUSTER_GROUP_ADDRESS_ARRAY(replication->slave->server);
        addr = addr_array->addrs[addr_array->index++];
        if (addr_array->index >= addr_array->count) {
            addr_array->index = 0;
        }

        replication->connection_info.start_time = g_current_time;
        replication->connection_info.conn = addr->conn;
        calc_next_connect_time(replication);
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

#define DECREASE_TASK_WAITING_RPC_COUNT(rb) \
    do { \
        if (__sync_sub_and_fetch(&((FDIRServerTaskArg *)rb->task->arg)-> \
                    context.service.waiting_rpc_count, 1) == 0) \
        { \
            sf_nio_notify(rb->task, SF_NIO_STAGE_CONTINUE);  \
        } \
    } while (0)

static void replication_queue_discard(FDIRSlaveReplication *replication)
{
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *current;

    pthread_mutex_lock(&replication->queue.lock);
    current = replication->queue.head;
    if (replication->queue.head != NULL) {
        replication->queue.head = replication->queue.tail = NULL;
    }
    pthread_mutex_unlock(&replication->queue.lock);

    while (current != NULL) {
        rb = current;
        current = current->nexts[replication->index];

        DECREASE_TASK_WAITING_RPC_COUNT(rb);
        server_binlog_release_rbuffer(rb);
    }
}

static int deal_connecting_replication(FDIRSlaveReplication *replication)
{
    int result;

    replication_queue_discard(replication);
    result = check_and_make_replica_connection(replication);
    if (result == 0) {
        result = send_join_slave_package(replication);
    }

    return result;
}

static int deal_replication_connectings(FDIRServerContext *server_ctx)
{
#define SUCCESS_ARRAY_ELEMENT_MAX  8

    int result;
    int i;
    char prompt[128];
    struct {
        int count;
        FDIRSlaveReplication *replications[SUCCESS_ARRAY_ELEMENT_MAX];
    } success_array;
    FDIRSlaveReplication *replication;

    if (server_ctx->cluster.connectings.count == 0) {
        return 0;
    }

    success_array.count = 0;
    for (i=0; i<server_ctx->cluster.connectings.count; i++) {
        replication = server_ctx->cluster.connectings.replications[i];
        result = deal_connecting_replication(replication);
        if (result == 0) {
            if (success_array.count < SUCCESS_ARRAY_ELEMENT_MAX) {
                success_array.replications[success_array.count++] = replication;
            }
        } else if (!(result == EINPROGRESS || result == EAGAIN)) {
            if (result != replication->connection_info.last_errno
                    || replication->connection_info.fail_count % 1 == 0)
            {
                replication->connection_info.last_errno = result;
                logError("file: "__FILE__", line: %d, "
                        "%dth connect to %s:%d fail, time used: %ds, "
                        "errno: %d, error info: %s", __LINE__,
                        replication->connection_info.fail_count + 1,
                        replication->connection_info.conn.ip_addr,
                        replication->connection_info.conn.port, (int)
                        (g_current_time - replication->connection_info.start_time),
                        result, STRERROR(result));
            }
            replication->connection_info.fail_count++;
        }
    }

    for (i=0; i<success_array.count; i++) {
        replication = success_array.replications[i];

        if (replication->connection_info.fail_count > 0) {
            sprintf(prompt, " after %d retries",
                    replication->connection_info.fail_count);
        } else {
            *prompt = '\0';
        }
        logInfo("file: "__FILE__", line: %d, "
                "connect to slave %s:%d successfully%s.", __LINE__,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port, prompt);

        if (remove_from_replication_ptr_array(&server_ctx->
                    cluster.connectings, replication) == 0)
        {
            add_to_replication_ptr_array(&server_ctx->
                    cluster.connected, replication);
        }

        replication->connection_info.fail_count = 0;
        replication->task->event.fd = replication->
            connection_info.conn.sock;
        snprintf(replication->task->client_ip,
                sizeof(replication->task->client_ip), "%s",
                replication->connection_info.conn.ip_addr);
        sf_nio_notify(replication->task, SF_NIO_STAGE_INIT);
    }
    return 0;
}

static void repush_to_replication_queue(FDIRSlaveReplication *replication,
        ServerBinlogRecordBuffer *head, ServerBinlogRecordBuffer *tail)
{
    pthread_mutex_lock(&replication->queue.lock);
    tail->nexts[replication->index] = replication->queue.head;
    replication->queue.head = head;
    if (replication->queue.tail == NULL) {
        replication->queue.tail = tail;
    }
    pthread_mutex_unlock(&replication->queue.lock);
}

static int sync_binlog_record_to_slave(FDIRSlaveReplication *replication)
{
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;

    if (!(replication->task->offset == 0 && replication->task->length == 0)) {
        return 0;
    }

    pthread_mutex_lock(&replication->queue.lock);
    head = replication->queue.head;
    tail = replication->queue.tail;
    if (replication->queue.head != NULL) {
        replication->queue.head = replication->queue.tail = NULL;
    }
    pthread_mutex_unlock(&replication->queue.lock);

    if (head == NULL) {
        return 0;
    }

    replication->task->length = sizeof(FDIRProtoHeader);
    while (head != NULL) {
        rb = head;

        if (replication->task->length + rb->buffer.length >
                replication->task->size)
        {
            break;
        }

        memcpy(replication->task->data + replication->task->length,
                rb->buffer.data, rb->buffer.length);
        replication->task->length += rb->buffer.length;

        head = head->nexts[replication->index];
        //DECREASE_TASK_WAITING_RPC_COUNT(rb);
        server_binlog_release_rbuffer(rb);
    }

    //TODO send data

    if (head != NULL) {
        repush_to_replication_queue(replication, head, tail);
    }

    return 0;
}

static int deal_connected_replication(FDIRSlaveReplication *replication)
{
    if (0) {
        return sync_binlog_record_to_slave(replication);
    } else {
        replication_queue_discard(replication);
        return 0;
    }
}

static int deal_replication_connected(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int i;

    if (server_ctx->cluster.connected.count == 0) {
        return 0;
    }

    for (i=0; i<server_ctx->cluster.connected.count; i++) {
        replication = server_ctx->cluster.connected.replications[i];
        if (deal_connected_replication(replication) != 0) {
        }
    }

    return 0;
}

int binlog_replication_process(FDIRServerContext *server_ctx)
{
    int result;
    static int count = 0;

    if (++count % 100 == 0) {
        logInfo("file: "__FILE__", line: %d, count: %d, g_current_time: %d",
                __LINE__, count, (int)g_current_time);
    }

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}

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
#include "binlog_read_thread.h"
#include "binlog_replication.h"

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

    replication->stage = FDIR_REPLICATION_STAGE_NONE;
    replication->context.last_data_versions.by_queue = DATA_CURRENT_VERSION;
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

    pthread_mutex_lock(&replication->context.queue.lock);
    current = replication->context.queue.head;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    pthread_mutex_unlock(&replication->context.queue.lock);

    while (current != NULL) {
        rb = current;
        current = current->nexts[replication->index];

        replication->context.last_data_versions.by_queue = rb->data_version;
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
            replication->stage = FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP;
        } else if (result == EINPROGRESS) {
            replication->stage = FDIR_REPLICATION_STAGE_CONNECTING;
        } else if (result != EAGAIN) {
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
            replication->slave->last_data_version = -1;
            replication->slave->binlog_pos_hint.index = -1;
            replication->slave->binlog_pos_hint.offset = -1;

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
    pthread_mutex_lock(&replication->context.queue.lock);
    tail->nexts[replication->index] = replication->context.queue.head;
    replication->context.queue.head = head;
    if (replication->context.queue.tail == NULL) {
        replication->context.queue.tail = tail;
    }
    pthread_mutex_unlock(&replication->context.queue.lock);
}

static int sync_binlog_from_queue(FDIRSlaveReplication *replication)
{
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;

    pthread_mutex_lock(&replication->context.queue.lock);
    head = replication->context.queue.head;
    tail = replication->context.queue.tail;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    pthread_mutex_unlock(&replication->context.queue.lock);

    if (head == NULL) {
        static int count = 0;
        if (++count % 100 == 0) {
            logInfo("empty queue");
        }
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

        replication->context.last_data_versions.by_queue = rb->data_version;
        memcpy(replication->task->data + replication->task->length,
                rb->buffer.data, rb->buffer.length);
        replication->task->length += rb->buffer.length;

        head = head->nexts[replication->index];
        server_binlog_release_rbuffer(rb);
    }

    FDIR_PROTO_SET_HEADER((FDIRProtoHeader *)replication->task->data,
            FDIR_CLUSTER_PROTO_MASTER_PUSH_BINLOG_REQ,
            replication->task->length - sizeof(FDIRProtoHeader));
    sf_send_add_event(replication->task);

    if (head != NULL) {
        repush_to_replication_queue(replication, head, tail);
    }
    return 0;
}

static int start_binlog_read_thread(FDIRSlaveReplication *replication)
{
    replication->context.reader_ctx = (BinlogReadThreadContext *)malloc(
            sizeof(BinlogReadThreadContext));
    if (replication->context.reader_ctx == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__,
                (int)sizeof(BinlogReadThreadContext));
        return ENOMEM;
    }

    return binlog_read_thread_init(replication->context.reader_ctx,
            &replication->slave->binlog_pos_hint, 
            replication->slave->last_data_version,
            replication->task->size - sizeof(FDIRProtoHeader));
}

static void sync_binlog_to_slave(FDIRSlaveReplication *replication,
        BufferInfo *buffer)
{
    FDIR_PROTO_SET_HEADER((FDIRProtoHeader *)replication->task->data,
            FDIR_CLUSTER_PROTO_MASTER_PUSH_BINLOG_REQ, buffer->length);
    memcpy(replication->task->data + sizeof(FDIRProtoHeader),
            buffer->buff, buffer->length);
    replication->task->length = sizeof(FDIRProtoHeader) + buffer->length;
    sf_send_add_event(replication->task);
}

static int sync_binlog_from_disk(FDIRSlaveReplication *replication)
{
    BinlogReadThreadResult *r;
    const bool block = false;

    r = binlog_read_thread_fetch_result_ex(replication->context.
            reader_ctx, block);
    if (r == NULL) {
        return 0;
    }

    logInfo("r: %p, buffer length: %d, result: %d, last_data_version: %"PRId64
            ", task offset: %d, length: %d", r, r->buffer.length,
            r->err_no, r->last_data_version, replication->task->offset,
            replication->task->length);

    if (r->err_no == 0) {
        replication->context.last_data_versions.by_disk =
            r->last_data_version;
        sync_binlog_to_slave(replication, &r->buffer);
    }
    binlog_read_thread_return_result_buffer(replication->context.reader_ctx, r);

    if (r->err_no == ENOENT && replication->context.last_data_versions.
            by_queue <= replication->context.last_data_versions.by_disk)
    {
        binlog_read_thread_terminate(replication->context.reader_ctx);
        free(replication->context.reader_ctx);
        replication->context.reader_ctx = NULL;

        replication->stage = FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE;
    }
    return 0;
}

static int deal_connected_replication(FDIRSlaveReplication *replication)
{
    int result;

    //logInfo("replication stage: %d", replication->stage);

    if (replication->stage != FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        replication_queue_discard(replication);
    }

    if (replication->stage == FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP) {
        if (replication->slave->last_data_version < 0 ||
                replication->slave->binlog_pos_hint.index < 0 ||
                replication->slave->binlog_pos_hint.offset < 0)
        {
            return 0;
        }

        replication->context.last_data_versions.by_resp = 0;
        replication->context.last_data_versions.by_disk =
            replication->slave->last_data_version;
        if ((result=start_binlog_read_thread(replication)) == 0) {
            replication->stage = FDIR_REPLICATION_STAGE_SYNC_FROM_DISK;
        }
        return result;
    }

    if (!(replication->task->offset == 0 && replication->task->length == 0)) {
        return 0;
    }

    if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_DISK) {
        if (replication->context.last_data_versions.by_resp == 0 ||
                replication->context.last_data_versions.by_resp >=
                replication->context.last_data_versions.by_disk)
        {
            return sync_binlog_from_disk(replication);
        }
    } else if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        return sync_binlog_from_queue(replication);
    }

    return 0;
}

static int deal_replication_connected(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int i;

    static int count = 0;

    if (++count % 1000 == 0) {
        logInfo("server_ctx %p, connected.count: %d", server_ctx, server_ctx->cluster.connected.count);
    }

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

    if (++count % 1000 == 0) {
        logInfo("file: "__FILE__", line: %d, count: %d, g_current_time: %d",
                __LINE__, count, (int)g_current_time);
    }

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}

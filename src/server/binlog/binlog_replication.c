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
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "sf/sf_nio.h"
#include "../../common/fdir_proto.h"
#include "../server_global.h"
#include "../cluster_info.h"
#include "binlog_func.h"
#include "binlog_pack.h"
#include "push_result_ring.h"
#include "binlog_producer.h"
#include "binlog_read_thread.h"
#include "binlog_replication.h"

static void replication_queue_discard_all(FDIRSlaveReplication *replication);

static int check_alloc_ptr_array(FDIRSlaveReplicationPtrArray *array)
{
    int bytes;

    if (array->replications != NULL) {
        return 0;
    }

    bytes = sizeof(FDIRSlaveReplication *) * CLUSTER_SERVER_ARRAY.count;
    array->replications = (FDIRSlaveReplication **)fc_malloc(bytes);
    if (array->replications == NULL) {
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
        logError("file: "__FILE__", line: %d, "
                "can't found replication slave id: %d",
                __LINE__, replication->slave->server->id);
        return ENOENT;
    }

    for (i=i+1; i<array->count; i++) {
        array->replications[i - 1] = array->replications[i];
    }
    array->count--;
    return 0;
}

static inline void set_replication_stage(FDIRSlaveReplication *
        replication, const int stage)
{
    int status;
    switch (stage) {
        case FDIR_REPLICATION_STAGE_NONE:
            status = __sync_add_and_fetch(&replication->slave->status, 0);
            if (status == FDIR_SERVER_STATUS_SYNCING ||
                    status == FDIR_SERVER_STATUS_ACTIVE)
            {
                cluster_info_set_status(replication->slave,
                        FDIR_SERVER_STATUS_OFFLINE);
            }
            break;

        case FDIR_REPLICATION_STAGE_SYNC_FROM_DISK:
            status = __sync_add_and_fetch(&replication->slave->status, 0);
            if (status == FDIR_SERVER_STATUS_INIT) {
                cluster_info_set_status(replication->slave,
                        FDIR_SERVER_STATUS_BUILDING);
            } else if (status != FDIR_SERVER_STATUS_BUILDING)
            {
                cluster_info_set_status(replication->slave,
                        FDIR_SERVER_STATUS_SYNCING);
            }
            break;

        case FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE:
            cluster_info_set_status(replication->slave,
                    FDIR_SERVER_STATUS_ACTIVE);
            break;

        default:
            break;
    }
    replication->stage = stage;
}

int binlog_replication_bind_thread(FDIRSlaveReplication *replication)
{
    int result;
    int alloc_size;
    struct fast_task_info *task;
    FDIRServerContext *server_ctx;

    if ((task=sf_alloc_init_task(&CLUSTER_SF_CTX, -1)) == NULL) {
        return ENOMEM;
    }

    alloc_size = 4 * task->size / BINLOG_RECORD_MIN_SIZE;
    if ((result=push_result_ring_check_init(&replication->
                    context.push_result_ctx, alloc_size)) != 0)
    {
        sf_release_task(task);
        return result;
    }

    task->thread_data = CLUSTER_SF_CTX.thread_data +
        replication->index % CLUSTER_SF_CTX.work_threads;

    set_replication_stage(replication, FDIR_REPLICATION_STAGE_NONE);
    replication->context.last_data_versions.by_disk.previous = 0;
    replication->context.last_data_versions.by_disk.current = 0;
    replication->context.last_data_versions.by_queue = 0;
    replication->context.last_data_versions.by_resp = 0;

    replication->context.sync_by_disk_stat.start_time_ms = 0;
    replication->context.sync_by_disk_stat.binlog_size = 0;
    replication->context.sync_by_disk_stat.record_count = 0;

    SERVER_TASK_TYPE = FDIR_SERVER_TASK_TYPE_REPLICA_MASTER;
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
        replication_queue_discard_all(replication);
        push_result_ring_clear_all(&replication->context.push_result_ctx);
        if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
            result = binlog_replication_bind_thread(replication);
        }
    }

    return result;
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
        if ((result=conn_pool_async_connect_server(&replication->
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
    SF_PROTO_SET_HEADER(header, FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    req = (FDIRProtoJoinSlaveReq *)(out_buff + sizeof(FDIRProtoHeader));
    int2buff(CLUSTER_ID, req->cluster_id);
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    int2buff(replication->task->size, req->buffer_size);
    memcpy(req->key, replication->slave->key, FDIR_REPLICA_KEY_SIZE);

    if ((result=tcpsenddata_nb(replication->connection_info.conn.sock,
                    out_buff, sizeof(out_buff), SF_G_NETWORK_TIMEOUT)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "send data to server %s:%u fail, "
                "errno: %d, error info: %s", __LINE__,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port,
                result, STRERROR(result));
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    }

    return result;
}

static void decrease_task_waiting_rpc_count(ServerBinlogRecordBuffer *rb)
{
    struct fast_task_info *task;
    task = (struct fast_task_info *)rb->args;
    if (__sync_sub_and_fetch(&((FDIRServerTaskArg *)task->arg)->
                context.service.waiting_rpc_count, 1) == 0)
    {
        sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
    }
}

static void discard_queue(FDIRSlaveReplication *replication,
        ServerBinlogRecordBuffer *head, ServerBinlogRecordBuffer *tail)
{
    ServerBinlogRecordBuffer *rb;
    while (head != tail) {
        rb = head;
        head = head->nexts[replication->index];

        replication->context.last_data_versions.by_queue =
            rb->data_version.last;
        decrease_task_waiting_rpc_count(rb);
        rb->release_func(rb);
    }
}

static void replication_queue_discard_all(FDIRSlaveReplication *replication)
{
    ServerBinlogRecordBuffer *head;

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    head = replication->context.queue.head;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head != NULL) {
        discard_queue(replication, head, NULL);
    }
}

static void replication_queue_discard_synced(FDIRSlaveReplication *replication)
{
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    do {
        head = replication->context.queue.head;
        if (head == NULL) {
            tail = NULL;
            break;
        }

        tail = head;
        while ((tail != NULL) && (tail->data_version.first <=
                    replication->context.last_data_versions.by_disk.current))
        {
            tail = tail->nexts[replication->index];
        }

        replication->context.queue.head = tail;
        if (tail == NULL) {
            replication->context.queue.tail = NULL;
        }
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head != NULL) {
        discard_queue(replication, head, tail);
    }
}

static int deal_connecting_replication(FDIRSlaveReplication *replication)
{
    int result;

    replication_queue_discard_all(replication);
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
            set_replication_stage(replication,
                    FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP);
        } else if (result == EINPROGRESS) {
            set_replication_stage(replication,
                    FDIR_REPLICATION_STAGE_CONNECTING);
        } else if (result != EAGAIN) {
            if (result != replication->connection_info.last_errno
                    || replication->connection_info.fail_count % 100 == 0)
            {
                replication->connection_info.last_errno = result;
                logError("file: "__FILE__", line: %d, "
                        "%dth connect to %s:%u fail, time used: %ds, "
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
                "connect to slave %s:%u successfully%s.", __LINE__,
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
        replication->task->port = replication->connection_info.conn.port;
        sf_nio_notify(replication->task, SF_NIO_STAGE_INIT);
    }
    return 0;
}

void clean_connected_replications(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int i;

    for (i=0; i<server_ctx->cluster.connected.count; i++) {
        replication = server_ctx->cluster.connected.replications[i];
        ioevent_add_to_deleted_list(replication->task);
    }
}

static void repush_to_replication_queue(FDIRSlaveReplication *replication,
        ServerBinlogRecordBuffer *head, ServerBinlogRecordBuffer *tail)
{
    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    tail->nexts[replication->index] = replication->context.queue.head;
    replication->context.queue.head = head;
    if (replication->context.queue.tail == NULL) {
        replication->context.queue.tail = tail;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);
}

static int sync_binlog_from_queue(FDIRSlaveReplication *replication)
{
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;
    struct fast_task_info *waiting_task;
    FDIRProtoPushBinlogReqBodyHeader *body_header;
    SFVersionRange data_version;
    int body_len;
    int result;

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    head = replication->context.queue.head;
    tail = replication->context.queue.tail;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head == NULL) {
        return 0;
    }

    data_version.first = head->data_version.first;
    data_version.last = head->data_version.last;
    replication->task->length = sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoPushBinlogReqBodyHeader);
    while (head != NULL) {
        rb = head;

        waiting_task = (struct fast_task_info *)rb->args;
        if (replication->task->length + rb->buffer.length >
                replication->task->size)
        {
            break;
        }

        data_version.last = rb->data_version.last;
        replication->context.last_data_versions.by_queue =
            rb->data_version.last;
        memcpy(replication->task->data + replication->task->length,
                rb->buffer.data, rb->buffer.length);
        replication->task->length += rb->buffer.length;

        if ((result=push_result_ring_add(&replication->context.
                        push_result_ctx, &rb->data_version,
                        waiting_task)) != 0)
        {
            sf_terminate_myself();
            return result;
        }

        head = head->nexts[replication->index];
        rb->release_func(rb);
    }

    body_header = (FDIRProtoPushBinlogReqBodyHeader *)
        (replication->task->data + sizeof(FDIRProtoHeader));
    body_len = replication->task->length - sizeof(FDIRProtoHeader);
    int2buff(body_len - sizeof(FDIRProtoPushBinlogReqBodyHeader),
            body_header->binlog_length);
    long2buff(data_version.first, body_header->data_version.first);
    long2buff(data_version.last, body_header->data_version.last);

    SF_PROTO_SET_HEADER((FDIRProtoHeader *)replication->task->data,
            FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ, body_len);
    sf_send_add_event(replication->task);

    if (head != NULL) {
        repush_to_replication_queue(replication, head, tail);
    }
    return 0;
}

static int start_binlog_read_thread(FDIRSlaveReplication *replication)
{
    int result;
    replication->context.reader_ctx = (BinlogReadThreadContext *)fc_malloc(
            sizeof(BinlogReadThreadContext));
    if (replication->context.reader_ctx == NULL) {
        return ENOMEM;
    }

    /*
    logInfo("file: "__FILE__", line: %d, binlog index: %d, offset: %"PRId64,
            __LINE__, replication->slave->binlog_pos_hint.index,
            replication->slave->binlog_pos_hint.offset);
            */

    if ((result=free_queue_realloc_max_buffer(replication->task)) != 0) {
        return result;
    }
    return binlog_read_thread_init(replication->context.reader_ctx,
            &replication->slave->binlog_pos_hint, 
            replication->slave->last_data_version,
            replication->task->size - (sizeof(FDIRProtoHeader) +
                sizeof(FDIRProtoPushBinlogReqBodyHeader)));
}

int binlog_replications_check_response_data_version(
        FDIRSlaveReplication *replication,
        const int64_t data_version)
{
    if (data_version > replication->context.last_data_versions.by_resp) {
        replication->context.last_data_versions.by_resp = data_version;
    }

    if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        return push_result_ring_remove(&replication->context.
                push_result_ctx, data_version);
    } else if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_DISK) {
        replication->context.sync_by_disk_stat.record_count++;
    }

    return 0;
}

static void sync_binlog_to_slave(FDIRSlaveReplication *replication,
        BinlogReadThreadResult *r)
{
    int body_len;
    FDIRProtoPushBinlogReqBodyHeader *body_header;

    body_header = (FDIRProtoPushBinlogReqBodyHeader *)
        (replication->task->data + sizeof(FDIRProtoHeader));
    body_len = sizeof(FDIRProtoPushBinlogReqBodyHeader) + r->buffer.length;
    SF_PROTO_SET_HEADER((FDIRProtoHeader *)replication->task->data,
            FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ, body_len);

    int2buff(r->buffer.length, body_header->binlog_length);
    long2buff(r->data_version.first, body_header->data_version.first);
    long2buff(r->data_version.last, body_header->data_version.last);
    memcpy(replication->task->data + sizeof(FDIRProtoHeader) +
            sizeof(FDIRProtoPushBinlogReqBodyHeader),
            r->buffer.buff, r->buffer.length);
    replication->task->length = sizeof(FDIRProtoHeader) + body_len;
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

    /*
    logInfo("r: %p, buffer length: %d, result: %d, last_data_version: %"PRId64
            ", task offset: %d, length: %d", r, r->buffer.length,
            r->err_no, r->data_version.last, replication->task->offset,
            replication->task->length);
            */

    if (r->err_no == 0) {
        if (r->data_version.last > replication->context.
                last_data_versions.by_disk.current)
        {
            replication->context.last_data_versions.by_disk.previous =
                replication->context.last_data_versions.by_disk.current;
            replication->context.last_data_versions.by_disk.current =
                r->data_version.last;
        }

        /*
        logInfo("first data_version: %"PRId64", last data_version: %"PRId64
                ", buffer length: %d", r->data_version.first,
                r->data_version.last, r->buffer.length);
                */

        replication->context.sync_by_disk_stat.binlog_size += r->buffer.length;
        sync_binlog_to_slave(replication, r);
    }
    binlog_read_thread_return_result_buffer(replication->context.reader_ctx, r);

    if ((r->err_no == ENOENT) && (replication->context.last_data_versions.
            by_queue <= replication->context.last_data_versions.by_disk.current)
            && (replication->context.last_data_versions.by_resp >=
            replication->context.last_data_versions.by_disk.current))
    {
        int64_t time_used;
        char time_buff[32];
        char size_buff[32];

        binlog_read_thread_terminate(replication->context.reader_ctx);
        free(replication->context.reader_ctx);
        replication->context.reader_ctx = NULL;

        if (replication->context.last_data_versions.by_disk.current > 0) {
            replication_queue_discard_synced(replication);
        }
        set_replication_stage(replication,
                FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE);

        time_used = get_current_time_ms() - replication->context.
            sync_by_disk_stat.start_time_ms;
        logInfo("file: "__FILE__", line: %d, "
                "sync to slave %s:%u by disk done, record count: %"PRId64", "
                "binlog size: %s, time used: %s ms", __LINE__,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(replication->slave->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(replication->slave->server),
                replication->context.sync_by_disk_stat.record_count,
                long_to_comma_str(replication->context.sync_by_disk_stat.
                    binlog_size, size_buff),
                long_to_comma_str(time_used, time_buff));
    }
    return 0;
}

static int deal_connected_replication(FDIRSlaveReplication *replication)
{
    int result;

    //logInfo("replication stage: %d", replication->stage);

    if (replication->stage != FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        replication_queue_discard_all(replication);
    }

    if (replication->stage == FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP) {
        if (replication->slave->last_data_version < 0 ||
                replication->slave->binlog_pos_hint.index < 0 ||
                replication->slave->binlog_pos_hint.offset < 0)
        {
            return 0;
        }

        if ((result=start_binlog_read_thread(replication)) == 0) {
            set_replication_stage(replication,
                    FDIR_REPLICATION_STAGE_SYNC_FROM_DISK);
            replication->context.sync_by_disk_stat.start_time_ms =
                get_current_time_ms();
        }
        return result;
    }

    if (!(replication->task->offset == 0 && replication->task->length == 0)) {
        return 0;
    }

    if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_DISK) {
        if (replication->context.last_data_versions.by_resp >=
                replication->context.last_data_versions.by_disk.previous) //flow control
        {
            return sync_binlog_from_disk(replication);
        }
    } else if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        push_result_ring_clear_timeouts(&replication->context.push_result_ctx);
        return sync_binlog_from_queue(replication);
    }

    return 0;
}

static int deal_replication_connected(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int i;

    /*
    static int count = 0;

    if (++count % 10000 == 0) {
        logInfo("server_ctx %p, connected.count: %d", server_ctx,
        server_ctx->cluster.connected.count);
    }
    */

    if (server_ctx->cluster.connected.count == 0) {
        return 0;
    }

    for (i=0; i<server_ctx->cluster.connected.count; i++) {
        replication = server_ctx->cluster.connected.replications[i];
        if (deal_connected_replication(replication) != 0) {
            ioevent_add_to_deleted_list(replication->task);
        }
    }

    return 0;
}

int binlog_replication_process(FDIRServerContext *server_ctx)
{
    int result;

    /*
    static int count = 0;

    if (++count % 10000 == 0) {
        logInfo("file: "__FILE__", line: %d, count: %d, g_current_time: %d",
                __LINE__, count, (int)g_current_time);
    }
    */

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}

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
#include "binlog_producer.h"
#include "binlog_read_thread.h"
#include "binlog_replication.h"

static void replication_queue_discard_all(FDIRSlaveReplication *replication);

static inline void add_to_replication_ptr_array(FDIRSlaveReplicationPtrArray *
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

static inline void terminate_binlog_read_thread(
        FDIRSlaveReplication *replication)
{
    if (replication->context.reader_ctx != NULL) {
        binlog_read_thread_terminate(replication->context.reader_ctx);
        free(replication->context.reader_ctx);
        replication->context.reader_ctx = NULL;
    }
}

static inline void set_replication_stage(FDIRSlaveReplication *
        replication, const int stage)
{
    int status;
    switch (stage) {
        case FDIR_REPLICATION_STAGE_NONE:
        case FDIR_REPLICATION_STAGE_IN_QUEUE:
            status = FC_ATOMIC_GET(replication->slave->status);
            if (status == FDIR_SERVER_STATUS_SYNCING ||
                    status == FDIR_SERVER_STATUS_ACTIVE)
            {
                cluster_info_set_status(replication->slave,
                        FDIR_SERVER_STATUS_OFFLINE);
            }
            break;

        case FDIR_REPLICATION_STAGE_SYNC_FROM_DISK:
            status = FC_ATOMIC_GET(replication->slave->status);
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
            replication->connection_info.send_heartbeat = false;
            cluster_info_set_status(replication->slave,
                    FDIR_SERVER_STATUS_ACTIVE);
            break;

        default:
            break;
    }

    FC_ATOMIC_SET(replication->stage, stage);
}

int binlog_replication_bind_thread(FDIRSlaveReplication *replication)
{
    int alloc_size;
    int bytes;
    struct nio_thread_data *thread_data;
    FDIRServerContext *server_ctx;

    alloc_size = 4 * g_sf_global_vars.max_buff_size /
        FDIR_BINLOG_RECORD_MIN_SIZE;
    bytes = sizeof(FDIRReplicaRPCResultEntry) * alloc_size;
    if ((replication->context.rpc_result_array.results=
                fc_malloc(bytes)) == NULL)
    {
        return ENOMEM;
    }
    replication->context.rpc_result_array.alloc = alloc_size;

    thread_data = CLUSTER_SF_CTX.thread_data + replication->
        index % CLUSTER_SF_CTX.work_threads;
    server_ctx = (FDIRServerContext *)thread_data->arg;

    set_replication_stage(replication, FDIR_REPLICATION_STAGE_IN_QUEUE);
    replication->context.last_data_versions.by_disk.previous = 0;
    replication->context.last_data_versions.by_disk.current = 0;
    replication->context.last_data_versions.by_queue = 0;
    replication->context.last_data_versions.by_resp = 0;

    replication->context.sync_by_disk_stat.start_time_ms = 0;
    replication->context.sync_by_disk_stat.binlog_size = 0;
    replication->context.sync_by_disk_stat.record_count = 0;
    replication->task = NULL;

    PTHREAD_MUTEX_LOCK(&server_ctx->cluster.queue.lock);
    replication->next = server_ctx->cluster.queue.head;
    server_ctx->cluster.queue.head = replication;
    PTHREAD_MUTEX_UNLOCK(&server_ctx->cluster.queue.lock);

    return 0;
}

int binlog_replication_rebind_thread(FDIRSlaveReplication *replication)
{
    int result;
    int stage;
    FDIRServerContext *server_ctx;
    FDIRSlaveReplicationPtrArray *parray;

    terminate_binlog_read_thread(replication);
    server_ctx = (FDIRServerContext *)replication->task->thread_data->arg;
    stage = FC_ATOMIC_GET(replication->stage);
    if (stage < FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP) {
        parray = &server_ctx->cluster.connectings;
    } else {
        parray = &server_ctx->cluster.connected;
    }
    if ((result=remove_from_replication_ptr_array(parray, replication)) == 0) {
        replication_queue_discard_all(replication);
        if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
            result = binlog_replication_bind_thread(replication);
        } else {
            FC_ATOMIC_SET(replication->stage, FDIR_REPLICATION_STAGE_NONE);
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
    struct fast_task_info *task;
    FCAddressPtrArray *addr_array;
    FCAddressInfo *addr;

    if (FC_ATOMIC_GET(replication->stage) !=
            FDIR_REPLICATION_STAGE_BEFORE_CONNECT)
    {
        return 0;
    }

    if (replication->connection_info.next_connect_time > g_current_time) {
        return EAGAIN;
    }

    if ((task=sf_alloc_init_task(CLUSTER_NET_HANDLER, -1)) == NULL) {
        return ENOMEM;
    }

    task->thread_data = CLUSTER_SF_CTX.thread_data +
        replication->index % CLUSTER_SF_CTX.work_threads;

    addr_array = &CLUSTER_GROUP_ADDRESS_ARRAY(replication->slave->server);
    addr = addr_array->addrs[addr_array->index++];
    if (addr_array->index >= addr_array->count) {
        addr_array->index = 0;
    }

    replication->connection_info.start_time = g_current_time;
    calc_next_connect_time(replication);

    SERVER_TASK_TYPE = FDIR_SERVER_TASK_TYPE_REPLICA_MASTER;
    CLUSTER_REPLICA = replication;
    replication->task = task;
    snprintf(task->server_ip, sizeof(task->server_ip),
            "%s", addr->conn.ip_addr);
    task->port = addr->conn.port;
    if (sf_nio_notify(task, SF_NIO_STAGE_CONNECT) == 0) {
        set_replication_stage(replication, FDIR_REPLICATION_STAGE_CONNECTING);
    }

    return 0;
}

static void on_connect_success(FDIRSlaveReplication *replication)
{
    char prompt[128];
    FDIRServerContext *server_ctx;

    server_ctx = (FDIRServerContext *)replication->task->thread_data->arg;
    if (remove_from_replication_ptr_array(&server_ctx->
                cluster.connectings, replication) == 0)
    {
        set_replication_stage(replication,
                FDIR_REPLICATION_STAGE_WAITING_JOIN_RESP);

        replication->slave->last_data_version = -1;
        replication->slave->binlog_pos_hint.index = -1;
        replication->slave->binlog_pos_hint.offset = -1;
        add_to_replication_ptr_array(&server_ctx->
                cluster.connected, replication);
    }

    if (replication->connection_info.fail_count > 0) {
        sprintf(prompt, " after %d retries",
                replication->connection_info.fail_count);
    } else {
        *prompt = '\0';
    }
    replication->connection_info.fail_count = 0;

    logInfo("file: "__FILE__", line: %d, "
            "cluster thread #%d, connect to slave id %d, %s:%u "
            "successfully%s. current connected count: %d",
            __LINE__, server_ctx->thread_index, replication->slave->
            server->id, replication->task->server_ip, replication->
            task->port, prompt, server_ctx->cluster.connected.count);
}

int binlog_replication_join_slave(struct fast_task_info *task)
{
    FDIRProtoJoinSlaveReq *req;
    FDIRSlaveReplication *replication;

    /* set magic number for the first request */
    SF_PROTO_SET_MAGIC(((FDIRProtoHeader *)task->send.ptr->data)->magic);

    RESPONSE.error.length = 0;
    RESPONSE.header.cmd = FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ;
    TASK_CTX.common.need_response = true;
    TASK_CTX.common.log_level = LOG_ERR;
    if ((replication=CLUSTER_REPLICA) == NULL) {
        TASK_CTX.common.response_done = false;
        return ENOENT;
    }

    req = (FDIRProtoJoinSlaveReq *)SF_PROTO_SEND_BODY(task);
    int2buff(CLUSTER_ID, req->cluster_id);
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    int2buff(task->send.ptr->size, req->buffer_size);
    memcpy(req->key, replication->slave->key, FDIR_REPLICA_KEY_SIZE);

    RESPONSE.header.body_len = sizeof(FDIRProtoJoinSlaveReq);
    TASK_CTX.common.response_done = true;
    on_connect_success(replication);
    return 0;
}

static void decrease_task_waiting_rpc_count(ServerBinlogRecordBuffer *rb)
{
    struct fast_task_info *task;
    task = (struct fast_task_info *)rb->args;
    if (__sync_sub_and_fetch(&((FDIRServerTaskArg *)task->arg)->
                context.service.rpc.waiting_count, 1) == 0)
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

static inline int deal_connecting_replication(FDIRSlaveReplication *replication)
{
    replication_queue_discard_all(replication);
    return check_and_make_replica_connection(replication);
}

void binlog_replication_connect_done(struct fast_task_info *task,
        const int err_no)
{
    FDIRSlaveReplication *replication;

    if ((replication=CLUSTER_REPLICA) == NULL) {
        return;
    }

    if (err_no == 0) {
        return;
    }

    set_replication_stage(replication, FDIR_REPLICATION_STAGE_IN_QUEUE);
    if (err_no != replication->connection_info.last_errno
            || replication->connection_info.fail_count % 10 == 0)
    {
        replication->connection_info.last_errno = err_no;
        logError("file: "__FILE__", line: %d, "
                "%dth connect to replication peer: %d, %s:%u fail, "
                "time used: %ds, errno: %d, error info: %s",
                __LINE__, replication->connection_info.fail_count + 1,
                replication->slave->server->id, task->server_ip, task->port,
                (int)(g_current_time - replication->connection_info.
                    start_time), err_no, STRERROR(err_no));
    }
    replication->connection_info.fail_count++;
    FC_ATOMIC_SET(replication->stage, FDIR_REPLICATION_STAGE_BEFORE_CONNECT);
}

static int deal_replication_connectings(FDIRServerContext *server_ctx)
{
    int i;
    FDIRSlaveReplication *replication;

    if (server_ctx->cluster.connectings.count == 0) {
        return 0;
    }

    for (i=0; i<server_ctx->cluster.connectings.count; i++) {
        replication = server_ctx->cluster.connectings.replications[i];
        deal_connecting_replication(replication);
    }

    return 0;
}

static void clean_connecting_replications(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int i;

    for (i=0; i<server_ctx->cluster.connectings.count; i++) {
        replication = server_ctx->cluster.connectings.replications[i];
        if (replication->task != NULL) {
            ioevent_add_to_deleted_list(replication->task);
        }
    }
    server_ctx->cluster.connectings.count = 0;
}

static void clean_connected_replications(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int i;

    for (i=0; i<server_ctx->cluster.connected.count; i++) {
        replication = server_ctx->cluster.connected.replications[i];
        ioevent_add_to_deleted_list(replication->task);
    }
}

void clean_master_replications(FDIRServerContext *server_ctx)
{
    FDIRSlaveReplication *replication;
    int count;

    count = 0;
    PTHREAD_MUTEX_LOCK(&server_ctx->cluster.queue.lock);
    if (server_ctx->cluster.queue.head != NULL) {
        replication = server_ctx->cluster.queue.head;
        do {
            ++count;
            set_replication_stage(replication, FDIR_REPLICATION_STAGE_NONE);
        } while ((replication=replication->next) != NULL);

        server_ctx->cluster.queue.head = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&server_ctx->cluster.queue.lock);

    count += server_ctx->cluster.connectings.count;
    count += server_ctx->cluster.connected.count;

    clean_connecting_replications(server_ctx);
    clean_connected_replications(server_ctx);
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

static int forward_requests(FDIRSlaveReplication *replication)
{
    struct fast_task_info *task;
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;
    FDIRReplicaRPCResultEntry *rentry;
    FDIRProtoHeader *header;
    FDIRProtoForwardRequestsBodyHeader *bheader;
    SFRequestMetadata *metadata;
    SFRequestMetadata *metaend;
    FDIRProtoForwardRequestMetadata *pmeta;
    SFVersionRange data_version;
    int binlog_length;
    int body_len;

    task = replication->task;
    if (TASK_PENDING_SEND_COUNT > 0) {
        return 0;
    }

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    head = replication->context.queue.head;
    tail = replication->context.queue.tail;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head == NULL) {
        if (replication->connection_info.send_heartbeat) {
            replication->connection_info.send_heartbeat = false;
            task->send.ptr->length = sizeof(FDIRProtoHeader);
            SF_PROTO_SET_HEADER((FDIRProtoHeader *)task->send.ptr->data,
                    SF_PROTO_ACTIVE_TEST_REQ, 0);
            sf_send_add_event(task);
        }

        return 0;
    }

    ++TASK_PENDING_SEND_COUNT;
    rentry = replication->context.rpc_result_array.results;
    metadata = replication->req_meta_array.elts;
    replication->req_meta_array.count = 0;
    data_version.first = head->data_version.first;
    data_version.last = head->data_version.last;
    task->send.ptr->length = sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoForwardRequestsBodyHeader);
    while (head != NULL) {
        rb = head;

        if (task->send.ptr->length + rb->buffer.length + sizeof(
                    *pmeta) * (replication->req_meta_array.count + 1) >
                task->send.ptr->size || rentry - replication->context.
                rpc_result_array.results == replication->context.
                rpc_result_array.alloc)
        {
            break;
        }

        data_version.last = rb->data_version.last;
        replication->context.last_data_versions.by_queue =
            rb->data_version.last;
        memcpy(task->send.ptr->data + task->send.ptr->length,
                rb->buffer.data, rb->buffer.length);
        task->send.ptr->length += rb->buffer.length;

        rentry->data_version = rb->data_version.last;
        rentry->waiting_task = rb->args;
        ++rentry;

        metadata->req_id = rb->req_id;
        metadata->data_version = rb->data_version.last;

        head = head->nexts[replication->index];
        rb->release_func(rb);

        ++metadata;
        if (++replication->req_meta_array.count ==
                replication->req_meta_array.alloc)
        {
            break;
        }
    }

    replication->context.rpc_result_array.count = rentry -
        replication->context.rpc_result_array.results;
    binlog_length = task->send.ptr->length - (sizeof(FDIRProtoHeader)
            + sizeof(FDIRProtoForwardRequestsBodyHeader));

    metaend = metadata;
    pmeta = (FDIRProtoForwardRequestMetadata *)(replication->
            task->send.ptr->data + task->send.ptr->length);
    for (metadata=replication->req_meta_array.elts;
            metadata<metaend; metadata++, pmeta++)
    {
        long2buff(metadata->req_id, pmeta->req_id);
        long2buff(metadata->data_version, pmeta->data_version);
    }

    task->send.ptr->length += sizeof(*pmeta) *
        replication->req_meta_array.count;
    header = (FDIRProtoHeader *)task->send.ptr->data;
    bheader = (FDIRProtoForwardRequestsBodyHeader *)
        (task->send.ptr->data + sizeof(FDIRProtoHeader));
    body_len = task->send.ptr->length - sizeof(FDIRProtoHeader);
    int2buff(binlog_length, bheader->binlog_length);
    int2buff(replication->req_meta_array.count, bheader->count);
    long2buff(data_version.first, bheader->data_version.first);
    long2buff(data_version.last, bheader->data_version.last);

    SF_PROTO_SET_HEADER(header, FDIR_REPLICA_PROTO_RPC_CALL_REQ, body_len);
    sf_send_add_event(task);

    if (head != NULL) {
        repush_to_replication_queue(replication, head, tail);
    }
    return 0;
}

static int start_binlog_read_thread(FDIRSlaveReplication *replication)
{
    int result;
    struct fast_task_info *task;

    replication->context.reader_ctx = (BinlogReadThreadContext *)
        fc_malloc(sizeof(BinlogReadThreadContext));
    if (replication->context.reader_ctx == NULL) {
        return ENOMEM;
    }

    /*
    logInfo("file: "__FILE__", line: %d, slave server id: %d, "
            "binlog index: %d, offset: %"PRId64, __LINE__,
            replication->slave->server->id,
            replication->slave->binlog_pos_hint.index,
            replication->slave->binlog_pos_hint.offset);
            */

    task = replication->task;
    if (SF_CTX->realloc_task_buffer) {
        if ((result=sf_set_task_send_max_buffer_size(task)) != 0 ||
                (result=sf_set_task_recv_max_buffer_size(task)) != 0)
        {
            free(replication->context.reader_ctx);
            replication->context.reader_ctx = NULL;
            return result;
        }
    }

    if ((result=binlog_read_thread_init(replication->context.reader_ctx,
                    &replication->slave->binlog_pos_hint,
                    replication->slave->last_data_version,
                    replication->task->send.ptr->size -
                    (sizeof(FDIRProtoHeader) +
                     sizeof(FDIRProtoPushBinlogReqBodyHeader)))) != 0)
    {
        free(replication->context.reader_ctx);
        replication->context.reader_ctx = NULL;
    }

    return result;
}

static void sync_binlog_to_slave(FDIRSlaveReplication *replication,
        BinlogReadThreadResult *r)
{
    int body_len;
    FDIRProtoPushBinlogReqBodyHeader *body_header;

    body_header = (FDIRProtoPushBinlogReqBodyHeader *)
        (replication->task->send.ptr->data + sizeof(FDIRProtoHeader));
    body_len = sizeof(FDIRProtoPushBinlogReqBodyHeader) + r->buffer.length;
    SF_PROTO_SET_HEADER((FDIRProtoHeader *)replication->task->send.ptr->data,
            FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ, body_len);

    int2buff(r->buffer.length, body_header->binlog_length);
    long2buff(r->data_version.first, body_header->data_version.first);
    long2buff(r->data_version.last, body_header->data_version.last);
    memcpy(replication->task->send.ptr->data + sizeof(FDIRProtoHeader) +
            sizeof(FDIRProtoPushBinlogReqBodyHeader),
            r->buffer.buff, r->buffer.length);
    replication->task->send.ptr->length = sizeof(FDIRProtoHeader) + body_len;
    sf_send_add_event(replication->task);
}

static int sync_binlog_from_disk(FDIRSlaveReplication *replication)
{
    struct fast_task_info *task;
    BinlogReadThreadResult *r;
    const bool block = false;

    task = replication->task;
    if (TASK_PENDING_SEND_COUNT > 0) {
        return 0;
    }

    r = binlog_read_thread_fetch_result_ex(replication->context.
            reader_ctx, block);
    if (r == NULL) {
        return 0;
    }

    /*
    logInfo("r: %p, buffer length: %d, result: %d, last_data_version: %"PRId64
            ", task offset: %d, length: %d", r, r->buffer.length,
            r->err_no, r->data_version.last, replication->task->send.ptr->offset,
            replication->task->send.ptr->length);
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

        ++TASK_PENDING_SEND_COUNT;
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

        terminate_binlog_read_thread(replication);
        if (replication->context.last_data_versions.by_disk.current > 0) {
            replication_queue_discard_synced(replication);
        }
        set_replication_stage(replication,
                FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE);

        time_used = get_current_time_ms() - replication->context.
            sync_by_disk_stat.start_time_ms;
        logInfo("file: "__FILE__", line: %d, "
                "sync to slave id %d, %s:%u by disk done, record count: "
                "%"PRId64", binlog size: %s, last data version: %"PRId64","
                "time used: %s ms", __LINE__, replication->slave->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(replication->slave->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(replication->slave->server),
                replication->context.sync_by_disk_stat.record_count,
                long_to_comma_str(replication->context.sync_by_disk_stat.
                    binlog_size, size_buff), replication->context.
                last_data_versions.by_disk.current,
                long_to_comma_str(time_used, time_buff));
    }

    return 0;
}

static int deal_connected_replication(FDIRSlaveReplication *replication)
{
    int result;

    /*
    logInfo("replication slave server id: %d, stage: %d",
            replication->slave->server->id, replication->stage);
            */

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

    if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_DISK) {
        if (replication->context.last_data_versions.by_resp >=
                replication->context.last_data_versions.by_disk.previous) //flow control
        {
            return sync_binlog_from_disk(replication);
        }
    } else if (replication->stage == FDIR_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        return forward_requests(replication);
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
    FDIRSlaveReplication *replication;

    /*
    static int count = 0;

    if (++count % 10000 == 0) {
        logInfo("file: "__FILE__", line: %d, count: %d, g_current_time: %d",
                __LINE__, count, (int)g_current_time);
    }
    */

    PTHREAD_MUTEX_LOCK(&server_ctx->cluster.queue.lock);
    if (server_ctx->cluster.queue.head != NULL) {
        replication = server_ctx->cluster.queue.head;
        do {
            FC_ATOMIC_SET(replication->stage,
                    FDIR_REPLICATION_STAGE_BEFORE_CONNECT);
            add_to_replication_ptr_array(&server_ctx->
                    cluster.connectings, replication);
        } while ((replication=replication->next) != NULL);

        server_ctx->cluster.queue.head = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&server_ctx->cluster.queue.lock);

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}

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

#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "fdir_proto.h"
#include "client_global.h"
#include "client_proto.h"

static inline void init_client_buffer(FDIRClientBuffer *buffer)
{
    buffer->buff = buffer->fixed;
    buffer->size = sizeof(buffer->fixed);
}

int fdir_client_dentry_array_init(FDIRClientDentryArray *array)
{
    array->alloc = array->count = 0;
    array->entries = NULL;
    init_client_buffer(&array->buffer);
    array->name_allocator.used = array->name_allocator.inited = false;
    return 0;
}

void fdir_client_dentry_array_free(FDIRClientDentryArray *array)
{
    if (array->buffer.buff != array->buffer.fixed) {
        free(array->buffer.buff);
        init_client_buffer(&array->buffer);
    }

    if (array->entries != NULL) {
        free(array->entries);
        array->entries = NULL;
        array->alloc = array->count = 0;
    }

    if (array->name_allocator.inited) {
        array->name_allocator.inited = false;
        fast_mpool_destroy(&array->name_allocator.mpool);
    }
}

static int client_check_set_proto_dentry(const FDIRDEntryFullName *fullname,
        FDIRProtoDEntryInfo *entry_proto)
{
    if (fullname->ns.len <= 0 || fullname->ns.len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, fullname->ns.len, NAME_MAX);
        return EINVAL;
    }

    if (fullname->path.len <= 0 || fullname->path.len > PATH_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid path length: %d, which <= 0 or > %d",
                __LINE__, fullname->path.len, PATH_MAX);
        return EINVAL;
    }

    entry_proto->ns_len = fullname->ns.len;
    short2buff(fullname->path.len, entry_proto->path_len);
    memcpy(entry_proto->ns_str, fullname->ns.str, fullname->ns.len);
    memcpy(entry_proto->ns_str + fullname->ns.len,
            fullname->path.str, fullname->path.len);
    return 0;
}

static int client_check_set_proto_pname(const string_t *ns,
        const FDIRDEntryPName *pname, FDIRProtoDEntryByPName *pname_proto)
{
    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    if (pname->name.len <= 0 || pname->name.len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid path length: %d, which <= 0 or > %d",
                __LINE__, pname->name.len, NAME_MAX);
        return EINVAL;
    }

    long2buff(pname->parent_inode, pname_proto->parent_inode);
    pname_proto->ns_len = ns->len;
    pname_proto->name_len = pname->name.len;
    memcpy(pname_proto->ns_str, ns->str, ns->len);
    memcpy(pname_proto->ns_str + ns->len, pname->name.str, pname->name.len);
    return 0;
}

int fdir_client_proto_join_server(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, FDIRConnectionParameters *conn_params)
{
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoClientJoinReq)];
    FDIRProtoHeader *proto_header;
    FDIRProtoClientJoinReq *req;
    FDIRProtoClientJoinResp join_resp;
    SFResponseInfo response;
    int result;
    int flags;

    proto_header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoClientJoinReq *)(proto_header + 1);

    if (client_ctx->idempotency_enabled) {
        flags = FDIR_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST;

        int2buff(__sync_add_and_fetch(&conn_params->channel->id, 0),
                req->idempotency.channel_id);
        int2buff(__sync_add_and_fetch(&conn_params->channel->key, 0),
                req->idempotency.key);
    } else {
        flags = 0;
    }
    int2buff(flags, req->flags);

    SF_PROTO_SET_HEADER(proto_header, FDIR_SERVICE_PROTO_CLIENT_JOIN_REQ,
            sizeof(FDIRProtoClientJoinReq));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_CLIENT_JOIN_RESP, (char *)&join_resp,
                    sizeof(FDIRProtoClientJoinResp))) == 0)
    {
        conn_params->buffer_size = buff2int(join_resp.buffer_size);
    } else {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

static inline void proto_unpack_dentry(FDIRProtoStatDEntryResp *proto_stat,
        FDIRDEntryInfo *dentry)
{
    dentry->inode = buff2long(proto_stat->inode);
    fdir_proto_unpack_dentry_stat(&proto_stat->stat, &dentry->stat);
}

static inline int do_update_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, char *out_buff, const int out_bytes,
        const int expect_cmd, FDIRDEntryInfo *dentry)
{
    SFResponseInfo response;
    FDIRProtoStatDEntryResp proto_stat;
    int result;

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->network_timeout,
                    expect_cmd, (char *)&proto_stat,
                    sizeof(proto_stat))) == 0)
    {
        proto_unpack_dentry(&proto_stat, dentry);
    } else {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

#define CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes) \
    do {   \
        header = (FDIRProtoHeader *)out_buff;  \
        out_bytes = sizeof(FDIRProtoHeader) + sizeof(*req);  \
        if (req_id > 0) {  \
            long2buff(req_id, ((SFProtoIdempotencyAdditionalHeader *)\
                        (header + 1))->req_id);  \
            out_bytes += sizeof(SFProtoIdempotencyAdditionalHeader); \
            req = (typeof(req))((char *)(header + 1) +   \
                    sizeof(SFProtoIdempotencyAdditionalHeader));     \
        } else {  \
            req = (typeof(req))(header + 1);  \
        }  \
    } while (0)

#define CLIENT_PROTO_SET_OMP(omp, proto_front) \
    do { \
        int2buff(omp->uid, proto_front.uid);   \
        int2buff(omp->gid, proto_front.gid);   \
        int2buff(omp->mode, proto_front.mode); \
    } while (0)

int fdir_client_proto_create_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoCreateDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoCreateDEntryReq) + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }

    CLIENT_PROTO_SET_OMP(omp, req->front);
    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP, dentry);
}

int fdir_client_proto_symlink_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *link, const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoSymlinkDEntryReq *req;
    FDIRProtoDEntryInfo *entry_proto;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoSymlinkDEntryReq) +
        NAME_MAX + 2 * PATH_MAX];
    int out_bytes;
    int result;

    if (link->len <= 0 || link->len > PATH_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid link length: %d, which <= 0 or > %d",
                __LINE__, link->len, NAME_MAX);
        return EINVAL;
    }

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    entry_proto = (FDIRProtoDEntryInfo *)(req->front.link_str + link->len);
    if ((result=client_check_set_proto_dentry(fullname, entry_proto)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_OMP(omp, req->front.common);
    short2buff(link->len, req->front.link_len);
    memcpy(req->front.link_str, link->str, link->len);
    out_bytes += fullname->ns.len + fullname->path.len + link->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SYMLINK_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_SYMLINK_DENTRY_RESP, dentry);
}

int fdir_client_proto_remove_dentry_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveDEntry *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoRemoveDEntry) + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }

    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP, dentry);
}

int fdir_client_proto_link_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoHDLinkDEntry *req;
    FDIRProtoDEntryInfo *dest_pentry;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoHDLinkDEntry) +
        2 * (NAME_MAX + PATH_MAX)];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(src, &req->src)) != 0) {
        return result;
    }

    dest_pentry = (FDIRProtoDEntryInfo *)((char *)(&req->src + 1) +
            src->ns.len + src->path.len);
    if ((result=client_check_set_proto_dentry(dest, dest_pentry)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_OMP(omp, req->front);
    out_bytes = ((char *)(dest_pentry + 1) + dest->ns.len +
            dest->path.len) - out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_HDLINK_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_HDLINK_DENTRY_RESP, dentry);
}

int fdir_client_proto_link_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const int64_t src_inode, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoHDLinkDEntryByPName *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoHDLinkDEntryByPName) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->dest)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_OMP(omp, req->front.common);
    long2buff(src_inode, req->front.src_inode);
    out_bytes += ns->len + pname->name.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_RESP, dentry);
}

static int do_rename_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, char *out_buff, const int out_bytes,
        const int expect_cmd, FDIRDEntryInfo **dentry)
{
    SFResponseInfo response;
    FDIRProtoStatDEntryResp proto_stat;
    int expect_body_lens[2];
    int body_len;
    int result;

    expect_body_lens[0] = 0;
    expect_body_lens[1] = sizeof(proto_stat);
    response.error.length = 0;
    if ((result=sf_send_and_recv_response_ex(conn, out_buff, out_bytes,
                    &response, client_ctx->network_timeout, expect_cmd,
                    (char *)&proto_stat, expect_body_lens, 2, &body_len)) == 0)
    {
        if (body_len == (int)sizeof(proto_stat)) {
            proto_unpack_dentry(&proto_stat, *dentry);
        } else {
            *dentry = NULL;
        }
    } else {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_rename_dentry_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const int flags, FDIRDEntryInfo **dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRenameDEntry *req;
    FDIRProtoDEntryInfo *dest_pentry;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoRenameDEntry) +
        2 * (NAME_MAX + PATH_MAX)];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(src, &req->src)) != 0) {
        return result;
    }

    dest_pentry = (FDIRProtoDEntryInfo *)((char *)(&req->src + 1) +
            src->ns.len + src->path.len);
    if ((result=client_check_set_proto_dentry(dest, dest_pentry)) != 0) {
        return result;
    }

    int2buff(flags, req->front.flags);
    out_bytes = ((char *)(dest_pentry + 1) + dest->ns.len +
            dest->path.len) - out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_RENAME_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_rename_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_RENAME_DENTRY_RESP, dentry);
}

int fdir_client_proto_rename_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *src_ns, const FDIRDEntryPName *src_pname,
        const string_t *dest_ns, const FDIRDEntryPName *dest_pname,
        const int flags, FDIRDEntryInfo **dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRenameDEntryByPName *req;
    FDIRProtoDEntryByPName *dest_pentry;
    int out_bytes;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoRenameDEntryByPName) +
        2 * (NAME_MAX + PATH_MAX)];
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(src_ns, src_pname,
                    &req->src)) != 0)
    {
        return result;
    }

    dest_pentry = (FDIRProtoDEntryByPName *)((char *)(&req->src + 1) +
            src_ns->len + src_pname->name.len);
    if ((result=client_check_set_proto_pname(dest_ns, dest_pname,
                    dest_pentry)) != 0)
    {
        return result;
    }

    int2buff(flags, req->front.flags);
    out_bytes = ((char *)(dest_pentry + 1) + dest_ns->len +
            dest_pname->name.len) - out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_RENAME_BY_PNAME_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_rename_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_RENAME_BY_PNAME_RESP, dentry);
}

static int setup_req_by_dentry_fullname(const FDIRDEntryFullName *fullname,
        const int req_cmd, char *out_buff, int *out_bytes)
{
    int result;
    FDIRProtoHeader *header;
    FDIRProtoDEntryInfo *proto_dentry;

    header = (FDIRProtoHeader *)out_buff;
    proto_dentry = (FDIRProtoDEntryInfo *)(header + 1);
    if ((result=client_check_set_proto_dentry(fullname, proto_dentry)) != 0) {
        return result;
    }
    *out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoDEntryInfo)
        + fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, req_cmd, *out_bytes - sizeof(FDIRProtoHeader));
    return 0;
}

static int setup_req_by_dentry_pname(const FDIRDEntryPName *pname,
        const int req_cmd, char *out_buff, int *out_bytes)
{
    FDIRProtoHeader *header;
    FDIRProtoStatDEntryByPNameReq *req;

    if (pname->name.len <= 0 || pname->name.len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid name length: %d, which <= 0 or > %d",
                __LINE__, pname->name.len, NAME_MAX);
        return EINVAL;
    }

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoStatDEntryByPNameReq *)(header + 1);
    long2buff(pname->parent_inode, req->parent_inode);
    req->name_len = pname->name.len;
    memcpy(req->name_str, pname->name.str, pname->name.len);
    *out_bytes = sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoStatDEntryByPNameReq) + pname->name.len;
    SF_PROTO_SET_HEADER(header, req_cmd,
            *out_bytes - sizeof(FDIRProtoHeader));
    return 0;
}

static int query_by_dentry_fullname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int req_cmd, const int resp_cmd, char *in_buff,
        const int in_len, const int enoent_log_level)
{
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoDEntryInfo)
        + NAME_MAX + PATH_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;
    int log_level;

    if ((result=setup_req_by_dentry_fullname(fullname, req_cmd,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->network_timeout,
                    resp_cmd, in_buff, in_len)) != 0)
    {
        log_level = (result == ENOENT) ? enoent_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

int fdir_client_proto_lookup_inode_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int enoent_log_level, int64_t *inode)
{
    FDIRProtoLookupInodeResp proto_resp;
    int result;

    if ((result=query_by_dentry_fullname(client_ctx, conn, fullname,
                    FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_REQ,
                    FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_RESP,
                    (char *)&proto_resp, sizeof(proto_resp),
                    enoent_log_level)) == 0)
    {
        *inode = buff2long(proto_resp.inode);
    } else {
        *inode = -1;
    }

    return result;
}

int fdir_client_proto_lookup_inode_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryPName *pname,
        const int enoent_log_level, int64_t *inode)
{
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoStatDEntryByPNameReq) + NAME_MAX];
    int out_bytes;
    int result;
    SFResponseInfo response;
    FDIRProtoLookupInodeResp proto_resp;
    int log_level;

    if ((result=setup_req_by_dentry_pname(pname,
                    FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        *inode = -1;
        return result;
    }

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_RESP,
                    (char *)&proto_resp, sizeof(proto_resp))) == 0)
    {
        *inode = buff2long(proto_resp.inode);
    } else {
        *inode = -1;
        log_level = (result == ENOENT) ? enoent_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

int fdir_client_proto_stat_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    FDIRProtoStatDEntryResp proto_stat;
    int result;

    if ((result=query_by_dentry_fullname(client_ctx, conn, fullname,
                    FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ,
                    FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP,
                    (char *)&proto_stat, sizeof(proto_stat),
                    enoent_log_level)) == 0)
    {
        proto_unpack_dentry(&proto_stat, dentry);
    }

    return result;
}

static int do_readlink(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, char *out_buff, const int out_bytes,
        const int expect_cmd, string_t *link, const int size)
{
    SFResponseInfo response;
    int result;

    response.error.length = 0;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, client_ctx->network_timeout,
                    expect_cmd)) == 0)
    {
        if (response.header.body_len >= size) {
            logError("file: "__FILE__", line: %d, "
                    "body length: %d exceeds max size: %d",
                    __LINE__, response.header.body_len, size);
            return EOVERFLOW;
        }

        if ((result=tcprecvdata_nb_ex(conn->sock, link->str, response.header.
                body_len, client_ctx->network_timeout, &link->len)) == 0)
        {
            *(link->str + link->len) = '\0';
        }
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_readlink_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        string_t *link, const int size)
{
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoDEntryInfo)
        + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    if ((result=setup_req_by_dentry_fullname(fullname,
                    FDIR_SERVICE_PROTO_READLINK_BY_PATH_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    return do_readlink(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_READLINK_BY_PATH_RESP, link, size);
}

int fdir_client_proto_readlink_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryPName *pname,
        string_t *link, const int size)
{
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoStatDEntryByPNameReq) + NAME_MAX];
    int out_bytes;
    int result;

    if ((result=setup_req_by_dentry_pname(pname,
                    FDIR_SERVICE_PROTO_READLINK_BY_PNAME_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    return do_readlink(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_READLINK_BY_PNAME_RESP, link, size);
}

static inline void setup_req_by_dentry_inode(const int64_t inode,
        const int req_cmd, char *out_buff, int *out_bytes)
{
    FDIRProtoHeader *header;

    header = (FDIRProtoHeader *)out_buff;
    *out_bytes = sizeof(FDIRProtoHeader) + 8;
    SF_PROTO_SET_HEADER(header, req_cmd, 8);
    long2buff(inode, out_buff + sizeof(FDIRProtoHeader));
}

int fdir_client_proto_readlink_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode, string_t *link,
        const int size)
{
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoStatDEntryByPNameReq) + NAME_MAX];
    int out_bytes;

    setup_req_by_dentry_inode(inode, FDIR_SERVICE_PROTO_READLINK_BY_INODE_REQ,
            out_buff, &out_bytes);
    return do_readlink(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_READLINK_BY_INODE_RESP, link, size);
}

static inline int do_stat_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, char *out_buff, const int out_bytes,
        const int expect_cmd, FDIRDEntryInfo *dentry,
        const int enoent_log_level)
{
    SFResponseInfo response;
    FDIRProtoStatDEntryResp proto_stat;
    int result;
    int log_level;

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->network_timeout,
                    expect_cmd, (char *)&proto_stat, sizeof(proto_stat))) == 0)
    {
        proto_unpack_dentry(&proto_stat, dentry);
    } else {
        log_level = (result == ENOENT) ? enoent_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

int fdir_client_proto_stat_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode, FDIRDEntryInfo *dentry)
{
    char out_buff[sizeof(FDIRProtoHeader) + 8];
    int out_bytes;

    setup_req_by_dentry_inode(inode, FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ,
            out_buff, &out_bytes);
    return do_stat_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_STAT_BY_INODE_RESP, dentry, LOG_ERR);
}

int fdir_client_proto_stat_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryPName *pname,
        const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoStatDEntryByPNameReq) + NAME_MAX];
    int out_bytes;
    int result;

    if ((result=setup_req_by_dentry_pname(pname,
                    FDIR_SERVICE_PROTO_STAT_BY_PNAME_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    return do_stat_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_STAT_BY_PNAME_RESP, dentry, enoent_log_level);
}

int fdir_client_proto_create_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoCreateDEntryByPNameReq *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoCreateDEntryByPNameReq) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->pname)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_OMP(omp, req->front);
    out_bytes += ns->len + pname->name.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_CREATE_BY_PNAME_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_CREATE_BY_PNAME_RESP, dentry);
}

int fdir_client_proto_symlink_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *link, const string_t *ns,
        const FDIRDEntryPName *pname, const FDIRClientOwnerModePair *omp,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoSymlinkDEntryByNameReq *req;
    FDIRProtoDEntryByPName *pname_proto;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoSymlinkDEntryByNameReq) +
        2 * NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    if (link->len <= 0 || link->len > PATH_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid link length: %d, which <= 0 or > %d",
                __LINE__, link->len, NAME_MAX);
        return EINVAL;
    }

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    pname_proto = (FDIRProtoDEntryByPName *)(req->front.link_str + link->len);
    if ((result=client_check_set_proto_pname(ns, pname, pname_proto)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_OMP(omp, req->front.common);
    short2buff(link->len, req->front.link_len);
    memcpy(req->front.link_str, link->str, link->len);
    out_bytes += ns->len + pname->name.len + link->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_RESP, dentry);
}

int fdir_client_proto_remove_dentry_by_pname_ex(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveDEntryByPName *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoRemoveDEntryByPName) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->pname)) != 0) {
        return result;
    }
    out_bytes += ns->len + pname->name.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_RESP, dentry);
}

#define FDIR_CLIENT_PROTO_PACK_DENTRY_SIZE(dsize, req) \
    long2buff(dsize->inode, req->inode);         \
    long2buff(dsize->file_size, req->file_size); \
    long2buff(dsize->inc_alloc, req->inc_alloc); \
    int2buff(dsize->flags, req->flags); \
    req->force = dsize->force

int fdir_client_proto_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoSetDentrySizeReq *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoSetDentrySizeReq) + NAME_MAX];
    int out_bytes;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    FDIR_CLIENT_PROTO_PACK_DENTRY_SIZE(dsize, req);
    req->ns_len = ns->len;
    memcpy(req + 1, ns->str, ns->len);
    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_RESP, dentry);
}

int fdir_client_proto_batch_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const FDIRSetDEntrySizeInfo *dsizes, const int count)
{
    FDIRProtoHeader *header;
    FDIRProtoBatchSetDentrySizeReqHeader *rheader;
    FDIRProtoBatchSetDentrySizeReqBody *rbody;
    const FDIRSetDEntrySizeInfo *dsize;
    const FDIRSetDEntrySizeInfo *dsend;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoBatchSetDentrySizeReqHeader) + NAME_MAX +
        FDIR_BATCH_SET_MAX_DENTRY_COUNT *
        sizeof(FDIRProtoBatchSetDentrySizeReqBody)];
    SFResponseInfo response;
    int out_bytes;
    int result;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }
    if (count <= 0 || count > FDIR_BATCH_SET_MAX_DENTRY_COUNT) {
        logError("file: "__FILE__", line: %d, "
                "invalid count: %d, which <= 0 or > %d", __LINE__,
                count, FDIR_BATCH_SET_MAX_DENTRY_COUNT);
        return EINVAL;
    }

    CLIENT_PROTO_SET_REQ(out_buff, header, rheader, req_id, out_bytes);
    int2buff(count, rheader->count);
    rheader->ns_len = ns->len;
    memcpy(rheader + 1, ns->str, ns->len);

    rbody = (FDIRProtoBatchSetDentrySizeReqBody *)(rheader->ns_str + ns->len);
    dsend = dsizes + count;
    for (dsize=dsizes; dsize<dsend; dsize++, rbody++) {
        FDIR_CLIENT_PROTO_PACK_DENTRY_SIZE(dsize, rbody);
    }

    out_bytes = (char *)rbody - out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff,
                    out_bytes, &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_RESP)) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_modify_dentry_stat(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const int64_t inode, const int64_t flags,
        const FDIRDEntryStatus *stat, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoModifyDentryStatReq *req;
    char out_buff[sizeof(FDIRProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FDIRProtoModifyDentryStatReq) + NAME_MAX];
    int out_bytes;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    CLIENT_PROTO_SET_REQ(out_buff, header, req, req_id, out_bytes);
    long2buff(inode, req->inode);
    long2buff(flags, req->mflags);
    req->ns_len = ns->len;
    memcpy(req->ns_str, ns->str, ns->len);
    fdir_proto_pack_dentry_stat(stat, &req->stat);
    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_RESP, dentry);
}

int fdir_client_init_session(FDIRClientContext *client_ctx,
    FDIRClientSession *session)
{
    int result;
    if ((session->mconn=client_ctx->conn_manager.get_master_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    session->ctx = client_ctx;
    return 0;
}

void fdir_client_close_session(FDIRClientSession *session,
        const bool force_close)
{
    if (session->mconn == NULL) {
        return;
    }

    if (force_close) {
        session->ctx->conn_manager.close_connection(
                session->ctx, session->mconn);
    } else if (session->ctx->conn_manager.release_connection != NULL) {
        session->ctx->conn_manager.release_connection(
                session->ctx, session->mconn);
    }
    session->mconn = NULL;
}

int fdir_client_flock_dentry_ex2(FDIRClientSession *session,
        const int64_t inode, const int operation, const int64_t offset,
        const int64_t length, const int64_t owner_id, const pid_t pid)
{
    FDIRProtoHeader *header;
    FDIRProtoFlockDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoFlockDEntryReq)];
    SFResponseInfo response;
    int result;

    if (session->mconn == NULL) {
        return EFAULT;
    }

    header = (FDIRProtoHeader *)out_buff;

    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ,
            sizeof(FDIRProtoFlockDEntryReq));
    req = (FDIRProtoFlockDEntryReq *)(header + 1);
    int2buff(operation, req->operation);
    long2buff(inode, req->inode);
    long2buff(offset, req->offset);
    long2buff(length, req->length);
    long2buff(owner_id, req->owner.id);
    int2buff(pid, req->owner.pid);

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(session->mconn, out_buff,
                    sizeof(out_buff), &response, session->ctx->
                    network_timeout, FDIR_SERVICE_PROTO_FLOCK_DENTRY_RESP,
                    NULL, 0)) != 0)
    {
        sf_log_network_error(&response, session->mconn, result);
    }

    return result;
}

int fdir_client_proto_getlk_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode, int *operation,
        int64_t *offset, int64_t *length, int64_t *owner_id, pid_t *pid)
{
    FDIRProtoHeader *header;
    FDIRProtoGetlkDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoGetlkDEntryReq)];
    FDIRProtoGetlkDEntryResp getlk_resp;
    SFResponseInfo response;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoGetlkDEntryReq *)(header + 1);
    long2buff(inode, req->inode);
    int2buff(*operation, req->operation);
    long2buff(*offset, req->offset);
    long2buff(*length, req->length);
    int2buff(*pid, req->pid);

    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GETLK_DENTRY_REQ,
            sizeof(FDIRProtoGetlkDEntryReq));

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_GETLK_DENTRY_RESP,
                    (char *)&getlk_resp, sizeof(getlk_resp))) == 0)
    {
        *operation = buff2int(getlk_resp.type);
        *offset = buff2long(getlk_resp.offset);
        *length = buff2long(getlk_resp.length);
        *owner_id = buff2long(getlk_resp.owner.id);
        *pid = buff2int(getlk_resp.owner.pid);
    } else {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_dentry_sys_lock(FDIRClientSession *session,
        const int64_t inode, const int flags, int64_t *file_size,
        int64_t *space_end)
{
    FDIRProtoHeader *header;
    FDIRProtoSysLockDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoSysLockDEntryReq)];
    FDIRProtoSysLockDEntryResp resp;
    SFResponseInfo response;
    int result;

    if (session->mconn == NULL) {
        return EFAULT;
    }

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoSysLockDEntryReq *)(header + 1);
    long2buff(inode, req->inode);
    int2buff(flags, req->flags);

    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_REQ,
            sizeof(FDIRProtoSysLockDEntryReq));

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(session->mconn, out_buff,
                    sizeof(out_buff), &response, session->ctx->
                    network_timeout, FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_RESP,
                    (char *)&resp, sizeof(resp))) == 0)
    {
        *file_size = buff2long(resp.size);
        *space_end = buff2long(resp.space_end);
    } else {
        sf_log_network_error(&response, session->mconn, result);
    }

    return result;
}

int fdir_client_dentry_sys_unlock_ex(FDIRClientSession *session,
        const string_t *ns, const int64_t old_size,
        const FDIRSetDEntrySizeInfo *dsize)
{
    FDIRProtoHeader *header;
    FDIRProtoSysUnlockDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoSysUnlockDEntryReq) + NAME_MAX];
    SFResponseInfo response;
    int new_flags;
    int pkg_len;
    int result;

    if (session->mconn == NULL) {
        return EFAULT;
    }
    if (ns != NULL) {
        if (ns->len <= 0 || ns->len > NAME_MAX) {
            logError("file: "__FILE__", line: %d, "
                    "invalid namespace length: %d, which <= 0 or > %d",
                    __LINE__, ns->len, NAME_MAX);
            return EINVAL;
        }
        new_flags = dsize->flags;
    } else {
        new_flags = 0;
    }
    if (dsize->inc_alloc != 0) {
        new_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC;
    }

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoSysUnlockDEntryReq *)(header + 1);
    long2buff(dsize->inode, req->inode);
    long2buff(old_size, req->old_size);
    long2buff(dsize->file_size, req->new_size);
    long2buff(dsize->inc_alloc, req->inc_alloc);
    int2buff(new_flags, req->flags);
    req->force = dsize->force;
    if (ns != NULL) {
        req->ns_len = ns->len;
        memcpy(req + 1, ns->str, ns->len);
    } else {
        req->ns_len = 0;
    }
    pkg_len = sizeof(FDIRProtoHeader) + sizeof(
            FDIRProtoSysUnlockDEntryReq) + req->ns_len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_REQ,
            pkg_len - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(session->mconn, out_buff,
                    pkg_len, &response, session->ctx->network_timeout,
                    FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_RESP,
                    NULL, 0)) != 0)
    {
        sf_log_network_error(&response, session->mconn, result);
    }

    return result;
}

static int check_realloc_client_buffer(SFResponseInfo *response,
        FDIRClientBuffer *buffer)
{
    char *new_buff;
    int alloc_size;

    if (response->header.body_len <= buffer->size) {
        return 0;
    }

    alloc_size = 2 * buffer->size;
    while (alloc_size < response->header.body_len) {
        alloc_size *= 2;
    }
    new_buff = (char *)fc_malloc(alloc_size);
    if (new_buff == NULL) {
        response->error.length = sprintf(response->error.message,
                "malloc %d bytes fail", alloc_size);
        return ENOMEM;
    }

    if (buffer->buff != buffer->fixed) {
        free(buffer->buff);
    }

    buffer->buff = new_buff;
    buffer->size = alloc_size;
    return 0;
}

static int check_realloc_dentry_array(SFResponseInfo *response,
        FDIRClientDentryArray *array, const int inc_count)
{
    FDIRClientDentry *new_entries;
    int target_count;
    int new_alloc;
    int bytes;

    target_count = array->count + inc_count;
    if (target_count <= array->alloc) {
        return 0;
    }

    if (array->alloc == 0) {
        new_alloc = 256;
    } else {
        new_alloc = 2 * array->alloc;
    }
    while (new_alloc < target_count) {
        new_alloc *= 2;
    }

    bytes = sizeof(FDIRClientDentry) * new_alloc;
    new_entries = (FDIRClientDentry *)fc_malloc(bytes);
    if (new_entries == NULL) {
        response->error.length = sprintf(response->error.message,
                "malloc %d bytes fail", bytes);
        return ENOMEM;
    }

    if (array->count > 0) {
        memcpy(new_entries, array->entries,
                sizeof(FDIRClientDentry) * array->count);
        free(array->entries);
    }
    array->entries = new_entries;
    array->alloc = new_alloc;
    return 0;
}

static int parse_list_dentry_response_body(ConnectionInfo *conn,
        SFResponseInfo *response, FDIRClientDentryArray *array,
        string_t *next_token)
{
    FDIRProtoListDEntryRespBodyHeader *body_header;
    FDIRProtoListDEntryRespBodyPart *part;
    FDIRClientDentry *cd;
    FDIRClientDentry *start;
    FDIRClientDentry *end;
    char *p;
    int result;
    int entry_len;
    int count;

    if (response->header.body_len < sizeof(FDIRProtoListDEntryRespBodyHeader)) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "server %s:%u response body length: %d < expected: %d",
                conn->ip_addr, conn->port, response->header.body_len,
                (int)sizeof(FDIRProtoListDEntryRespBodyHeader));
        return EINVAL;
    }

    body_header = (FDIRProtoListDEntryRespBodyHeader *)array->buffer.buff;
    count = buff2int(body_header->count);
    next_token->str = body_header->token;
    if (body_header->is_last) {
        next_token->len = 0;
    } else {
        next_token->len = sizeof(body_header->token);

        if (!array->name_allocator.inited) {
            if ((result=fast_mpool_init(&array->name_allocator.mpool,
                            64 * 1024, 8)) != 0)
            {
                response->error.length = sprintf(response->error.message,
                        "fast_mpool_init fail");
                return result;
            }
            array->name_allocator.inited = true;
        }
        array->name_allocator.used = true;
    }

    if ((result=check_realloc_dentry_array(response, array, count)) != 0) {
        return result;
    }

    p = array->buffer.buff + sizeof(FDIRProtoListDEntryRespBodyHeader);
    start = array->entries + array->count;
    end = start + count;
    for (cd=start; cd<end; cd++) {
        part = (FDIRProtoListDEntryRespBodyPart *)p;
        entry_len = sizeof(FDIRProtoListDEntryRespBodyPart) + part->name_len;
        if ((p - array->buffer.buff) + entry_len > response->header.body_len) {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "server %s:%u response body length exceeds header's %d",
                    conn->ip_addr, conn->port, response->header.body_len);
            return EINVAL;
        }

        cd->dentry.inode = buff2long(part->inode);
        fdir_proto_unpack_dentry_stat(&part->stat, &cd->dentry.stat);
        if (body_header->is_last) {
            FC_SET_STRING_EX(cd->name, part->name_str, part->name_len);
        } else if ((result=fast_mpool_alloc_string_ex(&array->name_allocator.mpool,
                        &cd->name, part->name_str, part->name_len)) != 0)
        {
            response->error.length = sprintf(response->error.message,
                    "strdup %d bytes fail", part->name_len);
            return result;
        }

        p += entry_len;
    }

    if ((int)(p - array->buffer.buff) != response->header.body_len) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "server %s:%u response body length: %d != header's %d",
                conn->ip_addr, conn->port, (int)(p - array->buffer.buff),
                response->header.body_len);
        return EINVAL;
    }

    array->count += count;
    return 0;
}

static int deal_list_dentry_response_body(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, SFResponseInfo *response,
        FDIRClientDentryArray *array, string_t *next_token)
{
    int result;
    if ((result=check_realloc_client_buffer(response, &array->buffer)) != 0) {
        return result;
    }

    if ((result=tcprecvdata_nb(conn->sock, array->buffer.buff,
                    response->header.body_len, client_ctx->
                    network_timeout)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv from server %s:%u fail, "
                "errno: %d, error info: %s",
                conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    return parse_list_dentry_response_body(conn, response, array, next_token);
}

static int do_list_dentry_next(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, string_t *next_token,
        SFResponseInfo *response, FDIRClientDentryArray *array)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryNextBody *entry_body;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryNextBody)];
    int out_bytes;
    int result;

    memset(out_buff, 0, sizeof(out_buff));
    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoListDEntryNextBody *)
        (out_buff + sizeof(FDIRProtoHeader));
    out_bytes = sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoListDEntryNextBody);
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    memcpy(entry_body->token, next_token->str, next_token->len);
    int2buff(array->count, entry_body->offset);
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    out_bytes, response, client_ctx->
                    network_timeout, FDIR_SERVICE_PROTO_LIST_DENTRY_RESP)) == 0)
    {
        return deal_list_dentry_response_body(client_ctx,
                conn, response, array, next_token);
    }

    return result;
}

static int deal_list_dentry_response(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, SFResponseInfo *response,
        FDIRClientDentryArray *array)
{
    string_t next_token;
    int result;

    if ((result=deal_list_dentry_response_body(client_ctx, conn,
                    response, array, &next_token)) != 0)
    {
        return result;
    }

    while (next_token.len > 0) {
        if ((result=do_list_dentry_next(client_ctx, conn,
                        &next_token, response, array)) != 0)
        {
            break;
        }
    }

    return result;
}

static int list_dentry(FDIRClientContext *client_ctx, ConnectionInfo *conn,
        char *out_buff, const int out_bytes, FDIRClientDentryArray *array)
{
    SFResponseInfo response;
    int result;

    array->count = 0;
    if (array->name_allocator.used) {
        fast_mpool_reset(&array->name_allocator.mpool);  //buffer recycle
        array->name_allocator.used = false;
    }
    response.error.length = 0;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_LIST_DENTRY_RESP)) == 0)
    {
        result = deal_list_dentry_response(client_ctx, conn, &response, array);
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_list_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        FDIRClientDentryArray *array)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryByPathBody *entry_body;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryByPathBody)
        + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoListDEntryByPathBody *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(fullname,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryByPathBody)
        + fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    return list_dentry(client_ctx, conn, out_buff, out_bytes, array);
}

int fdir_client_proto_list_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int64_t inode,
        FDIRClientDentryArray *array)
{
    FDIRProtoHeader *header;
    char out_buff[sizeof(FDIRProtoHeader) + 8];
    int out_bytes;

    header = (FDIRProtoHeader *)out_buff;
    long2buff(inode, out_buff + sizeof(FDIRProtoHeader));
    out_bytes = sizeof(out_buff);
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    return  list_dentry(client_ctx, conn, out_buff, out_bytes, array);
}

int fdir_client_service_stat(FDIRClientContext *client_ctx,
        const char *ip_addr, const int port, FDIRClientServiceStat *stat)
{
    FDIRProtoHeader *header;
    ConnectionInfo *conn;
    ConnectionInfo target_conn;
    char out_buff[sizeof(FDIRProtoHeader)];
    SFResponseInfo response;
    FDIRProtoServiceStatResp stat_resp;
    int result;

    conn_pool_set_server_info(&target_conn, ip_addr, port);
    if ((conn=client_ctx->conn_manager.get_spec_connection(
                    client_ctx, &target_conn, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SERVICE_STAT_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_SERVICE_STAT_RESP,
                    (char *)&stat_resp, sizeof(FDIRProtoServiceStatResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    if (result != 0) {
        return result;
    }

    stat->is_master = stat_resp.is_master;
    stat->status = stat_resp.status;
    stat->server_id = buff2int(stat_resp.server_id);
    stat->connection.current_count = buff2int(
            stat_resp.connection.current_count);
    stat->connection.max_count = buff2int(stat_resp.connection.max_count);

    stat->dentry.current_data_version = buff2long(
            stat_resp.dentry.current_data_version);
    stat->dentry.current_inode_sn = buff2long(
            stat_resp.dentry.current_inode_sn);
    stat->dentry.counters.ns = buff2long(stat_resp.dentry.counters.ns);
    stat->dentry.counters.dir = buff2long(stat_resp.dentry.counters.dir);
    stat->dentry.counters.file = buff2long(stat_resp.dentry.counters.file);

    return 0;
}

int fdir_client_cluster_stat(FDIRClientContext *client_ctx,
        FDIRClientClusterStatEntry *stats, const int size, int *count)
{
    FDIRProtoHeader *header;
    FDIRProtoClusterStatRespBodyHeader *body_header;
    FDIRProtoClusterStatRespBodyPart *body_part;
    FDIRProtoClusterStatRespBodyPart *body_end;
    FDIRClientClusterStatEntry *stat;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader)];
    char fixed_buff[8 * 1024];
    char *in_buff;
    SFResponseInfo response;
    int result;
    int calc_size;

    if ((conn=client_ctx->conn_manager.get_master_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    in_buff = fixed_buff;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    sizeof(out_buff), &response, client_ctx->
                    network_timeout, FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(fixed_buff)) {
            in_buff = (char *)fc_malloc(response.header.body_len);
            if (in_buff == NULL) {
                response.error.length = sprintf(response.error.message,
                        "malloc %d bytes fail", response.header.body_len);
                result = ENOMEM;
            }
        }

        if (result == 0) {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, client_ctx->
                    network_timeout);
        }
    }

    body_header = (FDIRProtoClusterStatRespBodyHeader *)in_buff;
    body_part = (FDIRProtoClusterStatRespBodyPart *)(in_buff +
            sizeof(FDIRProtoClusterStatRespBodyHeader));
    if (result == 0) {
        *count = buff2int(body_header->count);

        calc_size = sizeof(FDIRProtoClusterStatRespBodyHeader) +
            (*count) * sizeof(FDIRProtoClusterStatRespBodyPart);
        if (calc_size != response.header.body_len) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d != calculate size: %d, "
                    "server count: %d", response.header.body_len,
                    calc_size, *count);
            result = EINVAL;
        } else if (size < *count) {
            response.error.length = sprintf(response.error.message,
                    "entry size %d too small < %d", size, *count);
            *count = 0;
            result = ENOSPC;
        }
    } else {
        *count = 0;
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    } else {
        body_end = body_part + (*count);
        for (stat=stats; body_part<body_end; body_part++, stat++) {
            stat->is_master = body_part->is_master;
            stat->status = body_part->status;
            stat->server_id = buff2int(body_part->server_id);
            memcpy(stat->ip_addr, body_part->ip_addr, IP_ADDRESS_SIZE);
            *(stat->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
            stat->port = buff2short(body_part->port);
        }
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    if (in_buff != fixed_buff) {
        if (in_buff != NULL) {
            free(in_buff);
        }
    }

    return result;
}

int fdir_client_get_master(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *master)
{
    int result;
    ConnectionInfo *conn;
    FDIRProtoHeader *header;
    SFResponseInfo response;
    FDIRProtoGetServerResp server_resp;
    char out_buff[sizeof(FDIRProtoHeader)];

    conn = client_ctx->conn_manager.get_connection(client_ctx, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_GET_MASTER_RESP,
                    (char *)&server_resp, sizeof(FDIRProtoGetServerResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    } else {
        master->server_id = buff2int(server_resp.server_id);
        memcpy(master->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(master->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        master->conn.port = buff2short(server_resp.port);
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    return result;
}

int fdir_client_get_readable_server(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *server)
{
    int result;
    ConnectionInfo *conn;
    FDIRProtoHeader *header;
    SFResponseInfo response;
    FDIRProtoGetServerResp server_resp;
    char out_buff[sizeof(FDIRProtoHeader)];

    conn = client_ctx->conn_manager.get_connection(client_ctx, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP,
                    (char *)&server_resp, sizeof(FDIRProtoGetServerResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    } else {
        server->server_id = buff2int(server_resp.server_id);
        memcpy(server->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(server->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        server->conn.port = buff2short(server_resp.port);
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    return result;
}

int fdir_client_get_slaves(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *slaves, const int size, int *count)
{
    FDIRProtoHeader *header;
    FDIRProtoGetSlavesRespBodyHeader *body_header;
    FDIRProtoGetSlavesRespBodyPart *body_part;
    FDIRProtoGetSlavesRespBodyPart *body_end;
    FDIRClientServerEntry *slave;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader)];
    char fixed_buff[8 * 1024];
    char *in_buff;
    SFResponseInfo response;
    int result;
    int calc_size;

    if ((conn=client_ctx->conn_manager.get_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_SLAVES_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    in_buff = fixed_buff;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    sizeof(out_buff), &response, client_ctx->
                    network_timeout, FDIR_SERVICE_PROTO_GET_SLAVES_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(fixed_buff)) {
            in_buff = (char *)fc_malloc(response.header.body_len);
            if (in_buff == NULL) {
                response.error.length = sprintf(response.error.message,
                        "malloc %d bytes fail", response.header.body_len);
                result = ENOMEM;
            }
        }

        if (result == 0) {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, client_ctx->
                    network_timeout);
        }
    }

    body_header = (FDIRProtoGetSlavesRespBodyHeader *)in_buff;
    body_part = (FDIRProtoGetSlavesRespBodyPart *)(in_buff +
            sizeof(FDIRProtoGetSlavesRespBodyHeader));
    if (result == 0) {
        *count = buff2short(body_header->count);

        calc_size = sizeof(FDIRProtoGetSlavesRespBodyHeader) +
            (*count) * sizeof(FDIRProtoGetSlavesRespBodyPart);
        if (calc_size != response.header.body_len) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d != calculate size: %d, "
                    "server count: %d", response.header.body_len,
                    calc_size, *count);
            result = EINVAL;
        } else if (size < *count) {
            response.error.length = sprintf(response.error.message,
                    "entry size %d too small < %d", size, *count);
            *count = 0;
            result = ENOSPC;
        }
    } else {
        *count = 0;
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    } else {
        body_end = body_part + (*count);
        for (slave=slaves; body_part<body_end; body_part++, slave++) {
            slave->server_id = buff2int(body_part->server_id);
            memcpy(slave->conn.ip_addr, body_part->ip_addr, IP_ADDRESS_SIZE);
            *(slave->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
            slave->conn.port = buff2short(body_part->port);
            slave->status = body_part->status;
        }
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    if (in_buff != fixed_buff) {
        if (in_buff != NULL) {
            free(in_buff);
        }
    }

    return result;
}

int fdir_client_proto_namespace_stat(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, FDIRInodeStat *stat)
{
    FDIRProtoHeader *header;
    FDIRProtoNamespaceStatReq *req;
    FDIRProtoNamespaceStatResp resp;
    SFResponseInfo response;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoNamespaceStatReq)
        + NAME_MAX];
    int out_bytes;
    int result;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    header = (FDIRProtoHeader *)out_buff;
    req = (FDIRProtoNamespaceStatReq *)(header + 1);
    req->ns_len = ns->len;
    memcpy(req->ns_str, ns->str, ns->len);

    out_bytes = sizeof(FDIRProtoHeader) +
        sizeof(FDIRProtoNamespaceStatReq) + ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_NAMESPACE_STAT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->network_timeout,
                    FDIR_SERVICE_PROTO_NAMESPACE_STAT_RESP, (char *)&resp,
                    sizeof(FDIRProtoNamespaceStatResp))) == 0)
    {
        stat->total = buff2long(resp.inode_counters.total);
        stat->used = buff2long(resp.inode_counters.used);
        stat->avail = buff2long(resp.inode_counters.avail);
    } else {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

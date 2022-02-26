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
#ifdef OS_LINUX
#include <dirent.h>
#endif
#include "fdir_proto.h"
#include "fdir_func.h"
#include "client_global.h"
#include "client_proto.h"

static inline void init_client_buffer(FDIRClientBuffer *buffer)
{
    buffer->buff = buffer->fixed;
    buffer->size = sizeof(buffer->fixed);
}

int fdir_client_dentry_array_init_ex(FDIRClientDentryArray *array,
        struct fast_mpool_man *mpool)
{
    array->alloc = array->count = 0;
    array->entries = NULL;
    init_client_buffer(&array->buffer);

    if (mpool != NULL) {
        array->name_allocator.mpool.ptr = mpool;
        array->name_allocator.inited = true;
        array->name_allocator.used = true;
        array->name_allocator.cloned = true;
    } else {
        array->name_allocator.mpool.ptr =
            &array->name_allocator.mpool.holder;
        array->name_allocator.inited = false;
        array->name_allocator.used = false;
        array->name_allocator.cloned = false;
    }
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

    if (array->name_allocator.inited && !array->name_allocator.cloned) {
        array->name_allocator.inited = false;
        fast_mpool_destroy(array->name_allocator.mpool.ptr);
    }
}

void fdir_client_compact_dentry_array_init(
        FDIRClientCompactDentryArray *array)
{
    array->alloc = array->count = 0;
    array->entries = NULL;
    init_client_buffer(&array->buffer);
}

void fdir_client_compact_dentry_array_free(
        FDIRClientCompactDentryArray *array)
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
}

int fdir_client_namespace_stat_array_init(FDIRClientNamespaceStatArray *array)
{
    int result;

    if ((result=sf_init_recv_buffer(&array->buffer, 0)) != 0) {
        return result;
    }

    array->alloc = array->count = 0;
    array->entries = NULL;
    return 0;
}

void fdir_client_namespace_stat_array_free(FDIRClientNamespaceStatArray *array)
{
    if (array->entries != NULL) {
        free(array->entries);
        array->entries = NULL;
        array->alloc = array->count = 0;
    }

    sf_free_recv_buffer(&array->buffer);
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

static inline int client_check_set_proto_inode_info(const string_t *ns,
        const int64_t inode, FDIRProtoInodeInfo *ino_proto)
{
    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    long2buff(inode, ino_proto->inode);
    ino_proto->ns_len = ns->len;
    memcpy(ino_proto->ns_str, ns->str, ns->len);
    return 0;
}

static inline int client_check_set_proto_name_info(
        const string_t *name, FDIRProtoNameInfo *nm_proto)
{
    if (name->len <= 0 || name->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid name length: %d, which <= 0 or > %d",
                __LINE__, name->len, NAME_MAX);
        return EINVAL;
    }

    nm_proto->len = name->len;
    memcpy(nm_proto->str, name->str, name->len);
    return 0;
}

int fdir_client_proto_join_server(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, SFConnectionParameters *conn_params)
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
    req->auth_enabled = (client_ctx->auth.enabled ? 1 : 0);
    memcpy(&req->config_sign, &client_ctx->cluster.md5_digest,
            SF_CLUSTER_CONFIG_SIGN_LEN);

    SF_PROTO_SET_HEADER(proto_header, FDIR_SERVICE_PROTO_CLIENT_JOIN_REQ,
            sizeof(FDIRProtoClientJoinReq));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->common_cfg.network_timeout,
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
                    &response, client_ctx->common_cfg.network_timeout,
                    expect_cmd, (char *)&proto_stat,
                    sizeof(proto_stat))) == 0)
    {
        proto_unpack_dentry(&proto_stat, dentry);
    } else {
        sf_log_network_error_for_update(&response, conn, result);
    }

    return result;
}

#define CLIENT_PROTO_SET_OMP(omp, front) \
    do { \
        int2buff(omp->uid, front.uid);   \
        int2buff(omp->gid, front.gid);   \
        int2buff(omp->mode, front.mode); \
    } while (0)

#define CLIENT_PROTO_SET_CREATE_FRONT(omp, _rdev, front) \
    CLIENT_PROTO_SET_OMP(omp, front); \
    long2buff(_rdev, front.rdev)

#define CLIENT_PROTO_SET_LINK_FRONT(omp, _flags, front) \
    CLIENT_PROTO_SET_OMP(omp, front); \
    int2buff(_flags, front.flags)

int fdir_client_proto_create_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname,
        const FDIRClientOwnerModePair *omp,
        const dev_t rdev, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoCreateDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoCreateDEntryReq) + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }

    CLIENT_PROTO_SET_CREATE_FRONT(omp, rdev, req->front);
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoSymlinkDEntryReq) + NAME_MAX + 2 * PATH_MAX];
    int out_bytes;
    int result;

    if (link->len <= 0 || link->len > PATH_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid link length: %d, which <= 0 or > %d",
                __LINE__, link->len, NAME_MAX);
        return EINVAL;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
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
        const FDIRDEntryFullName *fullname, const int flags,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveDEntry *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoRemoveDEntry) + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP, dentry);
}

int fdir_client_proto_link_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *src, const FDIRDEntryFullName *dest,
        const FDIRClientOwnerModePair *omp, const int flags,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoHDLinkDEntry *req;
    FDIRProtoDEntryInfo *dest_pentry;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoHDLinkDEntry) + 2 * (NAME_MAX + PATH_MAX)];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(src, &req->src)) != 0) {
        return result;
    }

    dest_pentry = (FDIRProtoDEntryInfo *)((char *)(&req->src + 1) +
            src->ns.len + src->path.len);
    if ((result=client_check_set_proto_dentry(dest, dest_pentry)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_LINK_FRONT(omp, flags, req->front);
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
        const int flags, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoHDLinkDEntryByPName *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoHDLinkDEntryByPName) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->dest)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_LINK_FRONT(omp, flags, req->front.common);
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
                    &response, client_ctx->common_cfg.network_timeout,
                    expect_cmd, (char *)&proto_stat, expect_body_lens,
                    2, &body_len)) == 0)
    {
        if (body_len == (int)sizeof(proto_stat)) {
            proto_unpack_dentry(&proto_stat, *dentry);
        } else {
            *dentry = NULL;
        }
    } else {
        sf_log_network_error_for_update(&response, conn, result);
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoRenameDEntry) + 2 * (NAME_MAX + PATH_MAX)];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoRenameDEntryByPName) + 2 * (NAME_MAX + PATH_MAX)];
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
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

static int setup_req_by_dentry_fullname(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const int req_cmd,
        char *out_buff, int *out_bytes)
{
    int result;
    FDIRProtoHeader *header;
    FDIRProtoDEntryInfo *proto_dentry;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, proto_dentry, 0, *out_bytes);
    if ((result=client_check_set_proto_dentry(fullname, proto_dentry)) != 0) {
        return result;
    }
    *out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, req_cmd, *out_bytes - sizeof(FDIRProtoHeader));
    return 0;
}

static int setup_req_by_dentry_pname(FDIRClientContext *client_ctx,
        const string_t *ns, const FDIRDEntryPName *pname,
        const int req_cmd, char *out_buff, int *out_bytes)
{
    int result;
    FDIRProtoHeader *header;
    FDIRProtoDEntryByPName *req;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, *out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, req)) != 0) {
        return result;
    }

    *out_bytes += ns->len + pname->name.len;
    SF_PROTO_SET_HEADER(header, req_cmd, *out_bytes - sizeof(FDIRProtoHeader));
    return 0;
}

static inline int setup_req_by_dentry_inode(FDIRClientContext *client_ctx,
        const string_t *ns, const int64_t inode, const int req_cmd,
        char *out_buff, int *out_bytes)
{
    FDIRProtoHeader *header;
    FDIRProtoInodeInfo *req;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, *out_bytes);
    *out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, req_cmd, *out_bytes - sizeof(FDIRProtoHeader));

    long2buff(inode, req->inode);
    req->ns_len = ns->len;
    memcpy(req->ns_str, ns->str, ns->len);
    return 0;
}

static int query_by_dentry_fullname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const int req_cmd, const int resp_cmd, char *in_buff,
        const int in_len, const int enoent_log_level)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoDEntryInfo) + NAME_MAX + PATH_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;
    int log_level;

    if ((result=setup_req_by_dentry_fullname(client_ctx, fullname,
                    req_cmd, out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
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
        ConnectionInfo *conn, const string_t *ns, const FDIRDEntryPName *pname,
        const int enoent_log_level, int64_t *inode)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoDEntryByPName) + 2 * NAME_MAX];
    int out_bytes;
    int result;
    SFResponseInfo response;
    FDIRProtoLookupInodeResp proto_resp;
    int log_level;

    if ((result=setup_req_by_dentry_pname(client_ctx, ns, pname,
                    FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        *inode = -1;
        return result;
    }

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
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
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoStatDEntryReq) + NAME_MAX + PATH_MAX];
    FDIRProtoHeader *header;
    FDIRProtoStatDEntryReq *req;
    FDIRProtoStatDEntryResp proto_stat;
    SFResponseInfo response;
    int out_bytes;
    int result;
    int log_level;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname, &req->dentry)) != 0) {
        return result;
    }
    int2buff(flags, req->front.flags);
    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP,
                    (char *)&proto_stat, sizeof(proto_stat))) == 0)
    {
        proto_unpack_dentry(&proto_stat, dentry);
    } else {
        log_level = (result == ENOENT) ? enoent_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
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
                    out_bytes, &response, client_ctx->common_cfg.
                    network_timeout, expect_cmd)) == 0)
    {
        if (response.header.body_len >= size) {
            logError("file: "__FILE__", line: %d, "
                    "body length: %d exceeds max size: %d",
                    __LINE__, response.header.body_len, size);
            return EOVERFLOW;
        }

        if ((result=tcprecvdata_nb_ex(conn->sock, link->str,
                        response.header.body_len, client_ctx->common_cfg.
                        network_timeout, &link->len)) == 0)
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoDEntryInfo) + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    if ((result=setup_req_by_dentry_fullname(client_ctx, fullname,
                    FDIR_SERVICE_PROTO_READLINK_BY_PATH_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    return do_readlink(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_READLINK_BY_PATH_RESP, link, size);
}

int fdir_client_proto_readlink_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const FDIRDEntryPName *pname,
        string_t *link, const int size)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoDEntryByPName) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    if ((result=setup_req_by_dentry_pname(client_ctx, ns, pname,
                    FDIR_SERVICE_PROTO_READLINK_BY_PNAME_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }

    return do_readlink(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_READLINK_BY_PNAME_RESP, link, size);
}

int fdir_client_proto_readlink_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        string_t *link, const int size)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoInodeInfo) + NAME_MAX];
    int out_bytes;
    int result;

    if ((result=setup_req_by_dentry_inode(client_ctx, ns, inode,
                    FDIR_SERVICE_PROTO_READLINK_BY_INODE_REQ,
                    out_buff, &out_bytes)) != 0)
    {
        return result;
    }
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
                    &response, client_ctx->common_cfg.network_timeout,
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
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        const int flags, FDIRDEntryInfo *dentry)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoStatDEntryByInodeReq) + NAME_MAX];
    FDIRProtoHeader *header;
    FDIRProtoStatDEntryByInodeReq *req;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);
    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    return do_stat_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_STAT_BY_INODE_RESP, dentry, LOG_ERR);
}

int fdir_client_proto_stat_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const FDIRDEntryPName *pname,
        const int flags, const int enoent_log_level, FDIRDEntryInfo *dentry)
{
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoStatDEntryByPNameReq) + 2 * NAME_MAX];
    FDIRProtoHeader *header;
    FDIRProtoStatDEntryByPNameReq *req;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->pname)) != 0) {
        return result;
    }
    int2buff(flags, req->front.flags);
    out_bytes += ns->len + pname->name.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_STAT_BY_PNAME_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_stat_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_STAT_BY_PNAME_RESP, dentry, enoent_log_level);
}

int fdir_client_proto_create_dentry_by_pname(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRDEntryPName *pname,
        const FDIRClientOwnerModePair *omp,
        const dev_t rdev, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoCreateDEntryByPNameReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoCreateDEntryByPNameReq) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->pname)) != 0) {
        return result;
    }

    CLIENT_PROTO_SET_CREATE_FRONT(omp, rdev, req->front);
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
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

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
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
        const int flags, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveDEntryByPName *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoRemoveDEntryByPName) + 2 * NAME_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_pname(ns, pname, &req->pname)) != 0) {
        return result;
    }

    int2buff(flags, req->front.flags);
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
    if (dsize->force) { \
        int2buff(dsize->flags | FDIR_DENTRY_FIELD_MODIFIED_FLAG_FORCE, \
                req->flags); \
    } else { \
        int2buff(dsize->flags, req->flags); \
    }

int fdir_client_proto_set_dentry_size(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const FDIRSetDEntrySizeInfo *dsize,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoSetDentrySizeReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoSetDentrySizeReq) + NAME_MAX];
    int out_bytes;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
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

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, rheader, req_id, out_bytes);
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
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_RESP)) != 0)
    {
        sf_log_network_error_for_update(&response, conn, result);
    }

    return result;
}

#define CLIENT_PROTO_SET_MODIFY_STAT_FRONT(front) \
    long2buff(mflags, front.mflags); \
    int2buff(flags, front.flags);    \
    fdir_proto_pack_dentry_stat(stat, &front.stat)

int fdir_client_proto_modify_stat_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const string_t *ns, const int64_t inode, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoModifyStatByInodeReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoModifyStatByInodeReq) + NAME_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }

    CLIENT_PROTO_SET_MODIFY_STAT_FRONT(req->front);
    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_MODIFY_STAT_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_MODIFY_STAT_BY_INODE_RESP, dentry);
}

int fdir_client_proto_modify_stat_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FDIRDEntryFullName *fullname, const int64_t mflags,
        const FDIRDEntryStat *stat, const int flags, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoModifyStatByPathReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoModifyStatByPathReq) + NAME_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }

    CLIENT_PROTO_SET_MODIFY_STAT_FRONT(req->front);
    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_MODIFY_STAT_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    return do_update_dentry(client_ctx, conn, out_buff, out_bytes,
            FDIR_SERVICE_PROTO_MODIFY_STAT_BY_PATH_RESP, dentry);
}

int fdir_client_init_session(FDIRClientContext *client_ctx,
    FDIRClientSession *session)
{
    int result;
    if ((session->mconn=client_ctx->cm.ops.get_master_connection(
                    &client_ctx->cm, 0, &result)) == NULL)
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
        session->ctx->cm.ops.close_connection(
                &session->ctx->cm, session->mconn);
    } else if (session->ctx->cm.ops.release_connection != NULL) {
        session->ctx->cm.ops.release_connection(
                &session->ctx->cm, session->mconn);
    }
    session->mconn = NULL;
}

int fdir_client_flock_dentry_ex2(FDIRClientSession *session, const string_t *ns,
        const int64_t inode, const int operation, const int64_t offset,
        const int64_t length, const int64_t owner_id, const pid_t pid)
{
    FDIRProtoHeader *header;
    FDIRProtoFlockDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoFlockDEntryReq) + NAME_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    if (session->mconn == NULL) {
        return EFAULT;
    }

    SF_PROTO_CLIENT_SET_REQ(session->ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }
    int2buff(operation, req->operation);
    long2buff(offset, req->offset);
    long2buff(length, req->length);
    long2buff(owner_id, req->owner.id);
    int2buff(pid, req->owner.pid);

    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(session->mconn, out_buff,
                    out_bytes, &response, session->ctx->common_cfg.
                    network_timeout, FDIR_SERVICE_PROTO_FLOCK_DENTRY_RESP,
                    NULL, 0)) != 0)
    {
        sf_log_network_error(&response, session->mconn, result);
    }

    return result;
}

int fdir_client_proto_getlk_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        int *operation, int64_t *offset, int64_t *length,
        int64_t *owner_id, pid_t *pid)
{
    FDIRProtoHeader *header;
    FDIRProtoGetlkDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoGetlkDEntryReq) + NAME_MAX];
    FDIRProtoGetlkDEntryResp getlk_resp;
    SFResponseInfo response;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }
    int2buff(*operation, req->operation);
    long2buff(*offset, req->offset);
    long2buff(*length, req->length);
    int2buff(*pid, req->pid);

    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GETLK_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
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
        const string_t *ns, const int64_t inode, const int flags,
        int64_t *file_size, int64_t *space_end)
{
    FDIRProtoHeader *header;
    FDIRProtoSysLockDEntryReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoSysLockDEntryReq) + NAME_MAX];
    FDIRProtoSysLockDEntryResp resp;
    SFResponseInfo response;
    int out_bytes;
    int result;

    if (session->mconn == NULL) {
        return EFAULT;
    }

    SF_PROTO_CLIENT_SET_REQ(session->ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }
    int2buff(flags, req->flags);

    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(session->mconn, out_buff,
                    out_bytes, &response, session->ctx->common_cfg.
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
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoSysUnlockDEntryReq) + NAME_MAX];
    SFResponseInfo response;
    int new_flags;
    int out_bytes;
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

    SF_PROTO_CLIENT_SET_REQ(session->ctx, out_buff, header, req, 0, out_bytes);
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
    out_bytes += req->ns_len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(session->mconn, out_buff, out_bytes,
                    &response, session->ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_RESP, NULL, 0)) != 0)
    {
        sf_log_network_error(&response, session->mconn, result);
    }

    return result;
}

static inline void pack_set_xattr_fields(const key_value_pair_t *xattr,
        const int flags, FDIRProtoSetXAttrFields *fields)
{
    fields->name_len = xattr->key.len;
    short2buff(xattr->value.len, fields->value_len);
    int2buff(flags, fields->flags);
    memcpy(fields->name_str, xattr->key.str, xattr->key.len);
    memcpy(fields->name_str + xattr->key.len,
            xattr->value.str, xattr->value.len);
}

int fdir_client_proto_set_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const
        FDIRDEntryFullName *fullname, const key_value_pair_t *xattr,
        const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoSetXAttrByPathReq *req;
    FDIRProtoDEntryInfo *proto_dentry;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoSetXAttrByPathReq) + FDIR_XATTR_MAX_VALUE_SIZE +
        2 * NAME_MAX + PATH_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    if ((result=fdir_validate_xattr(xattr)) != 0) {
        return result;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, req_id, out_bytes);
    pack_set_xattr_fields(xattr, flags, &req->fields);
    proto_dentry = (FDIRProtoDEntryInfo *)(req->fields.name_str +
            xattr->key.len + xattr->value.len);
    if ((result=client_check_set_proto_dentry(fullname,
                    proto_dentry)) != 0)
    {
        return result;
    }

    out_bytes += xattr->key.len + xattr->value.len +
        fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_RESP)) != 0)
    {
        sf_log_network_error_for_update(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_set_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const int64_t inode, const key_value_pair_t *xattr,
        const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoSetXAttrByInodeReq *req;
    FDIRProtoInodeInfo *ino_proto;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoSetXAttrByInodeReq) +
        FDIR_XATTR_MAX_VALUE_SIZE + 2 * NAME_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    if ((result=fdir_validate_xattr(xattr)) != 0) {
        return result;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, req_id, out_bytes);
    pack_set_xattr_fields(xattr, flags, &req->fields);
    ino_proto = (FDIRProtoInodeInfo *)(req->fields.name_str +
            xattr->key.len + xattr->value.len);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, ino_proto)) != 0)
    {
        return result;
    }

    out_bytes += xattr->key.len + xattr->value.len + ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_RESP)) != 0)
    {
        sf_log_network_error_for_update(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_remove_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const
        FDIRDEntryFullName *fullname, const string_t *name,
        const int flags, const int enoattr_log_level)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveXAttrByPathReq *req;
    FDIRProtoDEntryInfo *proto_dentry;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoRemoveXAttrByPathReq) + 2 * NAME_MAX + PATH_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_name_info(name, &req->name)) != 0) {
        return result;
    }

    proto_dentry = (FDIRProtoDEntryInfo *)(req->name.str + name->len);
    if ((result=client_check_set_proto_dentry(fullname,
                    proto_dentry)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += name->len + fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_RESP)) != 0)
    {
        sf_log_network_error_for_delete(&response,
                conn, result, enoattr_log_level);
    }

    return result;
}

int fdir_client_proto_remove_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const string_t *ns,
        const int64_t inode, const string_t *name,
        const int flags, const int enoattr_log_level)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveXAttrByInodeReq *req;
    FDIRProtoInodeInfo *ino_proto;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoRemoveXAttrByInodeReq) + 2 * NAME_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    if ((result=client_check_set_proto_name_info(name, &req->name)) != 0) {
        return result;
    }

    ino_proto = (FDIRProtoInodeInfo *)(req->name.str + name->len);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, ino_proto)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += name->len + ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_RESP)) != 0)
    {
        sf_log_network_error_for_delete(&response,
                conn, result, enoattr_log_level);
    }

    return result;
}

#define RECV_SIZE_OR_VAR_RESPONSE(client_ctx, conn, out_buff, \
        out_bytes, response, resp_cmd, value, size) \
    response.error.length = 0; \
    if ((flags & FDIR_FLAGS_XATTR_GET_SIZE)) { \
        char in_buff[4]; \
        if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,  \
                        &response, client_ctx->common_cfg.network_timeout,\
                        resp_cmd, in_buff, sizeof(in_buff))) == 0) \
        { \
            value->len = buff2int(in_buff); \
        } \
    } else { \
        result = sf_send_and_recv_response_ex1(conn, out_buff, out_bytes, \
                &response, client_ctx->common_cfg.network_timeout, \
                resp_cmd, value->str, size, &value->len); \
    }


int fdir_client_proto_get_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        const string_t *name, const int enoattr_log_level,
        string_t *value, const int size, const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoGetXAttrByPathReq *req;
    FDIRProtoDEntryInfo *proto_dentry;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoGetXAttrByPathReq) + 2 * NAME_MAX + PATH_MAX];
    SFResponseInfo response;
    int out_bytes;
    int log_level;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_name_info(name, &req->name)) != 0) {
        return result;
    }

    proto_dentry = (FDIRProtoDEntryInfo *)(req->name.str + name->len);
    if ((result=client_check_set_proto_dentry(fullname,
                    proto_dentry)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += name->len + fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    RECV_SIZE_OR_VAR_RESPONSE(client_ctx, conn, out_buff, out_bytes, response,
            FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_RESP, value, size);
    if (result != 0) {
        log_level = (result == ENOENT || result == ENODATA) ?
            enoattr_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

int fdir_client_proto_get_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        const string_t *name, const int enoattr_log_level,
        string_t *value, const int size, const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoGetXAttrByInodeReq *req;
    FDIRProtoInodeInfo *ino;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoGetXAttrByInodeReq) + 2 * NAME_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;
    int log_level;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    if ((result=client_check_set_proto_name_info(name, &req->name)) != 0) {
        return result;
    }

    ino = (FDIRProtoInodeInfo *)(req->name.str + name->len);
    if ((result=client_check_set_proto_inode_info(ns, inode, ino)) != 0) {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += name->len + ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    RECV_SIZE_OR_VAR_RESPONSE(client_ctx, conn, out_buff, out_bytes, response,
            FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_RESP, value, size);
    if (result != 0) {
        log_level = (result == ENOENT || result == ENODATA) ?
            enoattr_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

int fdir_client_proto_list_xattr_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        string_t *list, const int size, const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoListXAttrByPathReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoListXAttrByPathReq) + NAME_MAX + PATH_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    RECV_SIZE_OR_VAR_RESPONSE(client_ctx, conn, out_buff, out_bytes, response,
            FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_RESP, list, size);
    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_list_xattr_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        string_t *list, const int size, const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoListXAttrByInodeReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoListXAttrByInodeReq) + NAME_MAX];
    SFResponseInfo response;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    RECV_SIZE_OR_VAR_RESPONSE(client_ctx, conn, out_buff, out_bytes, response,
            FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_RESP, list, size);
    if (result != 0) {
        sf_log_network_error(&response, conn, result);
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
        FDIRClientCommonDentryArray *array, const int element_size,
        const int target_count)
{
    void *new_entries;
    int new_alloc;
    int bytes;

    if (target_count <= array->alloc) {
        return 0;
    }

    if (array->alloc == 0) {
        new_alloc = 4;
    } else {
        new_alloc = 2 * array->alloc;
    }
    while (new_alloc < target_count) {
        new_alloc *= 2;
    }

    bytes = element_size * new_alloc;
    new_entries = fc_malloc(bytes);
    if (new_entries == NULL) {
        response->error.length = sprintf(
                response->error.message,
                "malloc %d bytes fail", bytes);
        return ENOMEM;
    }

    if (array->count > 0) {
        memcpy(new_entries, array->entries,
                element_size * array->count);
        free(array->entries);
    }
    array->entries = new_entries;
    array->alloc = new_alloc;
    return 0;
}

static int parse_list_dentry_bheader(SFResponseInfo *response,
        FDIRClientCommonDentryArray *array, const int element_size,
        string_t *next_token, const bool is_first, bool *is_last,
        int *count, char **p)
{
    FDIRProtoListDEntryRespBodyFirstHeader *first_header;
    FDIRProtoListDEntryRespBodyCommonHeader *common_header;
    int common_header_len;
    int total_count;
    int result;

    if (is_first) {
        common_header_len = sizeof(FDIRProtoListDEntryRespBodyFirstHeader);
        first_header = (FDIRProtoListDEntryRespBodyFirstHeader *)
            array->buffer.buff;
        common_header = &first_header->common;

        total_count = buff2int(first_header->total_count);
        if ((result=check_realloc_dentry_array(response, array,
                        element_size, total_count)) != 0)
        {
            return result;
        }
    } else {
        common_header_len = sizeof(FDIRProtoListDEntryRespBodyNextHeader);
        common_header = (FDIRProtoListDEntryRespBodyCommonHeader *)
            array->buffer.buff;
    }

    if (response->header.body_len < common_header_len) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "response body length: %d < expected: %d",
                response->header.body_len, common_header_len);
        return EINVAL;
    }

    *count = buff2int(common_header->count);
    if (array->count + *count > array->alloc) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "sum count: %d > alloc: %d (current count: %d)",
                array->count + *count, array->alloc, *count);
        return EOVERFLOW;
    }

    *p = array->buffer.buff + common_header_len;
    *is_last = (common_header->is_last == 1);
    next_token->str = common_header->token;
    if (common_header->is_last) {
        next_token->len = 0;
    } else {
        next_token->len = sizeof(common_header->token);
    }

    return 0;
}

typedef int (*list_dentry_parse_resp_body_func)(SFResponseInfo *response,
        void *array, string_t *next_token, const bool is_first);

static int parse_list_dentry_response_body(
        SFResponseInfo *response, FDIRClientDentryArray *array,
        string_t *next_token, const bool is_first)
{
    FDIRProtoListDEntryRespCompletePart *part;
    FDIRClientDentry *cd;
    FDIRClientDentry *start;
    FDIRClientDentry *end;
    char *p;
    int result;
    int entry_len;
    int count;
    bool is_last;

    if ((result=parse_list_dentry_bheader(response,
                    (FDIRClientCommonDentryArray *)array,
                    sizeof(FDIRClientDentry), next_token,
                    is_first, &is_last, &count, &p)) != 0)
    {
        return result;
    }

    if (!is_last) {
        if (!array->name_allocator.inited) {
            if ((result=fast_mpool_init(array->name_allocator.
                            mpool.ptr, 64 * 1024, 8)) != 0)
            {
                response->error.length = sprintf(
                        response->error.message,
                        "fast_mpool_init fail");
                return result;
            }
            array->name_allocator.inited = true;
        }
        array->name_allocator.used = true;
    }

    start = array->entries + array->count;
    end = start + count;
    for (cd=start; cd<end; cd++) {
        part = (FDIRProtoListDEntryRespCompletePart *)p;
        entry_len = sizeof(FDIRProtoListDEntryRespCompletePart) +
            part->common.name_len;
        if ((p - array->buffer.buff) + entry_len > response->header.body_len) {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "response body length exceeds header's %d",
                    response->header.body_len);
            return EINVAL;
        }

        cd->dentry.inode = buff2long(part->common.inode);
        fdir_proto_unpack_dentry_stat(&part->stat, &cd->dentry.stat);
        if (is_last && !array->name_allocator.cloned) {
            FC_SET_STRING_EX(cd->name, part->common.name_str,
                    part->common.name_len);
        } else if ((result=fast_mpool_alloc_string_ex(array->
                        name_allocator.mpool.ptr, &cd->name,
                        part->common.name_str, part->common.name_len)) != 0)
        {
            response->error.length = sprintf(response->error.message,
                    "strdup %d bytes fail", part->common.name_len);
            return result;
        }

        p += entry_len;
    }

    if ((int)(p - array->buffer.buff) != response->header.body_len) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "response body length: %d != header's %d",
                (int)(p - array->buffer.buff), response->header.body_len);
        return EINVAL;
    }

    array->count += count;
    return 0;
}

static int parse_list_compact_dentry_rbody(SFResponseInfo *response,
        FDIRClientCompactDentryArray *array,
        string_t *next_token, const bool is_first)
{
    FDIRProtoListDEntryRespCompactPart *part;
    FDIRDirent *cd;
    FDIRDirent *start;
    FDIRDirent *end;
    char *p;
    int result;
    int entry_len;
    int count;
    int mode;
    bool is_last;

    if ((result=parse_list_dentry_bheader(response,
                    (FDIRClientCommonDentryArray *)array,
                    sizeof(FDIRDirent), next_token,
                    is_first, &is_last, &count, &p)) != 0)
    {
        return result;
    }

    start = array->entries + array->count;
    end = start + count;
    for (cd=start; cd<end; cd++) {
        part = (FDIRProtoListDEntryRespCompactPart *)p;
        entry_len = sizeof(FDIRProtoListDEntryRespCompactPart) +
            part->common.name_len;
        if ((p - array->buffer.buff) + entry_len > response->header.body_len) {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "response body length exceeds header's %d",
                    response->header.body_len);
            return EINVAL;
        }

        mode = buff2int(part->mode);
        cd->d_ino = buff2long(part->common.inode);

#ifdef HAVE_DIRENT_D_NAMLEN
        cd->d_namlen =
#endif
        snprintf(cd->d_name, sizeof(cd->d_name), "%.*s",
                part->common.name_len, part->common.name_str);

#ifdef HAVE_DIRENT_D_TYPE
        if (S_ISBLK(mode)) {
            cd->d_type = DT_BLK;
        } else if (S_ISCHR(mode)) {
            cd->d_type = DT_CHR;
        } else if (S_ISDIR(mode)) {
            cd->d_type = DT_DIR;
        } else if (S_ISFIFO(mode)) {
            cd->d_type = DT_FIFO;
        } else if (S_ISREG(mode)) {
            cd->d_type = DT_REG;
        } else if (S_ISLNK(mode)) {
            cd->d_type = DT_LNK;
        } else if (S_ISSOCK(mode)) {
            cd->d_type = DT_SOCK;
        } else {
            cd->d_type = DT_UNKNOWN;
        }
#endif

#ifdef HAVE_DIRENT_D_RECLEN
        cd->d_reclen = sizeof(FDIRDirent);
#endif

#ifdef HAVE_DIRENT_D_OFF
        cd->d_off = (int)(cd - array->entries);
#endif

        p += entry_len;
    }

    if ((int)(p - array->buffer.buff) != response->header.body_len) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "response body length: %d != header's %d",
                (int)(p - array->buffer.buff), response->header.body_len);
        return EINVAL;
    }

    array->count += count;
    return 0;
}

static int deal_list_dentry_response_body(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, SFResponseInfo *response,
        list_dentry_parse_resp_body_func deal_func,
        FDIRClientCommonDentryArray *array,
        string_t *next_token, const bool is_first)
{
    int result;

    if ((result=check_realloc_client_buffer(response, &array->buffer)) != 0) {
        return result;
    }

    if ((result=tcprecvdata_nb(conn->sock, array->buffer.buff,
                    response->header.body_len, client_ctx->
                    common_cfg.network_timeout)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv from server %s:%u fail, "
                "errno: %d, error info: %s",
                conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    return deal_func(response, array, next_token, is_first);
}

static int do_list_dentry_next(FDIRClientContext *client_ctx, ConnectionInfo
        *conn, string_t *next_token, SFResponseInfo *response,
        list_dentry_parse_resp_body_func deal_func,
        FDIRClientCommonDentryArray *array)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryNextBody *entry_body;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoListDEntryNextBody)];
    int out_bytes;
    int result;

    memset(out_buff, 0, sizeof(out_buff));
    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, entry_body, 0, out_bytes);
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    memcpy(entry_body->token, next_token->str, next_token->len);
    int2buff(array->count, entry_body->offset);
    if ((result=sf_send_and_check_response_header(conn, out_buff, out_bytes,
                    response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_LIST_DENTRY_RESP)) == 0)
    {
        return deal_list_dentry_response_body(client_ctx,
                conn, response, deal_func, array, next_token, false);
    }

    return result;
}

static int deal_list_dentry_response(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, SFResponseInfo *response,
        list_dentry_parse_resp_body_func deal_func,
        FDIRClientCommonDentryArray *array)
{
    string_t next_token;
    int result;

    if ((result=deal_list_dentry_response_body(client_ctx, conn, response,
                    deal_func, array, &next_token, true)) != 0)
    {
        return result;
    }

    while (next_token.len > 0) {
        if ((result=do_list_dentry_next(client_ctx, conn, &next_token,
                        response, deal_func, array)) != 0)
        {
            break;
        }
    }

    return result;
}

static int list_dentry(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, char *out_buff, const int out_bytes,
        list_dentry_parse_resp_body_func deal_func,
        FDIRClientCommonDentryArray *array)
{
    SFResponseInfo response;
    int result;

    response.error.length = 0;
    if ((result=sf_send_and_check_response_header(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_LIST_DENTRY_RESP)) == 0)
    {
        result = deal_list_dentry_response(client_ctx,
                conn, &response, deal_func, array);
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

static int list_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        list_dentry_parse_resp_body_func deal_func,
        FDIRClientCommonDentryArray *array, const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryByPathReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoListDEntryByPathReq) + NAME_MAX + PATH_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, out_bytes);
    if ((result=client_check_set_proto_dentry(fullname,
                    &req->dentry)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += fullname->ns.len + fullname->path.len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    return list_dentry(client_ctx, conn, out_buff,
            out_bytes, deal_func, array);
}

static inline void reset_dentry_array(FDIRClientDentryArray *array)
{
    array->count = 0;
    if (array->name_allocator.used && !array->name_allocator.cloned) {
        fast_mpool_reset(array->name_allocator.mpool.ptr);  //buffer recycle
        array->name_allocator.used = false;
    }
}

static int list_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        list_dentry_parse_resp_body_func deal_func,
        FDIRClientCommonDentryArray *array, const int flags)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryByInodeReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoListDEntryByInodeReq) + NAME_MAX];
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, out_bytes);
    if ((result=client_check_set_proto_inode_info(
                    ns, inode, &req->ino)) != 0)
    {
        return result;
    }
    int2buff(flags, req->front.flags);

    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_BY_INODE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    return list_dentry(client_ctx, conn, out_buff,
            out_bytes, deal_func, array);
}

int fdir_client_proto_list_dentry_by_path(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const FDIRDEntryFullName *fullname,
        FDIRClientDentryArray *array)
{
    const int flags = 0;

    reset_dentry_array(array);
    return list_dentry_by_path(client_ctx, conn, fullname,
            (list_dentry_parse_resp_body_func)parse_list_dentry_response_body,
            (FDIRClientCommonDentryArray *)array, flags);
}

int fdir_client_proto_list_dentry_by_inode(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns, const int64_t inode,
        FDIRClientDentryArray *array)
{
    const int flags = 0;

    reset_dentry_array(array);
    return list_dentry_by_inode(client_ctx, conn, ns, inode,
            (list_dentry_parse_resp_body_func)parse_list_dentry_response_body,
            (FDIRClientCommonDentryArray *)array, flags);
}

int fdir_client_proto_list_compact_dentry_by_path(FDIRClientContext
        *client_ctx, ConnectionInfo *conn, const FDIRDEntryFullName
        *fullname, FDIRClientCompactDentryArray *array)
{
    const int flags = FDIR_LIST_DENTRY_FLAGS_COMPACT_OUTPUT;

    array->count = 0;
    return list_dentry_by_path(client_ctx, conn, fullname,
            (list_dentry_parse_resp_body_func)parse_list_compact_dentry_rbody,
            (FDIRClientCommonDentryArray *)array, flags);
}

int fdir_client_proto_list_compact_dentry_by_inode(FDIRClientContext
        *client_ctx, ConnectionInfo *conn, const string_t *ns,
        const int64_t inode, FDIRClientCompactDentryArray *array)
{
    const int flags = FDIR_LIST_DENTRY_FLAGS_COMPACT_OUTPUT;

    array->count = 0;
    return list_dentry_by_inode(client_ctx, conn, ns, inode,
            (list_dentry_parse_resp_body_func)parse_list_compact_dentry_rbody,
            (FDIRClientCommonDentryArray *)array, flags);
}

int fdir_client_service_stat(FDIRClientContext *client_ctx,
        const ConnectionInfo *spec_conn, FDIRClientServiceStat *stat)
{
    FDIRProtoHeader *header;
    SFProtoEmptyBodyReq *req;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE];
    SFResponseInfo response;
    FDIRProtoServiceStatResp stat_resp;
    int out_bytes;
    int result;

    if ((conn=client_ctx->cm.ops.get_spec_connection(
                    &client_ctx->cm, spec_conn, &result)) == NULL)
    {
        return result;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SERVICE_STAT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_SERVICE_STAT_RESP,
                    (char *)&stat_resp, sizeof(FDIRProtoServiceStatResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
    if (result != 0) {
        return result;
    }

    stat->is_master = stat_resp.is_master;
    stat->status = stat_resp.status;
    stat->server_id = buff2int(stat_resp.server_id);
    stat->connection.current_count = buff2int(
            stat_resp.connection.current_count);
    stat->connection.max_count = buff2int(stat_resp.connection.max_count);

    stat->binlog.current_version = buff2long(
            stat_resp.binlog.current_version);
    stat->binlog.writer.total_count = buff2long(
            stat_resp.binlog.writer.total_count);
    stat->binlog.writer.next_version = buff2long(
            stat_resp.binlog.writer.next_version);
    stat->binlog.writer.waiting_count = buff2int(
            stat_resp.binlog.writer.waiting_count);
    stat->binlog.writer.max_waitings = buff2int(
            stat_resp.binlog.writer.max_waitings);

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
    SFProtoEmptyBodyReq *req;
    FDIRProtoClusterStatRespBodyHeader *body_header;
    FDIRProtoClusterStatRespBodyPart *body_part;
    FDIRProtoClusterStatRespBodyPart *body_end;
    FDIRClientClusterStatEntry *stat;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE];
    char fixed_buff[8 * 1024];
    char *in_buff;
    SFResponseInfo response;
    int out_bytes;
    int result;
    int calc_size;

    if ((conn=client_ctx->cm.ops.get_master_connection(
                    &client_ctx->cm, 0, &result)) == NULL)
    {
        return result;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    in_buff = fixed_buff;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, client_ctx->common_cfg.
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
                    common_cfg.network_timeout);
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

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
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

    conn = client_ctx->cm.ops.get_connection(&client_ctx->cm, 0, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->common_cfg.network_timeout,
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

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
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

    conn = client_ctx->cm.ops.get_connection(&client_ctx->cm, 0, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->common_cfg.network_timeout,
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

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
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

    if ((conn=client_ctx->cm.ops.get_connection(
                    &client_ctx->cm, 0, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_SLAVES_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    in_buff = fixed_buff;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    sizeof(out_buff), &response, client_ctx->common_cfg.
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
                    common_cfg.network_timeout);
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

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
    if (in_buff != fixed_buff) {
        if (in_buff != NULL) {
            free(in_buff);
        }
    }

    return result;
}

int fdir_client_proto_namespace_stat(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const string_t *ns,
        FDIRClientNamespaceStat *stat)
{
    FDIRProtoHeader *header;
    FDIRProtoNamespaceStatReq *req;
    FDIRProtoNamespaceStatResp resp;
    SFResponseInfo response;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FDIRProtoNamespaceStatReq) + NAME_MAX];
    int out_bytes;
    int result;

    if (ns->len <= 0 || ns->len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, ns->len, NAME_MAX);
        return EINVAL;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    req->ns_len = ns->len;
    memcpy(req->ns_str, ns->str, ns->len);
    out_bytes += ns->len;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_NAMESPACE_STAT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_NAMESPACE_STAT_RESP, (char *)&resp,
                    sizeof(FDIRProtoNamespaceStatResp))) == 0)
    {
        stat->inode.total = buff2long(resp.inode_counters.total);
        stat->inode.used = buff2long(resp.inode_counters.used);
        stat->inode.avail = buff2long(resp.inode_counters.avail);
        stat->space.used = buff2long(resp.used_bytes);
    } else {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fdir_client_proto_nss_subscribe(FDIRClientContext *client_ctx,
        ConnectionInfo *conn)
{
    FDIRProtoHeader *header;
    SFProtoEmptyBodyReq *req;
    char out_buff[sizeof(FDIRProtoHeader) + SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(SFProtoEmptyBodyReq)];
    SFResponseInfo response;
    int out_bytes;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, header, req, 0, out_bytes);
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn,
                    out_buff, out_bytes, &response,
                    client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_RESP)) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

static int check_realloc_namespace_stat_array(SFResponseInfo *response,
        FDIRClientNamespaceStatArray *array, const int target_count)
{
    FDIRClientNamespaceStatEntry *new_entries;
    int new_alloc;
    int bytes;

    if (target_count <= array->alloc) {
        return 0;
    }

    if (array->alloc == 0) {
        new_alloc = 1024;
    } else {
        new_alloc = 2 * array->alloc;
    }
    while (new_alloc < target_count) {
        new_alloc *= 2;
    }

    bytes = sizeof(FDIRClientNamespaceStatEntry) * new_alloc;
    new_entries = (FDIRClientNamespaceStatEntry *)fc_malloc(bytes);
    if (new_entries == NULL) {
        response->error.length = sprintf(response->error.message,
                "malloc %d bytes fail", bytes);
        return ENOMEM;
    }

    if (array->entries != NULL) {
        free(array->entries);
    }
    array->entries = new_entries;
    array->alloc = new_alloc;
    return 0;
}

static int parse_nss_fetch_response_body(ConnectionInfo *conn,
        SFResponseInfo *response, FDIRClientNamespaceStatArray *array,
        bool *is_last)
{
    FDIRProtoNSSFetchRespBodyHeader *body_header;
    FDIRProtoNSSFetchRespBodyPart *part;
    FDIRClientNamespaceStatEntry *current;
    FDIRClientNamespaceStatEntry *end;
    char *p;
    int result;
    int entry_len;
    int count;

    body_header = (FDIRProtoNSSFetchRespBodyHeader *)array->buffer.buff;
    count = buff2int(body_header->count);
    *is_last = body_header->is_last;
    if ((result=check_realloc_namespace_stat_array(
                    response, array, count)) != 0)
    {
        return result;
    }

    p = (char *)(body_header + 1);
    end = array->entries + count;
    for (current=array->entries; current<end; current++) {
        part = (FDIRProtoNSSFetchRespBodyPart *)p;
        entry_len = sizeof(FDIRProtoNSSFetchRespBodyPart) + part->ns_name.len;
        if ((p - array->buffer.buff) + entry_len > response->header.body_len) {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "server %s:%u response body length exceeds header's %d",
                    conn->ip_addr, conn->port, response->header.body_len);
            return EINVAL;
        }

        current->used_bytes = buff2long(part->used_bytes);
        FC_SET_STRING_EX(current->ns_name, part->ns_name.str,
                part->ns_name.len);

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

    array->count = count;
    return 0;
}

int fdir_client_proto_nss_fetch(FDIRClientContext *client_ctx,
        ConnectionInfo *conn, FDIRClientNamespaceStatArray *array,
        bool *is_last)
{
    FDIRProtoHeader *header;
    char out_buff[sizeof(FDIRProtoHeader)];
    SFResponseInfo response;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_NSS_FETCH_REQ, 0);
    response.error.length = 0;
    if ((result=sf_send_and_recv_vary_response(conn,
                    out_buff, sizeof(out_buff), &response,
                    client_ctx->common_cfg.network_timeout,
                    FDIR_SERVICE_PROTO_NSS_FETCH_RESP, &array->buffer,
                    sizeof(FDIRProtoNSSFetchRespBodyHeader))) != 0)
    {
        sf_log_network_error(&response, conn, result);
        return result;
    }

    return parse_nss_fetch_response_body(conn, &response, array, is_last);
}

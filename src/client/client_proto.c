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

static inline void log_network_error_ex(FDIRResponseInfo *response,
        const ConnectionInfo *conn, const int result, const int line)
{
    if (response->error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%d, %s", line,
                conn->ip_addr, conn->port,
                response->error.message);
    } else {
        logError("file: "__FILE__", line: %d, "
                "communicate with dir server %s:%d fail, "
                "errno: %d, error info: %s", line,
                conn->ip_addr, conn->port,
                result, STRERROR(result));
    }
}

#define log_network_error(response, conn, result) \
        log_network_error_ex(response, conn, result, __LINE__)
 
static inline void fdir_client_release_connection(
        FDIRClientContext *client_ctx,
        ConnectionInfo *conn, const int result)
{
    if (result != 0 && is_network_error(result)) {
        client_ctx->conn_manager.close_connection(client_ctx, conn);
    } else if (client_ctx->conn_manager.release_connection != NULL) {
        client_ctx->conn_manager.release_connection(client_ctx, conn);
    }
}

static inline void proto_to_dentry(FDIRProtoStatDEntryResp *proto_stat,
        FDIRDEntryInfo *dentry)
{
    dentry->inode = buff2long(proto_stat->inode);
    dentry->stat.mode = buff2int(proto_stat->mode);
    dentry->stat.ctime = buff2int(proto_stat->ctime);
    dentry->stat.mtime = buff2int(proto_stat->mtime);
    dentry->stat.size = buff2long(proto_stat->size);
}

int fdir_client_create_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, const mode_t mode,
        FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoCreateDEntryBody *entry_body;
    int out_bytes;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoCreateDEntryBody)
        + NAME_MAX + PATH_MAX];
    FDIRResponseInfo response;
    FDIRProtoStatDEntryResp proto_stat;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoCreateDEntryBody *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(fullname,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    if ((conn=client_ctx->conn_manager.get_master_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    int2buff(mode, entry_body->front.mode);
    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoCreateDEntryBody)
        + fullname->ns.len + fullname->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    response.error.message[0] = '\0';
    if ((result=fdir_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, g_fdir_client_vars.network_timeout,
                    FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP,
                    (char *)&proto_stat, sizeof(proto_stat))) == 0)
    {
        proto_to_dentry(&proto_stat, dentry);
    } else {
        log_network_error(&response, conn, result);
    }

    fdir_client_release_connection(client_ctx, conn, result);
    return result;
}

int fdir_client_remove_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoRemoveDEntry *entry_body;
    int out_bytes;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoRemoveDEntry)
        + NAME_MAX + PATH_MAX];
    FDIRResponseInfo response;
    FDIRProtoStatDEntryResp proto_stat;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoRemoveDEntry *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(fullname,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    if ((conn=client_ctx->conn_manager.get_master_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoRemoveDEntry)
        + fullname->ns.len + fullname->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    response.error.message[0] = '\0';
    if ((result=fdir_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, g_fdir_client_vars.network_timeout,
                    FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP,
                    (char *)&proto_stat, sizeof(proto_stat))) == 0)
    {
        proto_to_dentry(&proto_stat, dentry);
    } else {
        log_network_error(&response, conn, result);
    }

    fdir_client_release_connection(client_ctx, conn, result);
    return result;
}

int fdir_client_stat_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRDEntryInfo *dentry)
{
    FDIRProtoHeader *header;
    FDIRProtoDEntryInfo *proto_dentry;
    int out_bytes;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoDEntryInfo)
        + NAME_MAX + PATH_MAX];
    FDIRResponseInfo response;
    FDIRProtoStatDEntryResp proto_stat;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    proto_dentry = (FDIRProtoDEntryInfo *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(fullname, proto_dentry)) != 0) {
        return result;
    }

    if ((conn=client_ctx->conn_manager.get_readable_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoDEntryInfo)
        + fullname->ns.len + fullname->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    response.error.length = 0;
    response.error.message[0] = '\0';
    if ((result=fdir_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, g_fdir_client_vars.network_timeout,
                    FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP,
                    (char *)&proto_stat, sizeof(proto_stat))) == 0)
    {
        proto_to_dentry(&proto_stat, dentry);
    } else {
        log_network_error(&response, conn, result);
    }

    fdir_client_release_connection(client_ctx, conn, result);
    return result;
}

static int check_realloc_client_buffer(FDIRResponseInfo *response,
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
    new_buff = (char *)malloc(alloc_size);
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

static int check_realloc_dentry_array(FDIRResponseInfo *response,
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
    new_entries = (FDIRClientDentry *)malloc(bytes);
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
        FDIRResponseInfo *response, FDIRClientDentryArray *array,
        string_t *next_token)
{
    FDIRProtoListDEntryRespBodyHeader *body_header;
    FDIRProtoListDEntryRespBodyPart *part;
    FDIRClientDentry *dentry;
    FDIRClientDentry *start;
    FDIRClientDentry *end;
    char *p;
    int result;
    int entry_len;
    int count;

    if (response->header.body_len < sizeof(FDIRProtoListDEntryRespBodyHeader)) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "server %s:%d response body length: %d < expected: %d",
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
    for (dentry=start; dentry<end; dentry++) {
        part = (FDIRProtoListDEntryRespBodyPart *)p;
        entry_len = sizeof(FDIRProtoListDEntryRespBodyPart) + part->name_len;
        if ((p - array->buffer.buff) + entry_len > response->header.body_len) {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "server %s:%d response body length exceeds header's %d",
                    conn->ip_addr, conn->port, response->header.body_len);
            return EINVAL;
        }


        if (body_header->is_last) {
            FC_SET_STRING_EX(dentry->name, part->name_str, part->name_len);
        } else if ((result=fast_mpool_alloc_string_ex(&array->name_allocator.mpool,
                        &dentry->name, part->name_str, part->name_len)) != 0)
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
                "server %s:%d response body length: %d != header's %d",
                conn->ip_addr, conn->port, (int)(p - array->buffer.buff),
                response->header.body_len);
        return EINVAL;
    }

    array->count += count;
    return 0;
}

static int deal_list_dentry_response_body(ConnectionInfo *conn,
        FDIRResponseInfo *response, FDIRClientDentryArray *array,
        string_t *next_token)
{
    int result;
    if ((result=check_realloc_client_buffer(response, &array->buffer)) != 0) {
        return result;
    }

    if ((result=tcprecvdata_nb(conn->sock, array->buffer.buff,
                    response->header.body_len, g_fdir_client_vars.
                    network_timeout)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv from server %s:%d fail, "
                "errno: %d, error info: %s",
                conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    return parse_list_dentry_response_body(conn, response, array, next_token);
}

static int do_list_dentry_next(ConnectionInfo *conn, string_t *next_token,
        FDIRResponseInfo *response, FDIRClientDentryArray *array)
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
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ,
            out_bytes - sizeof(FDIRProtoHeader));
    memcpy(entry_body->token, next_token->str, next_token->len);
    int2buff(array->count, entry_body->offset);
    if ((result=fdir_send_and_check_response_header(conn, out_buff,
                    out_bytes, response, g_fdir_client_vars.
                    network_timeout, FDIR_SERVICE_PROTO_LIST_DENTRY_RESP)) == 0)
    {
        return deal_list_dentry_response_body(conn, response,
                    array, next_token);
    }

    return result;
}

static int deal_list_dentry_response(ConnectionInfo *conn,
        FDIRResponseInfo *response, FDIRClientDentryArray *array)
{
    string_t next_token;
    int result;

    if ((result=deal_list_dentry_response_body(conn, response,
                    array, &next_token)) != 0)
    {
        return result;
    }

    while (next_token.len > 0) {
        if ((result=do_list_dentry_next(conn, &next_token,
                        response, array)) != 0) {
            break;
        }
    }

    return result;
}

int fdir_client_list_dentry(FDIRClientContext *client_ctx,
        const FDIRDEntryFullName *fullname, FDIRClientDentryArray *array)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryFirstBody *entry_body;
    int out_bytes;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryFirstBody)
        + NAME_MAX + PATH_MAX];
    FDIRResponseInfo response;
    int result;

    array->count = 0;
    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoListDEntryFirstBody *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(fullname,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    if ((conn=client_ctx->conn_manager.get_readable_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryFirstBody)
        + fullname->ns.len + fullname->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_LIST_DENTRY_FIRST_REQ,
            out_bytes - sizeof(FDIRProtoHeader));

    if (array->name_allocator.used) {
        fast_mpool_reset(&array->name_allocator.mpool);  //buffer recycle
        array->name_allocator.used = false;
    }
    response.error.length = 0;
    response.error.message[0] = '\0';
    if ((result=fdir_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, g_fdir_client_vars.
                    network_timeout, FDIR_SERVICE_PROTO_LIST_DENTRY_RESP)) == 0)
    {
        result = deal_list_dentry_response(conn, &response, array);
    }

    if (result != 0) {
        log_network_error(&response, conn, result);
    }

    fdir_client_release_connection(client_ctx, conn, result);
    return result;
}

int fdir_client_service_stat(FDIRClientContext *client_ctx,
        const char *ip_addr, const int port, FDIRClientServiceStat *stat)
{
    FDIRProtoHeader *header;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader)];
    FDIRResponseInfo response;
    FDIRProtoServiceStatResp stat_resp;
    int result;

    if ((conn=client_ctx->conn_manager.get_spec_connection(
                    client_ctx, ip_addr, port, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_SERVICE_STAT_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    if ((result=fdir_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, g_fdir_client_vars.network_timeout,
                    FDIR_SERVICE_PROTO_SERVICE_STAT_RESP,
                    (char *)&stat_resp, sizeof(FDIRProtoServiceStatResp))) != 0)
    {
        log_network_error(&response, conn, result);
    }

    fdir_client_release_connection(client_ctx, conn, result);
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
    FDIRResponseInfo response;
    int result;
    int calc_size;

    if ((conn=client_ctx->conn_manager.get_master_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    in_buff = fixed_buff;
    if ((result=fdir_send_and_check_response_header(conn, out_buff,
                    sizeof(out_buff), &response, g_fdir_client_vars.
                    network_timeout, FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(fixed_buff)) {
            in_buff = (char *)malloc(response.header.body_len);
            if (in_buff == NULL) {
                response.error.length = sprintf(response.error.message,
                        "malloc %d bytes fail", response.header.body_len);
                result = ENOMEM;
            }
        }

        if (result == 0) {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, g_fdir_client_vars.
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
        log_network_error(&response, conn, result);
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

    fdir_client_release_connection(client_ctx, conn, result);
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
    FDIRResponseInfo response;
    FDIRProtoGetServerResp server_resp;
    char out_buff[sizeof(FDIRProtoHeader)];

    conn = client_ctx->conn_manager.get_connection(client_ctx, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    if ((result=fdir_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, g_fdir_client_vars.network_timeout,
                    FDIR_SERVICE_PROTO_GET_MASTER_RESP,
                    (char *)&server_resp, sizeof(FDIRProtoGetServerResp))) != 0)
    {
        log_network_error(&response, conn, result);
    } else {
        master->server_id = buff2int(server_resp.server_id);
        memcpy(master->ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(master->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        master->port = buff2short(server_resp.port);
    }

    fdir_client_release_connection(client_ctx, conn, result);
    return result;
}

int fdir_client_get_readable_server(FDIRClientContext *client_ctx,
        FDIRClientServerEntry *server)
{
    int result;
    ConnectionInfo *conn;
    FDIRProtoHeader *header;
    FDIRResponseInfo response;
    FDIRProtoGetServerResp server_resp;
    char out_buff[sizeof(FDIRProtoHeader)];

    conn = client_ctx->conn_manager.get_connection(client_ctx, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    if ((result=fdir_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, g_fdir_client_vars.network_timeout,
                    FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP,
                    (char *)&server_resp, sizeof(FDIRProtoGetServerResp))) != 0)
    {
        log_network_error(&response, conn, result);
    } else {
        server->server_id = buff2int(server_resp.server_id);
        memcpy(server->ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(server->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        server->port = buff2short(server_resp.port);
    }

    fdir_client_release_connection(client_ctx, conn, result);
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
    FDIRResponseInfo response;
    int result;
    int calc_size;

    if ((conn=client_ctx->conn_manager.get_connection(
                    client_ctx, &result)) == NULL)
    {
        return result;
    }

    header = (FDIRProtoHeader *)out_buff;
    FDIR_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_SLAVES_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));

    in_buff = fixed_buff;
    if ((result=fdir_send_and_check_response_header(conn, out_buff,
                    sizeof(out_buff), &response, g_fdir_client_vars.
                    network_timeout, FDIR_SERVICE_PROTO_GET_SLAVES_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(fixed_buff)) {
            in_buff = (char *)malloc(response.header.body_len);
            if (in_buff == NULL) {
                response.error.length = sprintf(response.error.message,
                        "malloc %d bytes fail", response.header.body_len);
                result = ENOMEM;
            }
        }

        if (result == 0) {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, g_fdir_client_vars.
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
        log_network_error(&response, conn, result);
    } else {
        body_end = body_part + (*count);
        for (slave=slaves; body_part<body_end; body_part++, slave++) {
            slave->server_id = buff2int(body_part->server_id);
            memcpy(slave->ip_addr, body_part->ip_addr, IP_ADDRESS_SIZE);
            *(slave->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
            slave->port = buff2short(body_part->port);
            slave->status = body_part->status;
        }
    }

    fdir_client_release_connection(client_ctx, conn, result);
    if (in_buff != fixed_buff) {
        if (in_buff != NULL) {
            free(in_buff);
        }
    }

    return result;
}

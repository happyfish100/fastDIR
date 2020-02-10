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

int fdir_client_dentry_array_init(FDIRClientDentryArray *array)
{
    array->alloc = array->count = 0;
    array->entries = NULL;
    return fast_mpool_init(&array->mpool, 128 * 1024, 8);
}

void fdir_client_dentry_array_free(FDIRClientDentryArray *array)
{
    if (array->entries != NULL) {
        free(array->entries);
        array->entries = NULL;
        array->alloc = array->count = 0;
    }

    fast_mpool_destroy(&array->mpool);
}

static int client_check_set_proto_dentry(const FDIRDEntryInfo *entry_info,
        FDIRProtoDEntryInfo *entry_proto)
{
    if (entry_info->ns.len <= 0 || entry_info->ns.len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid namespace length: %d, which <= 0 or > %d",
                __LINE__, entry_info->ns.len, NAME_MAX);
        return EINVAL;
    }

    if (entry_info->path.len <= 0 || entry_info->path.len > PATH_MAX) {
        logError("file: "__FILE__", line: %d, "
                "invalid path length: %d, which <= 0 or > %d",
                __LINE__, entry_info->path.len, PATH_MAX);
        return EINVAL;
    }

    entry_proto->ns_len = entry_info->ns.len;
    short2buff(entry_info->path.len, entry_proto->path_len);
    memcpy(entry_proto->ns_str, entry_info->ns.str, entry_info->ns.len);
    memcpy(entry_proto->ns_str + entry_info->ns.len,
            entry_info->path.str, entry_info->path.len);
    return 0;
}

static inline int make_connection(ConnectionInfo *conn)
{
    if (conn->sock >= 0) {
        return 0;
    }

    return conn_pool_connect_server(conn, g_client_global_vars.
            network_timeout);
}

static ConnectionInfo *get_master_connection(FDIRServerCluster *server_cluster,
        int *err_no)
{
    if (server_cluster->master == NULL) {
        //TODO: fix me!!!
        server_cluster->master = server_cluster->server_group.servers;
    }

    if ((*err_no=make_connection(server_cluster->master)) != 0) {
        return NULL;
    }

    return server_cluster->master;
}

static ConnectionInfo *get_slave_connection(FDIRServerCluster *server_cluster,
        int *err_no)
{
    ConnectionInfo *conn;

    conn = NULL;
    if (server_cluster->slave_group.count > 0) {
        ConnectionInfo **pp;
        ConnectionInfo **current;
        ConnectionInfo **pp_end;

        current = server_cluster->slave_group.servers + server_cluster->
            slave_group.index;
        if ((*err_no=make_connection(*current)) == 0) {
            conn = *current;
        } else {
            pp_end = server_cluster->slave_group.servers +
                server_cluster->slave_group.count;
            for (pp=server_cluster->slave_group.servers; pp<pp_end; pp++) {
                if (pp != current) {
                    if ((*err_no=make_connection(*pp)) == 0) {
                        conn = *pp;
                        server_cluster->slave_group.index = pp -
                            server_cluster->slave_group.servers;
                        break;
                    }
                }
            }
        }

        server_cluster->slave_group.index++;
        if (server_cluster->slave_group.index >=
                server_cluster->slave_group.count)
        {
            server_cluster->slave_group.index = 0;
        }
    } else {
        //TODO: fix me
        ConnectionInfo *p;
        ConnectionInfo *end;

        end = server_cluster->server_group.servers +
            server_cluster->server_group.count;
        for (p=server_cluster->server_group.servers; p<end; p++) {
            if ((*err_no=make_connection(p)) == 0) {
                conn = p;
                break;
            }
        }
    }

    return conn;
}

int fdir_client_create_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryInfo *entry_info, const int flags,
        const mode_t mode)
{
    FDIRProtoHeader *header;
    FDIRProtoCreateDEntryBody *entry_body;
    int out_bytes;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoCreateDEntryBody)
        + NAME_MAX + PATH_MAX];
    FDIRResponseInfo response;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoCreateDEntryBody *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(entry_info,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    if ((conn=get_master_connection(server_cluster, &result)) == NULL) {
        return result;
    }

    int2buff(flags, entry_body->front.flags);
    int2buff(mode, entry_body->front.mode);
    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoCreateDEntryBody)
        + entry_info->ns.len + entry_info->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_PROTO_CREATE_DENTRY,
            out_bytes - sizeof(FDIRProtoHeader));


    logInfo("ns: %.*s, path: %.*s, req body_len: %d", entry_info->ns.len, entry_info->ns.str,
            entry_info->path.len, entry_info->path.str, out_bytes - (int)sizeof(FDIRProtoHeader));

    response.error.length = 0;
    response.error.message[0] = '\0';
    if ((result=fdir_send_and_recv_none_body_response(conn, out_buff,
                    out_bytes, &response, g_client_global_vars.
                    network_timeout, FDIR_PROTO_ACK)) != 0)
    {
        if (response.error.length > 0) {
            logError("file: "__FILE__", line: %d, "
                    "%s", __LINE__, response.error.message);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "communicate with dir server %s:%d fail, "
                    "errno: %d, error info: %s", __LINE__,
                    conn->ip_addr, conn->port,
                    result, STRERROR(result));
        }
    }

    if (is_network_error(result)) {
        conn_pool_disconnect_server(conn);
    }

    return result;
}

int fdir_client_list_dentry(FDIRServerCluster *server_cluster,
        const FDIRDEntryInfo *entry_info, FDIRClientDentryArray *array)
{
    FDIRProtoHeader *header;
    FDIRProtoListDEntryFirstBody *entry_body;
    int out_bytes;
    ConnectionInfo *conn;
    char out_buff[sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryFirstBody)
        + NAME_MAX + PATH_MAX];
    FDIRResponseInfo response;
    int result;

    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoListDEntryFirstBody *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(entry_info,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    if ((conn=get_slave_connection(server_cluster, &result)) == NULL) {
        return result;
    }

    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoListDEntryFirstBody)
        + entry_info->ns.len + entry_info->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_PROTO_LIST_DENTRY_FIRST_REQ,
            out_bytes - sizeof(FDIRProtoHeader));


    logInfo("ns: %.*s, path: %.*s, req body_len: %d", entry_info->ns.len, entry_info->ns.str,
            entry_info->path.len, entry_info->path.str, out_bytes - (int)sizeof(FDIRProtoHeader));

    response.error.length = 0;
    response.error.message[0] = '\0';
    if ((result=fdir_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, g_client_global_vars.
                    network_timeout, FDIR_PROTO_LIST_DENTRY_RESP)) != 0)
    {
        if (response.error.length > 0) {
            logError("file: "__FILE__", line: %d, "
                    "%s", __LINE__, response.error.message);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "communicate with dir server %s:%d fail, "
                    "errno: %d, error info: %s", __LINE__,
                    conn->ip_addr, conn->port,
                    result, STRERROR(result));
        }
    }

    //TODO: recv body

    if (is_network_error(result)) {
        conn_pool_disconnect_server(conn);
    }

    return result;
}

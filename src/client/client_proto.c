
#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "common/fdir_proto.h"
#include "client_global.h"
#include "client_proto.h"

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
    int2buff(entry_info->path.len, entry_proto->path_len);
    memcpy(entry_proto->ns_str, entry_info->ns.str, entry_info->ns.len);
    memcpy(entry_proto->ns_str + entry_info->ns.len,
            entry_info->path.str, entry_info->path.len);
    return 0;
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

    conn = NULL;
    //TODO make connection

    header = (FDIRProtoHeader *)out_buff;
    entry_body = (FDIRProtoCreateDEntryBody *)(out_buff +
            sizeof(FDIRProtoHeader));
    if ((result=client_check_set_proto_dentry(entry_info,
                    &entry_body->dentry)) != 0)
    {
        return result;
    }

    int2buff(flags, entry_body->front.flags);
    int2buff(mode, entry_body->front.mode);
    out_bytes = sizeof(FDIRProtoHeader) + sizeof(FDIRProtoCreateDEntryBody)
        + entry_info->ns.len + entry_info->path.len;
    FDIR_PROTO_SET_HEADER(header, FDIR_PROTO_CREATE_DENTRY,
            out_bytes - sizeof(FDIRProtoHeader));

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

    return result;
}

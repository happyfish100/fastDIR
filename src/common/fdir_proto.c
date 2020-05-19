
#include <errno.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/sockopt.h"
#include "fdir_types.h"
#include "fdir_proto.h"

void fdir_proto_init()
{
}

int fdir_proto_set_body_length(struct fast_task_info *task)
{
    FDIRProtoHeader *header;

    header = (FDIRProtoHeader *)task->data;
    if (!FDIR_PROTO_CHECK_MAGIC(header->magic)) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, magic "FDIR_PROTO_MAGIC_FORMAT
                " is invalid, expect: "FDIR_PROTO_MAGIC_FORMAT,
                __LINE__, task->client_ip,
                FDIR_PROTO_MAGIC_PARAMS(header->magic),
                FDIR_PROTO_MAGIC_EXPECT_PARAMS);
        return EINVAL;
    }

    task->length = buff2int(header->body_len); //set body length
    return 0;
}

int fdir_check_response(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd)
{
    int result;

    if (response->header.status == 0) {
        if (response->header.cmd != expect_cmd) {
            response->error.length = sprintf(
                    response->error.message,
                    "response cmd: %d != expect: %d",
                    response->header.cmd, expect_cmd);
            return EINVAL;
        }

        return 0;
    }

    if (response->header.body_len > 0) {
        int recv_bytes;
        if (response->header.body_len >= sizeof(response->error.message)) {
            response->error.length = sizeof(response->error.message) - 1;
        } else {
            response->error.length = response->header.body_len;
        }

        if ((result=tcprecvdata_nb_ex(conn->sock, response->error.message,
                response->error.length, network_timeout, &recv_bytes)) == 0)
        {
            response->error.message[response->error.length] = '\0';
        } else {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "recv error message fail, "
                    "recv bytes: %d, expect message length: %d, "
                    "errno: %d, error info: %s", recv_bytes,
                    response->error.length, result, STRERROR(result));
        }
    } else {
        response->error.length = 0;
        response->error.message[0] = '\0';
    }

    return response->header.status;
}

int fdir_send_and_recv_response_header(ConnectionInfo *conn, char *data,
        const int len, FDIRResponseInfo *response, const int network_timeout)
{
    int result;
    FDIRProtoHeader header_proto;

    if ((result=tcpsenddata_nb(conn->sock, data, len, network_timeout)) != 0) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "send data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        return result;
    }

    if ((result=tcprecvdata_nb(conn->sock, &header_proto,
            sizeof(FDIRProtoHeader), network_timeout)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        return result;
    }

    fdir_proto_extract_header(&header_proto, &response->header);
    return 0;
}

int fdir_send_and_recv_response(ConnectionInfo *conn, char *send_data,
        const int send_len, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd,
        char *recv_data, const int expect_body_len)
{
    int result;
    int recv_bytes;

    if ((result=fdir_send_and_check_response_header(conn,
                    send_data, send_len, response,
                    network_timeout, expect_cmd)) != 0)
    {
        return result;
    }

    if (response->header.body_len != expect_body_len) {
        response->error.length = sprintf(response->error.message,
                "response body length: %d != %d",
                response->header.body_len,
                expect_body_len);
        return EINVAL;
    }
    if (expect_body_len == 0) {
        return 0;
    }

    if ((result=tcprecvdata_nb_ex(conn->sock, recv_data,
                    expect_body_len, network_timeout, &recv_bytes)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv body fail, recv bytes: %d, expect body length: %d, "
                "errno: %d, error info: %s", recv_bytes,
                response->header.body_len,
                result, STRERROR(result));
    }
    return result;
}

int fdir_active_test(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout)
{
    FDIRProtoHeader fdir_header_proto;

    FDIR_PROTO_SET_HEADER(&fdir_header_proto, FDIR_PROTO_ACTIVE_TEST_REQ, 0);
    return fdir_send_and_recv_none_body_response(conn,
            (char *)&fdir_header_proto, sizeof(FDIRProtoHeader), response,
            network_timeout, FDIR_PROTO_ACTIVE_TEST_RESP);
}

const char *fdir_get_server_status_caption(const int status)
{

    switch (status) {
        case FDIR_SERVER_STATUS_INIT:
            return "INIT";
        case FDIR_SERVER_STATUS_BUILDING:
            return "BUILDING";
        case FDIR_SERVER_STATUS_DUMPING:
            return "DUMPING";
        case FDIR_SERVER_STATUS_OFFLINE:
            return "OFFLINE";
        case FDIR_SERVER_STATUS_SYNCING:
            return "SYNCING";
        case FDIR_SERVER_STATUS_ACTIVE:
            return "ACTIVE";
        default:
            return "UNKOWN";
    }
}

const char *fdir_get_cmd_caption(const int cmd)
{
    switch (cmd) {
        case FDIR_PROTO_ACK:
            return "ACK";
        case FDIR_PROTO_ACTIVE_TEST_REQ:
            return "ACTIVE_TEST_REQ";
        case FDIR_PROTO_ACTIVE_TEST_RESP:
            return "ACTIVE_TEST_RESP";
        case FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ:
            return "CREATE_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP:
            return "CREATE_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_CREATE_BY_PNAME_REQ:
            return "CREATE_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_CREATE_BY_PNAME_RESP:
            return "CREATE_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ:
            return "REMOVE_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP:
            return "REMOVE_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_REQ:
            return "REMOVE_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_RESP:
            return "REMOVE_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_REQ:
            return "LOOKUP_INODE_REQ";
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_RESP:
            return "LOOKUP_INODE_RESP";
        case FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ:
            return "STAT_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP:
            return "STAT_BY_PATH_RESP";
        case FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ:
            return "STAT_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_STAT_BY_INODE_RESP:
            return "STAT_BY_INODE_RESP";
        case FDIR_SERVICE_PROTO_STAT_BY_PNAME_REQ:
            return "STAT_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_STAT_BY_PNAME_RESP:
            return "STAT_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_REQ:
            return "SET_DENTRY_SIZE_REQ";
        case FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_RESP:
            return "SET_DENTRY_SIZE_RESP";
        case FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_REQ:
            return "MODIFY_DENTRY_STAT_REQ";
        case FDIR_SERVICE_PROTO_MODIFY_DENTRY_STAT_RESP:
            return "MODIFY_DENTRY_STAT_RESP";
        case FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ:
            return "FLOCK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_FLOCK_DENTRY_RESP:
            return "FLOCK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_GETLK_DENTRY_REQ:
            return "GETLK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_GETLK_DENTRY_RESP:
            return "GETLK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_REQ:
            return "SYS_LOCK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_SYS_LOCK_DENTRY_RESP:
            return "SYS_LOCK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_REQ:
            return "SYS_UNLOCK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_SYS_UNLOCK_DENTRY_RESP:
            return "SYS_UNLOCK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_LIST_DENTRY_BY_PATH_REQ:
            return "LIST_DENTRY_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_LIST_DENTRY_BY_INODE_REQ:
            return "LIST_DENTRY_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ:
            return "LIST_DENTRY_NEXT_REQ";
        case FDIR_SERVICE_PROTO_LIST_DENTRY_RESP:
            return "LIST_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_SERVICE_STAT_REQ:
            return "SERVICE_STAT_REQ";
        case FDIR_SERVICE_PROTO_SERVICE_STAT_RESP:
            return "SERVICE_STAT_RESP";
        case FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ:
            return "CLUSTER_STAT_REQ";
        case FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP:
            return "CLUSTER_STAT_RESP";
        case FDIR_SERVICE_PROTO_GET_MASTER_REQ:
            return "GET_MASTER_REQ";
        case FDIR_SERVICE_PROTO_GET_MASTER_RESP:
            return "GET_MASTER_RESP";
        case FDIR_SERVICE_PROTO_GET_SLAVES_REQ:
            return "GET_SLAVE_REQ";
        case FDIR_SERVICE_PROTO_GET_SLAVES_RESP:
            return "GET_SLAVE_RESP";
        case FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ:
            return "GET_READABLE_SERVER_REQ";
        case FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP:
            return "GET_READABLE_SERVER_RESP";
        case FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
            return "GET_SERVER_STATUS_REQ";
        case FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP:
            return "GET_SERVER_STATUS_RESP";
        case FDIR_CLUSTER_PROTO_JOIN_MASTER:
            return "JOIN_MASTER";
        case FDIR_CLUSTER_PROTO_PING_MASTER_REQ:
            return "PING_MASTER_REQ";
        case FDIR_CLUSTER_PROTO_PING_MASTER_RESP:
            return "PING_MASTER_RESP";
        case FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER:
            return "PRE_SET_NEXT_MASTER";
        case FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER:
            return "COMMIT_NEXT_MASTER";
        case FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ:
            return "JOIN_SLAVE_REQ";
        case FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP:
            return "JOIN_SLAVE_RESP";
        case FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ:
            return "PUSH_BINLOG_REQ";
        case FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP:
            return "PUSH_BINLOG_RESP";
        default:
            return "UNKOWN";
    }
}

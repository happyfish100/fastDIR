
#include <errno.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/sockopt.h"
#include "fdir_proto.h"
#include "fdir_types.h"

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

void fdir_set_admin_header (FDIRProtoHeader *fdir_header_proto,
        unsigned char cmd, int body_len)
{
    fdir_header_proto->cmd = cmd;
    int2buff(body_len, fdir_header_proto->body_len);
}

int fdir_check_response(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd)
{
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
        if (response->header.body_len >= sizeof(response->error.message)) {
            response->error.length = sizeof(response->error.message) - 1;
        } else {
            response->error.length = response->header.body_len;
        }
        tcprecvdata_nb_ex(conn->sock, response->error.message,
                response->error.length, network_timeout,
                &response->error.length);
        response->error.message[response->error.length] = '\0';
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
                "send data to server %s:%d fail, "
                "errno: %d, error info: %s",
                conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    if ((result=tcprecvdata_nb(conn->sock, &header_proto,
            sizeof(FDIRProtoHeader), network_timeout)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv from server %s:%d fail, "
                "errno: %d, error info: %s",
                conn->ip_addr, conn->port,
                result, STRERROR(result));
        return result;
    }

    fdir_proto_extract_header(&header_proto, &response->header);
    return 0;
}

int fdir_send_and_recv_none_body_response(ConnectionInfo *conn, char *data,
        const int len, FDIRResponseInfo *response, int network_timeout,
        const unsigned char expect_cmd)
{
    int result;

    if ((result=fdir_send_and_check_response_header(conn,
                    data, len, response, network_timeout, expect_cmd)) != 0)
    {
        return result;
    }

    if (response->header.body_len > 0) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "server %s:%d, response body length: %d != 0",
                conn->ip_addr, conn->port, response->header.body_len);
        return EINVAL;
    }

    return 0;
}

int fdir_send_active_test_req(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout)
{
    int ret;
    FDIRProtoHeader fdir_header_proto;

    fdir_set_admin_header(&fdir_header_proto, FDIR_PROTO_ACTIVE_TEST_REQ,
            0);
    ret = fdir_send_and_recv_response_header(conn, (char *)&fdir_header_proto,
            sizeof(FDIRProtoHeader), response, network_timeout);
    if (ret == 0) {
        ret = fdir_check_response(conn, response, network_timeout,
                FDIR_PROTO_ACTIVE_TEST_RESP);
    }

    return ret;
}

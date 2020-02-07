
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

    task->length = buff2int(header->body_len);
    return 0;
}

void fdir_proto_response_extract (FDIRProtoHeader *header_pro,
        FDIRResponseInfo *resp_info)
{
    resp_info->cmd      = header_pro->cmd;
    resp_info->body_len = buff2int(header_pro->body_len);
    resp_info->status   = header_pro->status;
}

void fdir_set_admin_header (FDIRProtoHeader *fdir_header_proto,
        unsigned char cmd, int body_len)
{
    fdir_header_proto->cmd = cmd;
    int2buff(body_len, fdir_header_proto->body_len);
}

int fdir_check_response(ConnectionInfo *conn, FDIRResponseInfo *resp_info,
        const int network_timeout, const unsigned char resp_cmd)
{
    if (resp_info->cmd == resp_cmd && resp_info->status == 0) {
        return 0;
    } else {
        if (resp_info->body_len > 0) {
            if (resp_info->body_len >= sizeof(resp_info->error.message)) {
                resp_info->error.length = sizeof(resp_info->error.message) - 1;
            } else {
                resp_info->error.length = resp_info->body_len;
            }
            tcprecvdata_nb_ex(conn->sock, resp_info->error.message,
                    resp_info->error.length, network_timeout,
                    &resp_info->error.length);
            resp_info->error.message[resp_info->error.length] = '\0';
        } else {
            resp_info->error.message[0] = '\0';
        }
        return 1;
    }
}

int send_and_recv_response_header(ConnectionInfo *conn, char *data, int len,
        FDIRResponseInfo *resp_info, int network_timeout)
{
    int ret;
    FDIRProtoHeader fdir_header_resp_pro;

    if ((ret = tcpsenddata_nb(conn->sock, data,
            len, network_timeout)) != 0) {
        return ret;
    }
    if ((ret = tcprecvdata_nb_ex(conn->sock, &fdir_header_resp_pro,
            sizeof(FDIRProtoHeader), network_timeout, NULL)) != 0) {
        return ret;
    }
    fdir_proto_response_extract(&fdir_header_resp_pro, resp_info);
    return 0;
}

int fdir_send_active_test_req(ConnectionInfo *conn, FDIRResponseInfo *resp_info,
        const int network_timeout)
{
    int ret;
    FDIRProtoHeader fdir_header_proto;

    fdir_set_admin_header(&fdir_header_proto, FDIR_PROTO_ACTIVE_TEST_REQ,
            0);
    ret = send_and_recv_response_header(conn, (char *)&fdir_header_proto,
            sizeof(FDIRProtoHeader), resp_info, network_timeout);
    if (ret == 0) {
        ret = fdir_check_response(conn, resp_info, network_timeout,
                FDIR_PROTO_ACTIVE_TEST_RESP);
    }

    return ret;
}

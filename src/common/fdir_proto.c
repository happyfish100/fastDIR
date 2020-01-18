
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
    task->length = buff2int(((FDIRProtoHeader *)task->data)->body_len);
    return 0;
}

int fdir_proto_deal_actvie_test(struct fast_task_info *task,
        const FDIRRequestInfo *request, FDIRResponseInfo *response)
{
    return FDIR_PROTO_EXPECT_BODY_LEN(task, request, response, 0);
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

int fdir_check_response(ConnectionInfo *join_conn,
        FDIRResponseInfo *resp_info, int network_timeout, unsigned char resp_cmd)
{
    if (resp_info->cmd == resp_cmd && resp_info->status == 0) {
        return 0;
    } else {
        if (resp_info->body_len) {
            tcprecvdata_nb_ex(join_conn->sock, resp_info->error.message,
                    resp_info->body_len, network_timeout, NULL);
            resp_info->error.message[resp_info->body_len] = '\0';
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
        int network_timeout)
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

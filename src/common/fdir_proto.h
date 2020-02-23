#ifndef _FDIR_PROTO_H
#define _FDIR_PROTO_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fdir_types.h"

#define FDIR_PROTO_ACK                      6

#define FDIR_PROTO_ACTIVE_TEST_REQ         35
#define FDIR_PROTO_ACTIVE_TEST_RESP        36

#define FDIR_PROTO_CREATE_DENTRY           41
#define FDIR_PROTO_REMOVE_DENTRY           43

#define FDIR_PROTO_LIST_DENTRY_FIRST_REQ   45
#define FDIR_PROTO_LIST_DENTRY_NEXT_REQ    47
#define FDIR_PROTO_LIST_DENTRY_RESP        48

//cluster commands
#define FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ   61
#define FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP  62
#define FDIR_CLUSTER_PROTO_JOIN_MASTER             63
#define FDIR_CLUSTER_PROTO_PING_MASTER_REQ         65
#define FDIR_CLUSTER_PROTO_PING_MASTER_RESP        66
#define FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER     67  //notify next leader to other servers
#define FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER      68  //commit next leader to other servers
#define FDIR_CLUSTER_PROTO_NOTIFY_RESELECT_MASTER  69  //followers notify reselect leader when split-brain
#define FDIR_CLUSTER_PROTO_MASTER_PUSH_CLUSTER_CHG 70
#define FDIR_CLUSTER_PROTO_MASTER_PUSH_BINLOG      71

#define FDIR_PROTO_MAGIC_CHAR        '#'
#define FDIR_PROTO_SET_MAGIC(m)   \
    m[0] = m[1] = m[2] = m[3] = FDIR_PROTO_MAGIC_CHAR

#define FDIR_PROTO_CHECK_MAGIC(m) \
    (m[0] == FDIR_PROTO_MAGIC_CHAR && m[1] == FDIR_PROTO_MAGIC_CHAR && \
     m[2] == FDIR_PROTO_MAGIC_CHAR && m[3] == FDIR_PROTO_MAGIC_CHAR)

#define FDIR_PROTO_MAGIC_FORMAT "0x%02X%02X%02X%02X"
#define FDIR_PROTO_MAGIC_EXPECT_PARAMS \
    FDIR_PROTO_MAGIC_CHAR, FDIR_PROTO_MAGIC_CHAR, \
    FDIR_PROTO_MAGIC_CHAR, FDIR_PROTO_MAGIC_CHAR

#define FDIR_PROTO_MAGIC_PARAMS(m) \
    m[0], m[1], m[2], m[3]

#define FDIR_PROTO_SET_HEADER(header, _cmd, _body_len) \
    do {  \
        FDIR_PROTO_SET_MAGIC((header)->magic);   \
        (header)->cmd = _cmd;      \
        (header)->status[0] = (header)->status[1] = 0; \
        int2buff(_body_len, (header)->body_len); \
    } while (0)

#define FDIR_PROTO_SET_RESPONSE_HEADER(proto_header, resp_header) \
    do {  \
        (proto_header)->cmd = (resp_header).cmd;       \
        short2buff((resp_header).status, (proto_header)->status);  \
        int2buff((resp_header).body_len, (proto_header)->body_len);\
    } while (0)

typedef struct fdir_proto_header {
    unsigned char magic[4]; //magic number
    char body_len[4];       //body length
    char status[2];         //status to store errno
    char flags[2];
    unsigned char cmd;      //the command code
    char padding[3];
} FDIRProtoHeader;

typedef struct fdir_proto_dentry_info {
    unsigned char ns_len;  //namespace length
    char path_len[2];
    char padding[5];
    char ns_str[0];      //namespace string
    //char *path_str;    //path_str = ns_str + ns_len
} FDIRProtoDEntryInfo;

typedef struct fdir_proto_create_dentry_front {
    char flags[4];
    char mode[4];
} FDIRProtoCreateDEntryFront;

typedef struct fdir_proto_create_dentry_body {
    FDIRProtoCreateDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoCreateDEntryBody;

typedef struct fdir_proto_remove_dentry{
    FDIRProtoDEntryInfo dentry;
} FDIRProtoRemoveDEntry;

typedef struct fdir_proto_list_dentry_first_body {
    FDIRProtoDEntryInfo dentry;
} FDIRProtoListDEntryFirstBody;

typedef struct fdir_proto_list_dentry_next_body {
    char token[8];
    char offset[4];    //for check, must be same with server's
    char padding[4];
} FDIRProtoListDEntryNextBody;

typedef struct fdir_proto_list_dentry_resp_body_header {
    char token[8];
    char count[4];
    char is_last;
    char padding[3];
} FDIRProtoListDEntryRespBodyHeader;

typedef struct fdir_proto_list_dentry_resp_body_part {
    unsigned char name_len;
    char name_str[0];
} FDIRProtoListDEntryRespBodyPart;

typedef struct fdir_proto_get_server_status_req {
    char server_id[4];
    char config_sign[16];
} FDIRProtoGetServerStatusReq;

typedef struct fdir_proto_get_server_status_resp {
    char is_master;
    char server_id[4];
    char data_version[8];
} FDIRProtoGetServerStatusResp;

typedef struct fdir_proto_join_master_req {
    char server_id[4];
    char config_sign[16];
    char data_version[8];
} FDIRProtoJoinMasterReq;

#ifdef __cplusplus
extern "C" {
#endif

void fdir_proto_init();

int fdir_proto_set_body_length(struct fast_task_info *task);

int fdir_check_response(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd);

int fdir_send_and_recv_response_header(ConnectionInfo *conn, char *data,
        const int len, FDIRResponseInfo *response, const int network_timeout);

static inline int fdir_send_and_check_response_header(ConnectionInfo *conn,
        char *data, const int len, FDIRResponseInfo *response,
        const int network_timeout,  const unsigned char expect_cmd)
{
    int result;

    if ((result=fdir_send_and_recv_response_header(conn, data, len,
                    response, network_timeout)) != 0)
    {
        return result;
    }


    if ((result=fdir_check_response(conn, response, network_timeout,
                    expect_cmd)) != 0)
    {
        return result;
    }

    return 0;
}

int fdir_send_and_recv_none_body_response(ConnectionInfo *conn, char *data,
        const int len, FDIRResponseInfo *response, int network_timeout,
        const unsigned char expect_cmd);

static inline void fdir_proto_extract_header(FDIRProtoHeader *header_proto,
        FDIRHeaderInfo *header_info)
{
    header_info->cmd = header_proto->cmd;
    header_info->body_len = buff2int(header_proto->body_len);
    header_info->flags = buff2short(header_proto->flags);
    header_info->status = buff2short(header_proto->status);
}

int fdir_send_active_test_req(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout);

#ifdef __cplusplus
}
#endif

#endif

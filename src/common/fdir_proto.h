#ifndef _FDIR_PROTO_H
#define _FDIR_PROTO_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fdir_types.h"

#define FDIR_PROTO_ACK                 6

#define FDIR_PROTO_ACTIVE_TEST_REQ    35
#define FDIR_PROTO_ACTIVE_TEST_RESP   36

#define FDIR_PROTO_CREATE_DENTRY      43
#define FDIR_PROTO_REMOVE_DENTRY      45
#define FDIR_PROTO_LIST_DENTRY_REQ    47
#define FDIR_PROTO_LIST_DENTRY_RESP   48

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

#define FDIR_PROTO_SET_HEADER(header, _cmd, _body_len)  \
    do {  \
        FDIR_PROTO_SET_MAGIC((header)->magic);   \
        (header)->cmd = _cmd;      \
        (header)->status = 0;      \
        int2buff(_body_len, (header)->body_len); \
    } while (0)

typedef struct fdir_proto_header {
    unsigned char magic[4];       //magic number
    char body_len[4];    //body length
    unsigned char cmd;   //the command code
    unsigned char status;//status to store errno
    char flags[2];
    char padding[4];
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

typedef struct fdir_proto_list_dentry {
    struct {
        char offset[2];
        char count[2];
    } limit;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoListDEntry;

#ifdef __cplusplus
extern "C" {
#endif

void fdir_proto_init();

int fdir_proto_set_body_length(struct fast_task_info *task);

int fdir_check_response(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd);

int fdir_send_and_recv_response_header(ConnectionInfo *conn, char *data,
        const int len, FDIRResponseInfo *response, const int network_timeout);

int fdir_send_and_recv_none_body_response(ConnectionInfo *conn, char *data,
        const int len, FDIRResponseInfo *response, int network_timeout,
        const unsigned char expect_cmd);

static inline void fdir_proto_extract_header(FDIRProtoHeader *header_proto,
        FDIRHeaderInfo *header_info)
{
    header_info->cmd = header_proto->cmd;
    header_info->body_len = buff2int(header_proto->body_len);
    header_info->flags = buff2short(header_proto->flags);
    header_info->status = header_proto->status;
}

int fdir_send_active_test_req(ConnectionInfo *conn, FDIRResponseInfo *response,
        const int network_timeout);

#ifdef __cplusplus
}
#endif

#endif

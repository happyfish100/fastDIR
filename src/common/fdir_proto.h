#ifndef _FDIR_PROTO_H
#define _FDIR_PROTO_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fdir_types.h"

#define FDIR_STATUS_MASTER_INCONSISTENT     9999

#define FDIR_PROTO_ACK                      6

#define FDIR_PROTO_ACTIVE_TEST_REQ         21
#define FDIR_PROTO_ACTIVE_TEST_RESP        22

//service commands
#define FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ       25
#define FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP      26
#define FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ       27
#define FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP      28

#define FDIR_SERVICE_PROTO_LIST_DENTRY_FIRST_REQ   29
#define FDIR_SERVICE_PROTO_LIST_DENTRY_NEXT_REQ    31
#define FDIR_SERVICE_PROTO_LIST_DENTRY_RESP        32
#define FDIR_SERVICE_PROTO_STAT_BY_PATH_REQ        33
#define FDIR_SERVICE_PROTO_STAT_BY_PATH_RESP       34
#define FDIR_SERVICE_PROTO_STAT_BY_INODE_REQ       35  //TODO
#define FDIR_SERVICE_PROTO_STAT_BY_INODE_RESP      36

#define FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_REQ     37  //modified by inode
#define FDIR_SERVICE_PROTO_SET_DENTRY_SIZE_RESP    38
#define FDIR_SERVICE_PROTO_SET_DENTRY_PERM_REQ     39
#define FDIR_SERVICE_PROTO_SET_DENTRY_PERM_RESP    40
#define FDIR_SERVICE_PROTO_FLOCK_DENTRY_REQ        41
#define FDIR_SERVICE_PROTO_FLOCK_DENTRY_RESP       42

#define FDIR_SERVICE_PROTO_SERVICE_STAT_REQ        45
#define FDIR_SERVICE_PROTO_SERVICE_STAT_RESP       46
#define FDIR_SERVICE_PROTO_CLUSTER_STAT_REQ        47
#define FDIR_SERVICE_PROTO_CLUSTER_STAT_RESP       48

#define FDIR_SERVICE_PROTO_GET_MASTER_REQ           51
#define FDIR_SERVICE_PROTO_GET_MASTER_RESP          52
#define FDIR_SERVICE_PROTO_GET_SLAVES_REQ           53
#define FDIR_SERVICE_PROTO_GET_SLAVES_RESP          54
#define FDIR_SERVICE_PROTO_GET_READABLE_SERVER_REQ  55
#define FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP 56

//cluster commands
#define FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_REQ   71
#define FDIR_CLUSTER_PROTO_GET_SERVER_STATUS_RESP  72
#define FDIR_CLUSTER_PROTO_JOIN_MASTER             73  //slave  -> master
#define FDIR_CLUSTER_PROTO_PING_MASTER_REQ         75
#define FDIR_CLUSTER_PROTO_PING_MASTER_RESP        76
#define FDIR_CLUSTER_PROTO_PRE_SET_NEXT_MASTER     77  //notify next leader to other servers
#define FDIR_CLUSTER_PROTO_COMMIT_NEXT_MASTER      78  //commit next leader to other servers

//replication commands, master -> slave
#define FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ          81
#define FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP         82
#define FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ         83
#define FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP        84

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
    char mode[4];
} FDIRProtoCreateDEntryFront;

typedef struct fdir_proto_create_dentry_body {
    FDIRProtoCreateDEntryFront front;
    FDIRProtoDEntryInfo dentry;
} FDIRProtoCreateDEntryBody;

typedef struct fdir_proto_remove_dentry{
    FDIRProtoDEntryInfo dentry;
} FDIRProtoRemoveDEntry;

typedef struct fdir_proto_set_modify_stat_req {
    char inode[8];
    char size[8];   /* file size in bytes */
    char force;
    unsigned char ns_len;  //namespace length
    char padding[6];
    char ns_str[0];      //namespace for hash code
} FDIRProtoSetModifyStatReq;

typedef struct fdir_proto_stat_dentry_resp {
    char inode[8];
    char mode[4];
    char ctime[4];  /* create time */
    char mtime[4];  /* modify time */
    char size[8];   /* file size in bytes */
} FDIRProtoStatDEntryResp;

typedef struct fdir_proto_flock_dentry_req {
    char inode[8];
    char offset[8];  /* lock region offset, -1 for file end */
    char length[8];  /* lock region  length, 0 for until end of file */
    char owner[8];
    char type[1];    /* lock type, LOCK_SH for read shared lock,
                        LOCK_EX for write exclusive lock,
                        LOCK_UN for unlock */
    char padding[7];
} FDIRProtoFlockDEntryReq;

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

typedef struct fdir_proto_service_stat_resp {
    char server_id[4];
    char is_master;
    char status;

    struct {
        char current_count[4];
        char max_count[4];
    } connection;

    struct {
        char current_data_version[8];
        char current_inode_sn[8];
        struct {
            char ns[8];
            char dir[8];
            char file[8];
        } counters;
    } dentry;
} FDIRProtoServiceStatResp;

typedef struct fdir_proto_cluster_stat_resp_body_header {
    char count[4];
} FDIRProtoClusterStatRespBodyHeader;

typedef struct fdir_proto_cluster_stat_resp_body_part {
    char server_id[4];
    char is_master;
    char status;
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
} FDIRProtoClusterStatRespBodyPart;

/* for FDIR_SERVICE_PROTO_GET_MASTER_RESP and
   FDIR_SERVICE_PROTO_GET_READABLE_SERVER_RESP
   */
typedef struct fdir_proto_get_server_resp {
    char server_id[4];
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
} FDIRProtoGetServerResp;

typedef struct fdir_proto_get_slaves_resp_body_header {
    char count[2];
} FDIRProtoGetSlavesRespBodyHeader;

typedef struct fdir_proto_get_slaves_resp_body_part {
    char server_id[4];
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
    char status;
} FDIRProtoGetSlavesRespBodyPart;

typedef struct fdir_proto_get_server_status_req {
    char server_id[4];
    char config_sign[16];
} FDIRProtoGetServerStatusReq;

typedef struct fdir_proto_get_server_status_resp {
    char is_master;
    char status;
    char server_id[4];
    char data_version[8];
} FDIRProtoGetServerStatusResp;

typedef struct fdir_proto_join_master_req {
    char cluster_id[4];    //the cluster id
    char server_id[4];     //the slave server id
    char config_sign[16];
    char key[FDIR_REPLICA_KEY_SIZE];   //the slave key used on JOIN_SLAVE
} FDIRProtoJoinMasterReq;

typedef struct fdir_proto_join_slave_req {
    char cluster_id[4];  //the cluster id
    char server_id[4];   //the master server id
    char buffer_size[4];   //the master task task size
    char key[FDIR_REPLICA_KEY_SIZE];  //the slave key passed / set by JOIN_MASTER
} FDIRProtoJoinSlaveReq;

typedef struct fdir_proto_join_slave_resp {
    struct {
        char index[4];   //binlog file index
        char offset[8];  //binlog file offset
    } binlog_pos_hint;
    char last_data_version[8];   //the slave's last data version
} FDIRProtoJoinSlaveResp;

typedef struct fdir_proto_ping_master_resp_header {
    char inode_sn[8];  //current inode sn of master
    char server_count[4];
} FDIRProtoPingMasterRespHeader;

typedef struct fdir_proto_ping_master_resp_body_part {
    char server_id[4];
    char status;
} FDIRProtoPingMasterRespBodyPart;

typedef struct fdir_proto_push_binlog_req_body_header {
    char binlog_length[4];
    char last_data_version[8];
} FDIRProtoPushBinlogReqBodyHeader;

typedef struct fdir_proto_push_binlog_resp_body_header {
    char count[4];
} FDIRProtoPushBinlogRespBodyHeader;

typedef struct fdir_proto_push_binlog_resp_body_part {
    char data_version[8];
    char err_no[2];
} FDIRProtoPushBinlogRespBodyPart;

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

int fdir_send_and_recv_response(ConnectionInfo *conn, char *send_data,
        const int send_len, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd,
        char *recv_data, const int expect_body_len);

static inline int fdir_send_and_recv_none_body_response(ConnectionInfo *conn,
        char *send_data, const int send_len, FDIRResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd)
{
    char *recv_data = NULL;
    const int expect_body_len = 0;

    return fdir_send_and_recv_response(conn, send_data, send_len, response,
        network_timeout, expect_cmd, recv_data, expect_body_len);
}

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

const char *fdir_get_server_status_caption(const int status);

const char *fdir_get_cmd_caption(const int cmd);

#ifdef __cplusplus
}
#endif

#endif

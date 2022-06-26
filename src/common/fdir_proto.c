/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */


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

const char *fdir_get_server_status_caption(const int status)
{

    switch (status) {
        case FDIR_SERVER_STATUS_INIT:
            return "INIT";
        case FDIR_SERVER_STATUS_BUILDING:
            return "BUILDING";
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
        case FDIR_SERVICE_PROTO_CLIENT_JOIN_REQ:
            return "CLIENT_JOIN_REQ";
        case FDIR_SERVICE_PROTO_CLIENT_JOIN_RESP:
            return "CLIENT_JOIN_RESP";
        case FDIR_SERVICE_PROTO_CREATE_DENTRY_REQ:
            return "CREATE_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_CREATE_DENTRY_RESP:
            return "CREATE_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_CREATE_BY_PNAME_REQ:
            return "CREATE_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_CREATE_BY_PNAME_RESP:
            return "CREATE_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_SYMLINK_DENTRY_REQ:
            return "SYMLINK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_SYMLINK_DENTRY_RESP:
            return "SYMLINK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_REQ:
            return "SYMLINK_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_SYMLINK_BY_PNAME_RESP:
            return "SYMLINK_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_HDLINK_DENTRY_REQ:
            return "HDLINK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_HDLINK_DENTRY_RESP:
            return "HDLINK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_REQ:
            return "HDLINK_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_HDLINK_BY_PNAME_RESP:
            return "HDLINK_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_READLINK_BY_INODE_REQ:
            return "READLINK_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_READLINK_BY_INODE_RESP:
            return "READLINK_BY_INODE_RESP";
        case FDIR_SERVICE_PROTO_READLINK_BY_PATH_REQ:
            return "READLINK_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_READLINK_BY_PATH_RESP:
            return "READLINK_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_READLINK_BY_PNAME_REQ:
            return "READLINK_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_READLINK_BY_PNAME_RESP:
            return "READLINK_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_REMOVE_DENTRY_REQ:
            return "REMOVE_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_REMOVE_DENTRY_RESP:
            return "REMOVE_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_REQ:
            return "REMOVE_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_REMOVE_BY_PNAME_RESP:
            return "REMOVE_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_RENAME_DENTRY_REQ:
            return "RENAME_DENTRY_REQ";
        case FDIR_SERVICE_PROTO_RENAME_DENTRY_RESP:
            return "RENAME_DENTRY_RESP";
        case FDIR_SERVICE_PROTO_RENAME_BY_PNAME_REQ:
            return "RENAME_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_RENAME_BY_PNAME_RESP:
            return "RENAME_BY_PNAME_RESP";
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_REQ:
            return "SET_XATTR_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_PATH_RESP:
            return "SET_XATTR_BY_PATH_RESP";
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_REQ:
            return "SET_XATTR_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_SET_XATTR_BY_INODE_RESP:
            return "SET_XATTR_BY_INODE_RESP";
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_REQ:
            return "REMOVE_XATTR_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_PATH_RESP:
            return "REMOVE_XATTR_BY_PATH_RESP";
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_REQ:
            return "REMOVE_XATTR_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_REMOVE_XATTR_BY_INODE_RESP:
            return "REMOVE_XATTR_BY_INODE_RESP";
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_REQ:
            return "LOOKUP_INODE_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PATH_RESP:
            return "LOOKUP_INODE_BY_PATH_RESP";
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_REQ:
            return "LOOKUP_INODE_BY_PNAME_REQ";
        case FDIR_SERVICE_PROTO_LOOKUP_INODE_BY_PNAME_RESP:
            return "LOOKUP_INODE_BY_PNAME_RESP";
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
        case FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_REQ:
            return "BATCH_SET_DENTRY_SIZE_REQ";
        case FDIR_SERVICE_PROTO_BATCH_SET_DENTRY_SIZE_RESP:
            return "BATCH_SET_DENTRY_SIZE_RESP";
        case FDIR_SERVICE_PROTO_MODIFY_STAT_BY_INODE_REQ:
            return "MODIFY_STAT_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_MODIFY_STAT_BY_INODE_RESP:
            return "MODIFY_STAT_BY_INODE_RESP";
        case FDIR_SERVICE_PROTO_MODIFY_STAT_BY_PATH_REQ:
            return "MODIFY_STAT_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_MODIFY_STAT_BY_PATH_RESP:
            return "MODIFY_STAT_BY_PATH_RESP";
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
        case FDIR_SERVICE_PROTO_NAMESPACE_STAT_REQ:
            return "NAMESPACE_STAT_REQ";
        case FDIR_SERVICE_PROTO_NAMESPACE_STAT_RESP:
            return "NAMESPACE_STAT_RESP";
        case FDIR_SERVICE_PROTO_GENERATE_NODE_ID_REQ:
            return "GENERATE_NODE_ID_REQ";
        case FDIR_SERVICE_PROTO_GENERATE_NODE_ID_RESP:
            return "GENERATE_NODE_ID_RESP";
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

        case FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_REQ:
            return "GET_XATTR_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_PATH_RESP:
            return "GET_XATTR_BY_PATH_RESP";
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_REQ:
            return "GET_XATTR_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_GET_XATTR_BY_INODE_RESP:
            return "GET_XATTR_BY_INODE_RESP";
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_REQ:
            return "LIST_XATTR_BY_PATH_REQ";
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_PATH_RESP:
            return "LIST_XATTR_BY_PATH_RESP";
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_REQ:
            return "LIST_XATTR_BY_INODE_REQ";
        case FDIR_SERVICE_PROTO_LIST_XATTR_BY_INODE_RESP:
            return "LIST_XATTR_BY_INODE_RESP";

        case FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_REQ:
            return "NSS_SUBSCRIBE_REQ";
        case FDIR_SERVICE_PROTO_NSS_SUBSCRIBE_RESP:
            return "NSS_SUBSCRIBE_RESP";
        case FDIR_SERVICE_PROTO_NSS_FETCH_REQ:
            return "NSS_FETCH_REQ";
        case FDIR_SERVICE_PROTO_NSS_FETCH_RESP:
            return "NSS_FETCH_RESP";

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

        case FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ:
            return "QUERY_BINLOG_INFO_REQ";
        case FDIR_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP:
            return "QUERY_BINLOG_INFO_RESP";
        case FDIR_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ:
            return "SYNC_BINLOG_FIRST_REQ";
        case FDIR_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ:
            return "SYNC_BINLOG_NEXT_REQ";
        case FDIR_REPLICA_PROTO_SYNC_BINLOG_RESP:
            return "SYNC_BINLOG_RESP";
        case FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_REQ:
            return "SYNC_DUMP_MARK_REQ";
        case FDIR_REPLICA_PROTO_SYNC_DUMP_MARK_RESP:
            return "SYNC_DUMP_MARK_RESP";
        case FDIR_REPLICA_PROTO_SYNC_BINLOG_REPORT:
            return "SYNC_BINLOG_REPORT";
        case FDIR_REPLICA_PROTO_JOIN_SLAVE_REQ:
            return "JOIN_SLAVE_REQ";
        case FDIR_REPLICA_PROTO_JOIN_SLAVE_RESP:
            return "JOIN_SLAVE_RESP";
        case FDIR_REPLICA_PROTO_FORWORD_REQUESTS_REQ:
            return "FORWORD_REQUESTS_REQ";
        case FDIR_REPLICA_PROTO_PUSH_BINLOG_REQ:
            return "PUSH_BINLOG_REQ";
        case FDIR_REPLICA_PROTO_PUSH_BINLOG_RESP:
            return "PUSH_BINLOG_RESP";
        case FDIR_REPLICA_PROTO_NOTIFY_SLAVE_QUIT:
            return "NOTIFY_SLAVE_QUIT";
        default:
            return sf_get_cmd_caption(cmd);
    }
}

int fdir_proto_get_master(ConnectionInfo *conn,
        const int network_timeout,
        FDIRClientServerEntry *master)
{
    int result;
    FDIRProtoHeader *header;
    SFResponseInfo response;
    FDIRProtoGetServerResp server_resp;
    char out_buff[sizeof(FDIRProtoHeader)];

    header = (FDIRProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FDIR_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FDIRProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    sizeof(out_buff), &response, network_timeout,
                    FDIR_SERVICE_PROTO_GET_MASTER_RESP, (char *)
                    &server_resp, sizeof(FDIRProtoGetServerResp))) != 0)
    {
        fdir_log_network_error(&response, conn, result);
    } else {
        master->server_id = buff2int(server_resp.server_id);
        memcpy(master->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(master->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        master->conn.port = buff2short(server_resp.port);
    }

    return result;
}

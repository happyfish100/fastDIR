#ifndef _FDIR_TYPES_H
#define _FDIR_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/connection_pool.h"

#define FDIR_ERROR_INFO_SIZE   256

#define FDIR_NETWORK_TIMEOUT_DEFAULT    30
#define FDIR_CONNECT_TIMEOUT_DEFAULT    30

#define FDIR_SERVER_DEFAULT_INNER_PORT  11011
#define FDIR_SERVER_DEFAULT_OUTER_PORT  11011

#define FDIR_MAX_PATH_COUNT  128

typedef struct {
    unsigned char cmd; //command
    short flags;
    int body_len;      //body length
    int status;
} FDIRHeaderInfo;

typedef struct {
    FDIRHeaderInfo header;
    bool forwarded;    //if forwarded request
    char *body;
} FDIRRequestInfo;

typedef struct {
    FDIRHeaderInfo header;
    struct {
        int length;
        char message[FDIR_ERROR_INFO_SIZE];
    } error;
} FDIRResponseInfo;

typedef struct fdir_dstatus {
    int64_t inode;
    mode_t mode;
    int ctime;  /* create time */
    int mtime;  /* modify time */
    int atime;  /* last access time */
    int64_t size;   /* file size in bytes */
} FDIRDStatus;

typedef struct fdir_dentry_info {
    string_t ns;
    string_t path;
} FDIRDEntryInfo;

typedef struct fdir_server_group {
    int count;
    ConnectionInfo *servers;
} FDIRServerGroup;

typedef struct fdir_slave_group {
    int count;
    int index;  //server index for roundrobin
    ConnectionInfo **servers;
} FDIRSlaveGroup;

typedef struct fdir_server_cluster {
    FDIRServerGroup server_group;
    FDIRSlaveGroup slave_group;
    ConnectionInfo *master;
} FDIRServerCluster;

#endif

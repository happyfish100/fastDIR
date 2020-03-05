#ifndef _FDIR_TYPES_H
#define _FDIR_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/server_id_func.h"

#define FDIR_ERROR_INFO_SIZE   256
#define FDIR_REPLICA_KEY_SIZE    8

#define FDIR_NETWORK_TIMEOUT_DEFAULT    30
#define FDIR_CONNECT_TIMEOUT_DEFAULT     5

#define FDIR_DEFAULT_BINLOG_BUFFER_SIZE (64 * 1024)

#define FDIR_SERVER_DEFAULT_CLUSTER_PORT  11011
#define FDIR_SERVER_DEFAULT_SERVICE_PORT  11012

#define FDIR_MAX_PATH_COUNT  128

typedef struct {
    int body_len;      //body length
    short flags;
    short status;
    unsigned char cmd; //command
} FDIRHeaderInfo;

typedef struct {
    FDIRHeaderInfo header;
    char *body;
} FDIRRequestInfo;

typedef struct {
    FDIRHeaderInfo header;
    struct {
        int length;
        char message[FDIR_ERROR_INFO_SIZE];
    } error;
} FDIRResponseInfo;

typedef struct fdir_dentry_full_name {
    string_t ns;    //namespace
    string_t path;  //full path
} FDIRDEntryFullName;

#endif

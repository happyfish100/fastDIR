#ifndef _FDIR_TYPES_SERVER_H
#define _FDIR_TYPES_SERVER_H

#include <time.h>
#include "fastcommon/common_define.h"

#define FDIR_ERROR_INFO_SIZE   256

#define FDIR_NETWORK_TIMEOUT_DEFAULT    30
#define FDIR_CONNECT_TIMEOUT_DEFAULT    30

#define FDIR_SERVER_DEFAULT_INNER_PORT  11011
#define FDIR_SERVER_DEFAULT_OUTER_PORT  11011

typedef struct {
    unsigned char cmd;  //response command
    int body_len;       //response body length
} FDIRRequestInfo;

typedef struct {
    struct {
        char message[FDIR_ERROR_INFO_SIZE];
        int length;
    } error;
    int status;
    int body_len;    //response body length
    bool response_done;
    bool log_error;
    unsigned char cmd;   //response command
} FDIRResponseInfo;

#endif

#ifndef _CLIENT_TYPES_H
#define _CLIENT_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/connection_pool.h"
#include "fdir_types.h"

struct fdir_client_context;

typedef ConnectionInfo *(*fdir_get_connection_func)(
        struct fdir_client_context *client_ctx, int *err_no);
typedef void (*fdir_release_connection_func)(ConnectionInfo *conn);
typedef void (*fdir_close_connection_func)(ConnectionInfo *conn);

typedef struct fdir_dstatus {
    int64_t inode;
    mode_t mode;
    int ctime;  /* create time */
    int mtime;  /* modify time */
    int atime;  /* last access time */
    int64_t size;   /* file size in bytes */
} FDIRDStatus;

typedef struct fdir_client_server_entry {
    int server_id;
    char ip_addr[IP_ADDRESS_SIZE];
    short port;
    char status;
} FDIRClientServerEntry;

typedef struct fdir_server_group {
    int alloc_size;
    int count;
    ConnectionInfo *servers;
} FDIRServerGroup;

typedef struct fdir_connection_manager {
    fdir_get_connection_func get_connection;   //get connection by the config
    fdir_get_connection_func get_master_connection;
    fdir_get_connection_func get_readable_connection;
    
    fdir_release_connection_func release_connection; //eg. push back pool
    fdir_close_connection_func close_connection;     //should disconnect the conneciton
    void *args;
} FDIRConnectionManager;

typedef struct fdir_client_context {
    FDIRServerGroup server_group;
    FDIRConnectionManager conn_manager;
    bool is_simple_conn_mananger;
} FDIRClientContext;

#endif

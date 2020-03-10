//binlog_replication.h

#ifndef _BINLOG_REPLICATION_H_
#define _BINLOG_REPLICATION_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replication_add_to_thread(FDIRSlaveReplication *replication);
int binlog_replication_process(FDIRServerContext *server_ctx);

#ifdef __cplusplus
}
#endif

#endif

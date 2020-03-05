//binlog_func.h

#ifndef _BINLOG_FUNC_H_
#define _BINLOG_FUNC_H_

#include "binlog_types.h"


#define BINLOG_BUFFER_LENGTH(buffer) ((buffer).end - (buffer).buff)
#define BINLOG_BUFFER_REMAIN(buffer) ((buffer).end - (buffer).current)

#ifdef __cplusplus
extern "C" {
#endif

int binlog_buffer_init(ServerBinlogBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif

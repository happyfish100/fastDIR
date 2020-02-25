//binlog_func.h

#ifndef _BINLOG_FUNC_H_
#define _BINLOG_FUNC_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_buffer_init(ServerBinlogBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif

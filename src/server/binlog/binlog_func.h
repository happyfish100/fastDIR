//binlog_func.h

#ifndef _BINLOG_FUNC_H_
#define _BINLOG_FUNC_H_

#include "sf/sf_func.h"
#include "../server_global.h"
#include "binlog_types.h"


#ifdef __cplusplus
extern "C" {
#endif

static inline int binlog_buffer_init(SFBinlogBuffer *buffer)
{
    const int size = BINLOG_BUFFER_SIZE;
    return sf_binlog_buffer_init(buffer, size);
}

#ifdef __cplusplus
}
#endif

#endif

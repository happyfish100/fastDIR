//binlog_func.h

#ifndef _BINLOG_FUNC_H_
#define _BINLOG_FUNC_H_

#include "binlog_types.h"


#ifdef __cplusplus
extern "C" {
#endif

int binlog_buffer_init(ServerBinlogBuffer *buffer);

static inline void binlog_buffer_destroy(ServerBinlogBuffer *buffer)
{
    if (buffer->buff != NULL) {
        free(buffer->buff);
        buffer->current = buffer->end = buffer->buff = NULL;
        buffer->size = 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif

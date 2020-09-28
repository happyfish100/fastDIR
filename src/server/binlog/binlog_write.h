//binlog_write.h

#ifndef _BINLOG_WRITE_H_
#define _BINLOG_WRITE_H_

#include "sf/sf_binlog_writer.h"
#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern SFBinlogWriterContext g_binlog_writer_ctx;

int binlog_write_init();
static inline void binlog_write_finish()
{
    sf_binlog_writer_finish(&g_binlog_writer_ctx.writer);
}

static inline int binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&g_binlog_writer_ctx.writer);
}

static inline void binlog_get_current_write_position(
        SFBinlogFilePosition *position)
{
    position->index = g_binlog_writer_ctx.writer.binlog.index;
    position->offset = g_binlog_writer_ctx.writer.file.size;
}

static inline int push_to_binlog_write_queue(ServerBinlogRecordBuffer *rbuffer)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &g_binlog_writer_ctx.thread)) == NULL)
    {
        return ENOMEM;
    }

    memcpy(wbuffer->bf.buff, rbuffer->buffer.data, rbuffer->buffer.length);
    wbuffer->bf.length = rbuffer->buffer.length;
    wbuffer->version = rbuffer->data_version;
    sf_push_to_binlog_write_queue(&g_binlog_writer_ctx.thread, wbuffer);
    return 0;
}

#ifdef __cplusplus
}
#endif

#endif

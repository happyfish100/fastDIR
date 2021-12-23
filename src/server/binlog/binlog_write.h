/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

//binlog_write.h

#ifndef _BINLOG_WRITE_H_
#define _BINLOG_WRITE_H_

#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern SFBinlogWriterContext g_binlog_writer_ctx;

int binlog_write_init();

static inline int binlog_write_set_order_by(const short order_by)
{
    return sf_binlog_writer_change_order_by(
            &g_binlog_writer_ctx.writer, order_by);
}

static inline int binlog_write_set_next_version()
{
    return sf_binlog_writer_change_next_version(&g_binlog_writer_ctx.writer,
            __sync_sub_and_fetch(&DATA_CURRENT_VERSION, 0) + 1);
}

static inline int64_t binlog_writer_get_last_version()
{
    return sf_binlog_writer_get_last_version(&g_binlog_writer_ctx.writer);
}

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
    sf_binlog_get_current_write_position(
            &g_binlog_writer_ctx.writer, position);
}

static inline int push_to_binlog_write_queue(ServerBinlogRecordBuffer *rbuffer)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &g_binlog_writer_ctx.thread)) == NULL)
    {
        return ENOMEM;
    }

    if (wbuffer->bf.alloc_size < rbuffer->buffer.length) {
        char *new_buff;
        int alloc_size;

        alloc_size = wbuffer->bf.alloc_size * 2;
        while (alloc_size < rbuffer->buffer.length) {
            alloc_size *= 2;
        }
        new_buff = (char *)fc_malloc(alloc_size);
        if (new_buff == NULL) {
            return ENOMEM;
        }

        free(wbuffer->bf.buff);
        wbuffer->bf.buff = new_buff;
        wbuffer->bf.alloc_size = alloc_size;
    }

    memcpy(wbuffer->bf.buff, rbuffer->buffer.data, rbuffer->buffer.length);
    wbuffer->bf.length = rbuffer->buffer.length;
    wbuffer->version = rbuffer->data_version;
    sf_push_to_binlog_write_queue(&g_binlog_writer_ctx.writer, wbuffer);
    return 0;
}

#ifdef __cplusplus
}
#endif

#endif

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

//binlog_writer.h

#ifndef _INODE_BINLOG_WRITER_H_
#define _INODE_BINLOG_WRITER_H_

#include "diskallocator/binlog/common/binlog_writer.h"
#include "../storage_global.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline int inode_binlog_writer_log(FDIRInodeSegmentIndexInfo
        *segment, const BufferInfo *buffer)
{
    return da_binlog_writer_log(&INODE_BINLOG_WRITER,
            segment->binlog_id, buffer);
}

static inline int inode_binlog_writer_shrink(
        FDIRInodeSegmentIndexInfo *segment)
{
    return da_binlog_writer_shrink(&INODE_BINLOG_WRITER, segment);
}

int inode_binlog_pack_record(const DAPieceFieldInfo *field,
        char *buff, const int size);

static inline void inode_binlog_pack(const DAPieceFieldInfo
        *field, BufferInfo *buffer)
{
    buffer->length = inode_binlog_pack_record(field,
            buffer->buff, buffer->alloc_size);
}

int inode_binlog_shrink_callback(DABinlogWriter *writer, void *args);

#ifdef __cplusplus
}
#endif

#endif

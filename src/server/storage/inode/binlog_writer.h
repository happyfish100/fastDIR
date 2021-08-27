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

#include "diskallocator/binlog/space/binlog_writer.h"
#include "../storage_types.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline int inode_binlog_writer_log(FDIRInodeSegmentIndexInfo
        *segment, const DABinlogOpType op_type,
        const FDIRStorageInodeIndexInfo *inode_index)
{
    return da_binlog_writer_log(&segment->writer,
            op_type, (void *)inode_index);
}

static inline int inode_binlog_writer_synchronize(
        FDIRInodeSegmentIndexInfo *segment)
{
    return da_binlog_writer_synchronize(&segment->writer);
}

static inline int inode_binlog_writer_shrink(
        FDIRInodeSegmentIndexInfo *segment)
{
    return da_binlog_writer_shrink(&segment->writer, segment);
}

int inode_binlog_pack_record_callback(void *args,
        const DABinlogOpType op_type,
        char *buff, const int size);

int inode_binlog_shrink_callback(DABinlogWriter *writer, void *args);

int inode_binlog_batch_update_callback(DABinlogWriter *writer,
            DABinlogRecord **records, const int count);

#ifdef __cplusplus
}
#endif

#endif

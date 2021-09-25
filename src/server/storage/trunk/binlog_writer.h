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

#ifndef _SLICE_BINLOG_WRITER_H_
#define _SLICE_BINLOG_WRITER_H_

#include "diskallocator/binlog/common/binlog_writer.h"
#include "trunk_types.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline int slice_binlog_writer_log(FDIRTrunkInfo
        *trunk, const DABinlogOpType op_type,
        const FDIRTrunkSliceInfo *slice)
{
    return da_binlog_writer_log(&trunk->writer,
            op_type, (void *)slice);
}

static inline int slice_binlog_writer_synchronize(
        FDIRTrunkInfo *trunk)
{
    return da_binlog_writer_synchronize(&trunk->writer);
}

int slice_binlog_pack_record_callback(void *args,
        const DABinlogOpType op_type,
        char *buff, const int size);

#ifdef __cplusplus
}
#endif

#endif

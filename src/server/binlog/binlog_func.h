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

static inline int binlog_alloc_records(FDIRBinlogRecord
        **records, const int alloc_size)
{
    int bytes;

    bytes = sizeof(FDIRBinlogRecord) * alloc_size;
    *records = (FDIRBinlogRecord *)fc_malloc(bytes);
    if (*records == NULL) {
        return ENOMEM;
    }
    memset(*records, 0, bytes);
    return 0;
}

static inline void binlog_free_records(FDIRBinlogRecord
        *records, const int alloc_size)
{
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *end;

    end = records + alloc_size;
    for (record=records; record<end; record++) {
        if (record->xattr_kvarray.elts != NULL) {
            free(record->xattr_kvarray.elts);
        }
    }

    free(records);
}

#ifdef __cplusplus
}
#endif

#endif

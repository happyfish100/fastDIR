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

//binlog_pack.h

#ifndef _BINLOG_PACK_H_
#define _BINLOG_PACK_H_

#include "fastcommon/fast_mpool.h"
#include "binlog_types.h"

#define BINLOG_RECORD_MIN_SIZE            64
#define BINLOG_RECORD_MAX_SIZE          9999
#define BINLOG_RECORD_SIZE_STRLEN          4
#define BINLOG_RECORD_SIZE_PRINTF_FMT  "%04d"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_pack_init();

int binlog_pack_record(const FDIRBinlogRecord *record, FastBuffer *buffer);

int binlog_unpack_record_ex(const char *str, const int len,
        FDIRBinlogRecord *record, const char **record_end,
        char *error_info, const int error_size,
        struct fast_mpool_man *mpool);

#define binlog_unpack_record(str, len, record, record_end, \
        error_info, error_size) \
    binlog_unpack_record_ex(str, len, record, record_end, \
            error_info, error_size, NULL)

int binlog_detect_record(const char *str, const int len,
        int64_t *data_version, const char **rec_end,
        char *error_info, const int error_size);

int binlog_detect_record_forward(const char *str, const int len,
        int64_t *data_version, int *rstart_offset, int *rend_offset,
        char *error_info, const int error_size);

int binlog_detect_record_reverse(const char *str, const int len,
        int64_t *data_version, const char **rec_end,
        char *error_info, const int error_size);

int binlog_detect_last_record_end(const char *str, const int len,
        const char **rec_end);

#ifdef __cplusplus
}
#endif

#endif

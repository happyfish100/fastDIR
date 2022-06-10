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
#include "fastcommon/json_parser.h"
#include "binlog_types.h"

#define FDIR_BINLOG_RECORD_MIN_SIZE           64
#define FDIR_BINLOG_RECORD_MAX_SIZE          (64 * 1024)

typedef struct {
    fc_json_context_t json_ctx;
} BinlogPackContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_pack_init();

int binlog_pack_context_init_ex(BinlogPackContext *context,
        const bool decode_use_mpool, const int alloc_size_once,
        const int init_buff_size);

static inline int binlog_pack_context_init(BinlogPackContext *context)
{
    const bool decode_use_mpool = false;
    const int alloc_size_once = 0;
    const int init_buff_size = 0;
    return binlog_pack_context_init_ex(context, decode_use_mpool,
            alloc_size_once, init_buff_size);
}

static inline void binlog_pack_context_reset(BinlogPackContext *context)
{
    fc_reset_json_context(&context->json_ctx);
}

void binlog_pack_context_destroy(BinlogPackContext *context);

int binlog_pack_record_ex(BinlogPackContext *context,
        const FDIRBinlogRecord *record, FastBuffer *buffer);

int binlog_repack_buffer(const char *input, const int in_len,
        const int64_t new_data_version, char *output, int *out_len);

int binlog_unpack_record_ex(BinlogPackContext *context, const char *str,
        const int len, FDIRBinlogRecord *record, const char **record_end,
        char *error_info, const int error_size, struct fast_mpool_man *mpool);

#define binlog_pack_record(record, buffer) \
    binlog_pack_record_ex(NULL, record, buffer)

#define binlog_unpack_record(str, len, record, record_end, \
        error_info, error_size) \
    binlog_unpack_record_ex(NULL, str, len, record, \
            record_end, error_info, error_size, NULL)

int binlog_unpack_inode(const char *str, const int len,
        int64_t *inode, const char **record_end,
        char *error_info, const int error_size);

int binlog_detect_record(const char *str, const int len,
        int64_t *data_version, const char **rec_end,
        char *error_info, const int error_size);

int binlog_detect_record_forward(const char *str, const int len,
        int64_t *data_version, int *rstart_offset, int *rend_offset,
        char *error_info, const int error_size);

int binlog_detect_record_reverse_ex(const char *str, const int len,
        int64_t *data_version, time_t *timestamp, const char **rec_end,
        char *error_info, const int error_size);

#define binlog_detect_record_reverse(str, len, data_version, \
        rec_end, error_info, error_size)  \
    binlog_detect_record_reverse_ex(str, len, data_version,  \
            NULL, rec_end, error_info, error_size)

int binlog_detect_last_record_end(const char *str, const int len,
        const char **rec_end);

#ifdef __cplusplus
}
#endif

#endif

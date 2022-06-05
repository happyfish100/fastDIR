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

//binlog_reader.h

#ifndef _BINLOG_READER_H_
#define _BINLOG_READER_H_

#include "sf/sf_binlog_writer.h"
#include "binlog_types.h"

typedef struct {
    const char *subdir_name;
    SFBinlogFilePosition hint_pos;
    int64_t last_data_version;
} BinlogReaderParams;

typedef struct {
    char subdir_name[64];
    char filename[PATH_MAX];
    int fd;
    int start_index;  //binlog start index
    int last_index;   //binlog last index
    SFBinlogFilePosition position;
    SFBinlogBuffer binlog_buffer;
} ServerBinlogReader;

#define BINLOG_READER_SET_PARAMS(params, sname, pos, lastdv) \
    (params).subdir_name = sname; \
    (params).hint_pos = pos;      \
    (params).last_data_version = lastdv

#define BINLOG_READER_INIT_PARAMS(params, sname) \
    (params).subdir_name = sname;  \
    (params).hint_pos.index = 0;   \
    (params).hint_pos.offset = 0;  \
    (params).last_data_version = 0

#ifdef __cplusplus
extern "C" {
#endif

int binlog_reader_init_ex(ServerBinlogReader *reader, const char *subdir_name,
        const SFBinlogFilePosition *hint_pos, const int64_t last_data_version);

#define binlog_reader_init(reader, hint_pos, last_data_version) \
    binlog_reader_init_ex(reader, FDIR_BINLOG_SUBDIR_NAME, \
        hint_pos, last_data_version)

static inline int binlog_reader_init1(ServerBinlogReader *reader,
        const BinlogReaderParams *params)
{
    return binlog_reader_init_ex(reader, params->subdir_name,
            &params->hint_pos, params->last_data_version);
}

void binlog_reader_destroy(ServerBinlogReader *reader);

int binlog_reader_read(ServerBinlogReader *reader);

int binlog_reader_integral_read(ServerBinlogReader *reader, char *buff,
        const int size, int *read_bytes, SFVersionRange *data_version);

int binlog_get_first_data_version(const char *subdir_name,
        const int file_index, int64_t *data_version);

int binlog_get_last_dv_timestamp(const char *subdir_name,
        const int file_index, int64_t *data_version,
        time_t *timestamp);

static inline int binlog_get_last_data_version(const char *subdir_name,
        const int file_index, int64_t *data_version)
{
    time_t *timestamp = NULL;
    return binlog_get_last_dv_timestamp(subdir_name,
            file_index, data_version, timestamp);
}

static inline int binlog_get_last_timestamp(const char *subdir_name,
        const int file_index, time_t *timestamp)
{
    int64_t data_version;
    return binlog_get_last_dv_timestamp(subdir_name,
            file_index, &data_version, timestamp);
}

int binlog_get_max_record_version(int64_t *data_version);

static inline int binlog_find_position_ex(const char *subdir_name,
        const SFBinlogFilePosition *hint_pos,
        const int64_t last_data_version,
        SFBinlogFilePosition *position)
{
    int result;
    ServerBinlogReader reader;

    if ((result=binlog_reader_init_ex(&reader, subdir_name,
                    hint_pos, last_data_version)) != 0)
    {
        return result;
    }

    *position = reader.position;
    binlog_reader_destroy(&reader);
    return 0;
}

#define binlog_find_position(hint_pos, last_data_version, position) \
    binlog_find_position_ex(FDIR_BINLOG_SUBDIR_NAME, hint_pos, \
            last_data_version, position)

int binlog_check_consistency(const string_t *sbinlog,
        const SFBinlogFilePosition *hint_pos,
        int *binlog_count, uint64_t *first_unmatched_dv);

#ifdef __cplusplus
}
#endif

#endif

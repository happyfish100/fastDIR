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

#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "../../server_global.h"
#include "trunk_index.h"

#define TRUNK_INDEX_FILENAME   "trunk_index.dat"
#define TRUNK_INDEX_RECORD_MAX_SIZE   64

#define TRUNK_RECORD_FIELD_COUNT   5
#define TRUNK_RECORD_FIELD_INDEX_VERSION      0
#define TRUNK_RECORD_FIELD_INDEX_ID           1
#define TRUNK_RECORD_FIELD_INDEX_FILE_SIZE    2
#define TRUNK_RECORD_FIELD_INDEX_USED_BYTES   3
#define TRUNK_RECORD_FIELD_INDEX_FREE_START   4

SFBinlogIndexContext g_trunk_index_ctx;

static int pack_record(char *buff, FDIRTrunkIndexInfo *record)
{
    return sprintf(buff, "%"PRId64" %d %d %d %d\n",
            record->version, record->trunk_id, record->file_size,
            record->used_bytes, record->free_start);
}

static int unpack_record(const string_t *line,
        FDIRTrunkIndexInfo *record, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[TRUNK_RECORD_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            TRUNK_RECORD_FIELD_COUNT, false);
    if (count != TRUNK_RECORD_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, TRUNK_RECORD_FIELD_COUNT);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(record->version, "version",
            TRUNK_RECORD_FIELD_INDEX_VERSION, ' ', 1);
    SF_BINLOG_PARSE_INT_SILENCE(record->trunk_id, "trunk id",
            TRUNK_RECORD_FIELD_INDEX_ID, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(record->file_size, "file size",
            TRUNK_RECORD_FIELD_INDEX_FILE_SIZE, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(record->used_bytes, "used bytes",
            TRUNK_RECORD_FIELD_INDEX_USED_BYTES, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(record->free_start, "free start",
            TRUNK_RECORD_FIELD_INDEX_FREE_START, '\n', 0);
    return 0;
}

void trunk_index_init()
{
    char filename[PATH_MAX];

    snprintf(filename, sizeof(filename), "%s/%s",
            STORAGE_PATH_STR, TRUNK_INDEX_FILENAME);
    sf_binlog_index_init(&g_trunk_index_ctx, "trunk", filename,
            TRUNK_INDEX_RECORD_MAX_SIZE, sizeof(FDIRTrunkIndexInfo),
            (pack_record_func)pack_record, (unpack_record_func)unpack_record);
}

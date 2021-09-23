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
#include "../storage_global.h"
#include "binlog_index.h"

#define BINLOG_INDEX_FILENAME   "ib_index.dat"
#define BINLOG_INDEX_RECORD_MAX_SIZE   64

#define BINLOG_RECORD_FIELD_COUNT   3
#define BINLOG_RECORD_FIELD_INDEX_ID           0
#define BINLOG_RECORD_FIELD_INDEX_FIRST_INODE  1
#define BINLOG_RECORD_FIELD_INDEX_LAST_INODE   2

SFBinlogIndexContext g_binlog_index_ctx;

static int pack_record(char *buff, FDIRInodeBinlogIndexInfo *record)
{
    return sprintf(buff, "%"PRId64" %"PRId64" %"PRId64"\n",
            record->binlog_id, record->inodes.first,
            record->inodes.last);
}

static int unpack_record(const string_t *line,
        FDIRInodeBinlogIndexInfo *record, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_RECORD_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_RECORD_FIELD_COUNT, false);
    if (count != BINLOG_RECORD_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, BINLOG_RECORD_FIELD_COUNT);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(record->binlog_id, "binlog id",
            BINLOG_RECORD_FIELD_INDEX_ID, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(record->inodes.first, "first inode",
            BINLOG_RECORD_FIELD_INDEX_FIRST_INODE, ' ', 1);
    SF_BINLOG_PARSE_INT_SILENCE(record->inodes.last, "last inode",
            BINLOG_RECORD_FIELD_INDEX_LAST_INODE, '\n', 1);
    return 0;
}

void binlog_index_init()
{
    char filename[PATH_MAX];

    snprintf(filename, sizeof(filename), "%s/%s",
            STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    sf_binlog_index_init(&g_binlog_index_ctx, "trunk", filename,
            BINLOG_INDEX_RECORD_MAX_SIZE, sizeof(FDIRInodeBinlogIndexInfo),
            (pack_record_func)pack_record, (unpack_record_func)unpack_record);
}

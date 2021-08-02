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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "../../server_global.h"
#include "binlog_index.h"

#define BINLOG_MIN_FIELD_COUNT   2
#define BINLOG_MAX_FIELD_COUNT   4

#define BINLOG_FIELD_INDEX_INODE    0
#define BINLOG_FIELD_INDEX_OP_TYPE  1
#define BINLOG_FIELD_INDEX_FILE_ID  2
#define BINLOG_FIELD_INDEX_OFFSET   3

#define BINLOG_RECORD_MAX_SIZE     64

static int binlog_parse(const string_t *line,
        FDIRStorageInodeIndexOpType *op_type,
        FDIRStorageInodeIndexInfo *inode_index, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_MAX_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (count < BINLOG_MIN_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, BINLOG_MIN_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(inode_index->inode, "inode",
            BINLOG_FIELD_INDEX_INODE, ' ', 0);
    *op_type = cols[BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    if (*op_type == inode_index_op_type_create) {
        if (count != BINLOG_MAX_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MAX_FIELD_COUNT);
            return EINVAL;
        }
        BINLOG_PARSE_INT_SILENCE(inode_index->file_id, "file id",
                BINLOG_FIELD_INDEX_FILE_ID, ' ', 0);
        BINLOG_PARSE_INT_SILENCE(inode_index->offset, "offset",
                BINLOG_FIELD_INDEX_OFFSET, '\n', 0);
    } else if (*op_type == inode_index_op_type_remove) {
        if (count != BINLOG_MIN_FIELD_COUNT) {
            sprintf(error_info, "field count: %d != %d",
                    count, BINLOG_MIN_FIELD_COUNT);
            return EINVAL;
        }
    } else {
        sprintf(error_info, "unkown op type: %d (0x%02x)",
                *op_type, (unsigned char)*op_type);
        return EINVAL;
    }

    return 0;
}

int binlog_index_load(const int binlog_index,
        FDIRStorageInodeIndexArray *index_array)
{
    int result;
    char filename[PATH_MAX];

    return 0;
}

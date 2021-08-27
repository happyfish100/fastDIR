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

#ifndef _INODE_BINLOG_READER_H_
#define _INODE_BINLOG_READER_H_

#include "inode_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_reader_unpack_record(const string_t *line,
        void *args, char *error_info);

int binlog_reader_load(FDIRInodeSegmentIndexInfo *segment);

int binlog_reader_get_first_inode(const uint64_t binlog_id, int64_t *inode);

int binlog_reader_get_last_inode(const uint64_t binlog_id, int64_t *inode);

#ifdef __cplusplus
}
#endif

#endif

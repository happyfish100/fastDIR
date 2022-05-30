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

//binlog_dump.h

#ifndef _BINLOG_DUMP_H_
#define _BINLOG_DUMP_H_

#include "sf/sf_func.h"
#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "binlog_types.h"

typedef struct fdir_binlog_dump_context {
    volatile int64_t current_version;
    SFSynchronizeContext sctx;
    SFBinlogWriterContext bwctx;
} FDIRBinlogDumpContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_dump_all(const char *filename);

#ifdef __cplusplus
}
#endif

#endif

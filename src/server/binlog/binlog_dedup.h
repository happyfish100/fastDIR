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


#ifndef _BINLOG_DEDUP_H
#define _BINLOG_DEDUP_H

#include "fastcommon/sched_thread.h"
#include "../server_global.h"
#include "binlog_types.h"

#define FDIR_DEDUP_SUBDIR_NAME    "binlog/dump"

#ifdef __cplusplus
extern "C" {
#endif

    static inline const char *binlog_dedup_get_filename(
            char *filename, const int size)
    {
        return sf_binlog_writer_get_filename(DATA_PATH_STR,
                FDIR_DEDUP_SUBDIR_NAME, 0, filename, size);
    }

    int binlog_dedup_add_schedule();

#ifdef __cplusplus
}
#endif

#endif

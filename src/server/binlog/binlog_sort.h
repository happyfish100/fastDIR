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

//binlog_sort.h

#ifndef _BINLOG_SORT_H_
#define _BINLOG_SORT_H_

#include "sf/sf_func.h"
#include "../server_global.h"
#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline const char *binlog_sort_get_inodes_filename(
            char *full_filename, const int size)
    {
#define INODES_FILENAME_STR  "inodes.dat"
#define INODES_FILENAME_LEN  (sizeof(INODES_FILENAME_STR) - 1)
        char filename[256];
        int name_len;

        name_len = fc_get_full_filename(FDIR_DATA_DUMP_SUBDIR_NAME_STR,
                FDIR_DATA_DUMP_SUBDIR_NAME_LEN, INODES_FILENAME_STR,
                INODES_FILENAME_LEN, filename);
        fc_get_full_filename_ex(DATA_PATH_STR, DATA_PATH_LEN,
                filename, name_len, full_filename, size);
        return full_filename;
    }

    int binlog_sort_generate_inodes_file();

    int binlog_sort_delete_inodes_file();

#ifdef __cplusplus
}
#endif

#endif

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

#define INODE_ADD_MARK_FILENAME_STR  ".inode_add_mark"
#define INODE_ADD_MARK_FILENAME_LEN  (sizeof(INODE_ADD_MARK_FILENAME_STR) - 1)

#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "inode_add_mark.h"

static inline const char *inode_add_mark_get_filename(
        char *filename, const int size)
{
    char filepath[PATH_MAX];

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FDIR_DATA_DUMP_SUBDIR_NAME_STR,
            filepath, sizeof(filepath));
    fc_get_full_filename_ex(filepath, strlen(filepath),
            INODE_ADD_MARK_FILENAME_STR, INODE_ADD_MARK_FILENAME_LEN,
            filename, size);
    return filename;
}

int inode_add_mark_load(InodeAddMarkStatus *status)
{
    int result;
    int64_t file_size;
    char filename[PATH_MAX];
    char buff[16];

    inode_add_mark_get_filename(filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        *status = inode_add_mark_status_none;
        result = errno != 0 ? errno : EPERM;
        return result == ENOENT ? 0 : result;
    }

    file_size = sizeof(buff);
    if ((result=getFileContentEx(filename, buff, 0, &file_size)) != 0) {
        *status = inode_add_mark_status_none;
        return result;
    }

    *status = strtol(buff, NULL, 10);
    return 0;
}

int inode_add_mark_save(const InodeAddMarkStatus status)
{
    char filename[PATH_MAX];
    char buff[16];
    int len;

    inode_add_mark_get_filename(filename, sizeof(filename));
    len = fc_itoa(status, buff);
    return safeWriteToFile(filename, buff, len);
}

int inode_add_mark_delete()
{
    char filename[PATH_MAX];

    inode_add_mark_get_filename(filename, sizeof(filename));
    return fc_delete_file(filename);
}

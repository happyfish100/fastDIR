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


#ifndef _FDIR_INODE_ADD_MARK_H
#define _FDIR_INODE_ADD_MARK_H

#include "../server_types.h"

typedef enum {
    inode_add_mark_status_none   =  0,
    inode_add_mark_status_doing  =  1,
    inode_add_mark_status_done   =  2
} InodeAddMarkStatus;

#ifdef __cplusplus
extern "C" {
#endif

    int inode_add_mark_load(InodeAddMarkStatus *status);

    int inode_add_mark_save(const InodeAddMarkStatus status);

    int inode_add_mark_delete();

#ifdef __cplusplus
}
#endif

#endif

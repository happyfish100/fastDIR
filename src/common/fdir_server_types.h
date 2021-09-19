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

#ifndef _FDIR_SERVER_COMMON_TYPES_H
#define _FDIR_SERVER_COMMON_TYPES_H

#include "fastcommon/common_define.h"

//piece storage field indexes
#define FDIR_PIECE_FIELD_INDEX_BASIC       0
#define FDIR_PIECE_FIELD_INDEX_CHILDREN    1
#define FDIR_PIECE_FIELD_INDEX_XATTR       2
#define FDIR_PIECE_FIELD_COUNT             3

//virtual field index for sort and check
#define FDIR_PIECE_FIELD_INDEX_FOR_REMOVE 10

typedef struct fdir_server_piece_storage {
    int file_id;
    int offset;
    int size;
} FDIRServerPieceStorage;

#endif

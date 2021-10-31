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

//segment_index.h

#ifndef _INODE_SEGMENT_INDEX_H_
#define _INODE_SEGMENT_INDEX_H_

#include "../storage_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int inode_segment_index_init();

int inode_segment_index_pre_add(const int64_t inode);

int inode_segment_index_real_add(const DAPieceFieldInfo *field,
        FDIRInodeUpdateResult *r);

int inode_segment_index_delete(FDIRStorageInodeIndexInfo *inode,
        FDIRInodeUpdateResult *r);

int inode_segment_index_update(const DAPieceFieldInfo *field,
        const bool normal_update, FDIRInodeUpdateResult *r);

int inode_segment_index_get(FDIRStorageInodeIndexInfo *inode);
int inode_segment_index_shrink(FDIRInodeSegmentIndexInfo *segment);

int inode_segment_index_eliminate(const int min_elements);

#ifdef __cplusplus
}
#endif

#endif

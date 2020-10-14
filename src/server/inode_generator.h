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

//inode_generator.h

#ifndef _INODE_GENERATOR_H_
#define _INODE_GENERATOR_H_

#include "server_global.h"

#define INODE_SN_MAX_QPS   (1000 * 1000)

#ifdef __cplusplus
extern "C" {
#endif

int inode_generator_init();
void inode_generator_destroy();

//skip avoid conflict
static inline void inode_generator_skip()
{
    __sync_add_and_fetch(&CURRENT_INODE_SN, INODE_SN_MAX_QPS);
}

static inline int64_t inode_generator_next()
{
    return INODE_CLUSTER_PART | __sync_add_and_fetch(&CURRENT_INODE_SN, 1);
}

#ifdef __cplusplus
}
#endif

#endif

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


#ifndef _FDIR_DENTRY_LRU_H
#define _FDIR_DENTRY_LRU_H

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define dentry_lru_add(head, dentry) \
    fc_list_add_tail(&(dentry)->db_args->lru_dlink, head)

#define dentry_lru_del(dentry) \
    fc_list_del_init(&(dentry)->db_args->lru_dlink)

#define dentry_lru_move_tail(head, dentry) \
    fc_list_move_tail(&(dentry)->db_args->lru_dlink, head)

    int dentry_lru_eliminate(struct fc_list_head *head,
            const int target_count);

#ifdef __cplusplus
}
#endif

#endif

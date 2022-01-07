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

#define MEMORY_LIMIT_DENTRY_RATIO  0.75
#define MEMORY_LIMIT_INODE_RATIO  (1.00 - MEMORY_LIMIT_DENTRY_RATIO)

struct fdir_data_thread_context;

#ifdef __cplusplus
extern "C" {
#endif

    int dentry_lru_init();

#define dentry_lru_init_dlink(dentry) \
    FC_INIT_LIST_HEAD(&(dentry)->db_args->lru_dlink)

#define dentry_lru_add(dentry) \
    if ((dentry)->parent != NULL) { \
        (dentry)->parent->db_args->loaded_count++;   \
    } \
    fc_list_add_tail(&(dentry)->db_args->lru_dlink,  \
            &(dentry)->context->thread_ctx->lru_ctx.head)

#define dentry_lru_del(dentry) \
    if ((dentry)->parent != NULL) { \
        (dentry)->parent->db_args->loaded_count--;   \
    } \
    fc_list_del_init(&(dentry)->db_args->lru_dlink)

#define dentry_lru_move_tail(dentry) \
    fc_list_move_tail(&(dentry)->db_args->lru_dlink, \
            &(dentry)->context->thread_ctx->lru_ctx.head)

    void dentry_lru_eliminate(struct fdir_data_thread_context *thread_ctx,
            const int64_t target_reclaims);

#ifdef __cplusplus
}
#endif

#endif

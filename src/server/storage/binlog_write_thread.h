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


#ifndef _FDIR_BINLOG_WRITE_THREAD_H
#define _FDIR_BINLOG_WRITE_THREAD_H

#include "storage_global.h"

#ifdef __cplusplus
extern "C" {
#endif

    int binlog_write_thread_init();
    void binlog_write_thread_destroy();

    int binlog_write_thread_push(const DAPieceFieldInfo *field,
            struct fc_queue_info *space_chain);

    static inline void binlog_write_thread_push_queue(
            struct fc_queue_info *qinfo)
    {
        fc_queue_push_queue_to_tail(&BINLOG_WRITE_THREAD_CTX.queue, qinfo);
    }

#ifdef __cplusplus
}
#endif

#endif

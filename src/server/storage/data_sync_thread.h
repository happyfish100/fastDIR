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

#ifndef _DATA_SYNC_THREAD_H
#define _DATA_SYNC_THREAD_H

#include "storage_global.h"

#ifdef __cplusplus
extern "C" {
#endif

    int data_sync_thread_init();
    int data_sync_thread_start();

    static inline void data_sync_thread_push(FDIRDBUpdateDentry *dentry)
    {
        FDIRDataSyncThreadInfo *thread;

        thread = DATA_SYNC_THREAD_ARRAY.threads + (uint64_t)
            dentry->inode % DATA_SYNC_THREAD_ARRAY.count;
        fc_queue_push(&thread->queue, dentry);
    }

#ifdef __cplusplus
}
#endif

#endif

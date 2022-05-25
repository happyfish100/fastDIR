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


#ifndef _FDIR_FLOCK_H
#define _FDIR_FLOCK_H

#include "fastcommon/fc_list.h"
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#define FDIR_FLOCK_TASK_NOT_IN_QUEUE             0
#define FDIR_FLOCK_TASK_IN_LOCKED_QUEUE          1
#define FDIR_FLOCK_TASK_IN_REGION_WAITING_QUEUE  2
#define FDIR_FLOCK_TASK_IN_GLOBAL_WAITING_QUEUE  3

#define FDIR_SYS_TASK_STATUS_NONE      0
#define FDIR_SYS_TASK_STATUS_LOCKED    1
#define FDIR_SYS_TASK_STATUS_WAITING   2

#define FDIR_FTASK_CHANGE_EVENT_INSERT  'I'
#define FDIR_FTASK_CHANGE_EVENT_REMOVE  'R'

typedef void (*sys_lock_release_callback)(FDIRServerDentry *dentry, void *args);

typedef struct fdir_flock_region {
    int64_t offset;   /* starting offset */
    int64_t length;   /* 0 means until end of file */

    struct {
        int reads;
        int writes;
        struct fc_list_head head;  //element: FDIRFLockTask
    } locked;
    struct fc_list_head waiting;  //element: FDIRFLockTask for local

    int ref_count;
    struct fc_list_head dlink;
} FDIRFLockRegion;

typedef struct fdir_flock_entry {
    struct fc_list_head regions; //FDIRFLockRegion order by offset and length
    struct fc_list_head waiting_tasks;  //element: FDIRFLockTask for global
    struct {
        FDIRSysLockTask *locked_task;
        struct fc_list_head waiting;  //element: FDIRSysLockTask
    } sys_lock;  //system lock for file append and ftruncate
} FLockEntry;

typedef struct fdir_flock_context {
    struct {
        struct fast_mblock_man entry;
        struct fast_mblock_man region;
        struct fast_mblock_man ftask;
        struct fast_mblock_man sys_task;
    } allocators;
} FLockContext;

#ifdef __cplusplus
extern "C" {
#endif

    int flock_init(FLockContext *ctx);
    void flock_destroy(FLockContext *ctx);

    static inline FLockEntry *flock_alloc_entry(FLockContext *ctx)
    {
        return (FLockEntry *)fast_mblock_alloc_object(&ctx->allocators.entry);
    }

    static inline void flock_free_entry(FLockContext *ctx, FLockEntry *entry)
    {
        fast_mblock_free_object(&ctx->allocators.entry, entry);
    }

    FDIRFLockTask *flock_alloc_ftask(FLockContext *ctx,
            FDIRServerDentry *dentry);

    static inline void flock_hold_ftask(FDIRFLockTask *ftask)
    {
        FC_ATOMIC_INC(ftask->reffer_count);
    }

    void flock_release_ftask(FDIRFLockTask *ftask);

    int flock_apply(FLockContext *ctx, const int64_t offset,
            const int64_t length, FDIRFLockTask *ftask, const bool block);

    int flock_unlock(FLockContext *ctx, FDIRServerDentry *dentry,
            const FDIRFlockParams *params);

    void flock_release(FLockContext *ctx, FLockEntry *entry, FDIRFLockTask *ftask);

    int flock_get_conflict_lock(FLockContext *ctx, FDIRFLockTask *ftask);

    static inline FDIRSysLockTask *flock_alloc_sys_task(FLockContext *ctx)
    {
        return (FDIRSysLockTask *)fast_mblock_alloc_object(
                &ctx->allocators.sys_task);
    }

    static inline void flock_free_sys_task(FLockContext *ctx,
            FDIRSysLockTask *sys_task)
    {
        fast_mblock_free_object(&ctx->allocators.sys_task, sys_task);
    }

    int sys_lock_apply(FLockEntry *entry, FDIRSysLockTask *sys_task,
            const bool block);
    
    int sys_lock_release(FLockEntry *entry, FDIRSysLockTask *sys_task);

#ifdef __cplusplus
}
#endif

#endif

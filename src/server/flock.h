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

typedef void (*sys_lock_release_callback)(FDIRServerDentry *dentry, void *args);

struct flock_region;
typedef struct flock_owner {
    pid_t   pid;
    int64_t id;   //owner id
} FlockOwner;

typedef struct flock_params {
    short type;
    FlockOwner owner;
    int64_t offset;
    int64_t length;
} FlockParams;

typedef struct flock_task {
    /* LOCK_SH for shared read lock, LOCK_EX for exclusive write lock  */
    short type;
    short which_queue;
    FlockOwner owner;
    struct flock_region *region;
    struct fast_task_info *task;
    FDIRServerDentry *dentry;
    struct fc_list_head flink;  //for flock queue
    struct fc_list_head clink;  //for connection double link chain
} FLockTask;

typedef struct sys_lock_task {
    short status;
    struct fast_task_info *task;
    FDIRServerDentry *dentry;
    struct fc_list_head dlink;
} SysLockTask;

typedef struct flock_region {
    int64_t offset;   /* starting offset */
    int64_t length;   /* 0 means until end of file */

    struct {
        int reads;
        int writes;
        struct fc_list_head head;  //element: FLockTask
    } locked;
    struct fc_list_head waiting;  //element: FLockTask for local

    int ref_count;
    struct fc_list_head dlink;
} FLockRegion;

typedef struct flock_entry {
    struct fc_list_head regions; //FLockRegion order by offset and length
    struct fc_list_head waiting_tasks;  //element: FLockTask for global
    struct {
        SysLockTask *locked_task;
        struct fc_list_head waiting;  //element: SysLockTask
    } sys_lock;  //system lock for file append and ftruncate
} FLockEntry;

typedef struct flock_context {
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

    static inline FLockTask *flock_alloc_ftask(FLockContext *ctx)
    {
        return (FLockTask *)fast_mblock_alloc_object(&ctx->allocators.ftask);
    }

    static inline void flock_free_ftask(FLockContext *ctx, FLockTask *ftask)
    {
        fast_mblock_free_object(&ctx->allocators.ftask, ftask);
    }

    int flock_apply(FLockContext *ctx, const int64_t offset,
            const int64_t length, FLockTask *ftask, const bool block);

    void flock_release(FLockContext *ctx, FLockEntry *entry, FLockTask *ftask);

    int flock_get_conflict_lock(FLockContext *ctx, FLockTask *ftask);

    static inline SysLockTask *flock_alloc_sys_task(FLockContext *ctx)
    {
        return (SysLockTask *)fast_mblock_alloc_object(
                &ctx->allocators.sys_task);
    }

    static inline void flock_free_sys_task(FLockContext *ctx,
            SysLockTask *sys_task)
    {
        fast_mblock_free_object(&ctx->allocators.sys_task, sys_task);
    }

    int sys_lock_apply(FLockEntry *entry, SysLockTask *sys_task,
            const bool block);
    
    int sys_lock_release(FLockEntry *entry, SysLockTask *sys_task);

#ifdef __cplusplus
}
#endif

#endif

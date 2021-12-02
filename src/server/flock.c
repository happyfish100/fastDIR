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


#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/hash.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_nio.h"
#include "common/fdir_types.h"
#include "server_global.h"
#include "flock.h"

static int flock_entry_alloc_init_func(void *element, void *args)
{
    FC_INIT_LIST_HEAD(&((FLockEntry *)element)->regions);
    FC_INIT_LIST_HEAD(&((FLockEntry *)element)->waiting_tasks);
    FC_INIT_LIST_HEAD(&((FLockEntry *)element)->sys_lock.waiting);
    return 0;
}

static int flock_task_alloc_init_func(void *element, void *args)
{
    FC_INIT_LIST_HEAD(&((FLockTask *)element)->flink);
    FC_INIT_LIST_HEAD(&((FLockTask *)element)->clink);
    return 0;
}

static int sys_task_alloc_init_func(void *element, void *args)
{
    FC_INIT_LIST_HEAD(&((SysLockTask *)element)->dlink);
    return 0;
}

int flock_init(FLockContext *ctx)
{
    int result;
    if ((result=fast_mblock_init_ex1(&ctx->allocators.entry,
                    "flock_entry", sizeof(FLockEntry), 4096,
                    0, flock_entry_alloc_init_func, NULL, false)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->allocators.region,
                    "flock_region", sizeof(FLockRegion), 4096,
                    0, NULL, NULL, false)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->allocators.ftask,
                    "flock_task", sizeof(FLockTask), 4096,
                    0, flock_task_alloc_init_func, NULL, false)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->allocators.sys_task,
                    "sys_lck_task", sizeof(SysLockTask), 4096,
                    0, sys_task_alloc_init_func, NULL, false)) != 0)
    {
        return result;
    }

    return 0;
}

void flock_destroy(FLockContext *ctx)
{
    fast_mblock_destroy(&ctx->allocators.entry);
    fast_mblock_destroy(&ctx->allocators.region);
}

static FLockRegion *get_region(FLockContext *ctx, FLockEntry *entry,
        const int64_t offset, const int64_t length)
{
    FLockRegion *region;
    FLockRegion *new_region;

    fc_list_for_each_entry(region, &entry->regions, dlink) {
        if (offset == region->offset) {
            if (length == region->length) {
                region->ref_count++;
                return region;
            } else if (length < region->length) {
                break;
            }
        } else if (offset < region->offset) {
            break;
        }
    }

    new_region = (FLockRegion *)fast_mblock_alloc_object(
            &ctx->allocators.region);
    if (new_region == NULL) {
        return NULL;
    }

    new_region->ref_count = 1;
    new_region->offset = offset;
    new_region->length = length;
    new_region->locked.reads = new_region->locked.writes = 0;
    FC_INIT_LIST_HEAD(&new_region->locked.head);
    FC_INIT_LIST_HEAD(&new_region->waiting);
    fc_list_add_before(&new_region->dlink, &region->dlink);

    return new_region;
}

static inline void add_to_locked(FLockTask *ftask)
{
    if (ftask->type == LOCK_SH) {
        ftask->region->locked.reads++;
    } else {
        ftask->region->locked.writes++;
    }
    ftask->which_queue = FDIR_FLOCK_TASK_IN_LOCKED_QUEUE;
    fc_list_add_tail(&ftask->flink, &ftask->region->locked.head);
}

static inline void remove_from_locked(FLockTask *ftask)
{
    if (ftask->type == LOCK_SH) {
        ftask->region->locked.reads--;
    } else {
        ftask->region->locked.writes--;
    }
    ftask->which_queue = FDIR_FLOCK_TASK_NOT_IN_QUEUE;
    fc_list_del_init(&ftask->flink);
}

static inline bool is_region_overlap(FLockRegion *r1, FLockRegion *r2)
{
    if (r1->offset < r2->offset) {
        return (r1->length == 0) || (r1->offset + r1->length > r2->offset);
    } else if (r1->offset == r2->offset) {
        return true;
    } else {
        return (r2->length == 0) || (r2->offset + r2->length > r1->offset);
    }
}

static inline FLockTask *get_conflict_ftask_by_region(FLockEntry *entry,
        FLockTask *ftask, const bool check_waiting, int *conflict_regions)
{
    FLockRegion *region;
    FLockTask *wait;
    FLockTask *found;

    found = NULL;
    *conflict_regions = 0;
    fc_list_for_each_entry(region, &entry->regions, dlink) {
        if (is_region_overlap(ftask->region, region)) {
            if (check_waiting && (wait=fc_list_first_entry(&region->waiting,
                            FLockTask, flink)) != NULL)
            {
                (*conflict_regions)++;
                if (found == NULL) {
                    found = wait;
                }
            } else if ((region->locked.writes > 0) || (ftask->type == LOCK_EX &&
                        region->locked.reads > 0))
            {
                (*conflict_regions)++;
                if (found == NULL) {
                    found = fc_list_first_entry(&region->locked.head,
                            FLockTask, flink);
                }
            }
        } else if ((ftask->region->length > 0) && (ftask->region->offset +
                    ftask->region->length < region->offset))
        {
            break;
        }
    }

    return found;
}

static inline FLockTask *get_conflict_flock_task(FLockTask *ftask,
        bool *global_conflict)
{
    const bool check_waiting = true;
    FLockTask *found;
    FLockTask *wait;
    int conflict_regions;

    if ((found=get_conflict_ftask_by_region(ftask->dentry->flock_entry,
                    ftask, check_waiting, &conflict_regions)) == NULL)
    {
        if (ftask->type == LOCK_EX) {
            *global_conflict = false;
            return NULL;
        }
    } else if (conflict_regions > 1) {
        *global_conflict = true;
        return found;
    }

    fc_list_for_each_entry(wait, &ftask->dentry->
            flock_entry->waiting_tasks, flink)
    {
        if (is_region_overlap(ftask->region, wait->region)) {
            *global_conflict = true;
            return wait;
        }
    }

    if (found == NULL) {
        *global_conflict = false;
    } else {
        *global_conflict = found->region != ftask->region;
    }
    return found;
}

int flock_apply(FLockContext *ctx, const int64_t offset,
        const int64_t length, FLockTask *ftask, const bool block)
{
    FLockTask *holder;
    bool global_conflict;

    if ((ftask->region=get_region(ctx, ftask->dentry->flock_entry,
                    offset, length)) == NULL)
    {
        return ENOMEM;
    }

    if ((holder=get_conflict_flock_task(ftask, &global_conflict)) == NULL) {
        add_to_locked(ftask);
        return 0;
    }

    if (!block) {
        return EWOULDBLOCK;
    }

    if (ftask->task == holder->task) {
        return EDEADLK;
    }

    if (global_conflict) {
        ftask->which_queue = FDIR_FLOCK_TASK_IN_GLOBAL_WAITING_QUEUE;
        fc_list_add_tail(&ftask->flink, &ftask->dentry->
                flock_entry->waiting_tasks);
    } else {
        ftask->which_queue = FDIR_FLOCK_TASK_IN_REGION_WAITING_QUEUE;
        fc_list_add_tail(&ftask->flink, &ftask->region->waiting);
    }
    return EINPROGRESS;
}

int flock_get_conflict_lock(FLockContext *ctx, FLockTask *ftask)
{
    bool global_conflict;
    FLockTask *holder;

    if ((holder=get_conflict_flock_task(ftask, &global_conflict)) == NULL) {
        return ENOENT;
    }

    ftask->type = holder->type;
    ftask->owner = holder->owner;
    *(ftask->region) = *(holder->region);
    ftask->task = holder->task;
    return 0;
}

static int awake_waiting_tasks(FLockEntry *entry,
        struct fc_list_head *waiting_head, const bool check_waiting)
{
    FLockTask *wait;
    int conflict_regions;
    int count;

    count = 0;
    while ((wait=fc_list_first_entry(waiting_head,
                    FLockTask, flink)) != NULL)
    {
        if (get_conflict_ftask_by_region(entry, wait, check_waiting,
                    &conflict_regions) != NULL)
        {
            break;
        }

        ++count;
        fc_list_del_init(&wait->flink);
        add_to_locked(wait);

        sf_nio_notify(wait->task, SF_NIO_STAGE_CONTINUE);
    }

    return count;
}

void flock_release(FLockContext *ctx, FLockEntry *entry, FLockTask *ftask)
{
    switch (ftask->which_queue) {
        case FDIR_FLOCK_TASK_IN_LOCKED_QUEUE:
            remove_from_locked(ftask);
            if (!fc_list_empty(&ftask->region->waiting)) {
                awake_waiting_tasks(entry, &ftask->region->waiting, false);
            }
            if (!fc_list_empty(&entry->waiting_tasks)) {
                awake_waiting_tasks(entry, &entry->waiting_tasks, true);
            }
            ftask->region->ref_count--;
            break;
        case FDIR_FLOCK_TASK_IN_REGION_WAITING_QUEUE:
        case FDIR_FLOCK_TASK_IN_GLOBAL_WAITING_QUEUE:
            ftask->which_queue = FDIR_FLOCK_TASK_NOT_IN_QUEUE;
            fc_list_del_init(&ftask->flink);
            ftask->region->ref_count--;
        default:
            break;
    }
}

int sys_lock_apply(FLockEntry *entry, SysLockTask *sys_task,
        const bool block)
{
    if (entry->sys_lock.locked_task == NULL) {
        entry->sys_lock.locked_task = sys_task;
        sys_task->status = FDIR_SYS_TASK_STATUS_LOCKED;
        return 0;
    }

    if (!block) {
        return EWOULDBLOCK;
    }

    if (sys_task->task == entry->sys_lock.locked_task->task) {
        return EDEADLK;
    }

    sys_task->status = FDIR_SYS_TASK_STATUS_WAITING;
    fc_list_add_tail(&sys_task->dlink, &entry->sys_lock.waiting);
    return EINPROGRESS;
}

int sys_lock_release(FLockEntry *entry, SysLockTask *sys_task)
{
    SysLockTask *wait;

    if (sys_task->status == FDIR_SYS_TASK_STATUS_WAITING) {
        sys_task->status = FDIR_SYS_TASK_STATUS_NONE;
        fc_list_del_init(&sys_task->dlink);
        return 0;
    }

    if (sys_task->status != FDIR_SYS_TASK_STATUS_LOCKED) {
        return EINVAL;
    }

    if (sys_task != entry->sys_lock.locked_task) {
        logWarning("file: "__FILE__", line: %d, "
                "sys_task: %p != locked_task: %p", __LINE__,
                sys_task, entry->sys_lock.locked_task);
        return ENOENT;
    }

    if ((wait=fc_list_first_entry(&entry->sys_lock.waiting,
                    SysLockTask, dlink)) != NULL)
    {
        wait->status = FDIR_SYS_TASK_STATUS_LOCKED;
        entry->sys_lock.locked_task = wait;
        fc_list_del_init(&wait->dlink);
        sf_nio_notify(wait->task, SF_NIO_STAGE_CONTINUE);
    } else {
        sys_task->status = FDIR_SYS_TASK_STATUS_NONE;
        entry->sys_lock.locked_task = NULL;
    }

    return 0;
}

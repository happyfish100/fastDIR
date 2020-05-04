
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
#include "common/fdir_types.h"
#include "server_global.h"
#include "flock.h"

int flock_init(FLockContext *ctx)
{
    int result;
    if ((result=fast_mblock_init_ex2(&ctx->allocators.entry,
                    "flock_entry", sizeof(FLockEntry), 4096,
                    NULL, NULL, false, NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex2(&ctx->allocators.region,
                    "flock_region", sizeof(FLockRegion), 4096,
                    NULL, NULL, false, NULL, NULL, NULL)) != 0)
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
        const int64_t offset, const int64_t length, FLockTask *ftask)
{
    FLockRegion *region;
    FLockRegion *new_region;

    fc_list_for_each_entry(region, &entry->regions, dlink) {
        if (offset == region->offset) {
            if (length == region->length) {
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

    new_region->offset = offset;
    new_region->length = length;
    new_region->locked.reads = new_region->locked.writes = 0;
    FC_INIT_LIST_HEAD(&new_region->locked.head);
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
    fc_list_add_tail(&ftask->dlink, &ftask->region->locked.head);
}

static inline void remove_from_locked(FLockTask *ftask)
{
    if (ftask->type == LOCK_SH) {
        ftask->region->locked.reads--;
    } else {
        ftask->region->locked.writes--;
    }
    ftask->which_queue = FDIR_FLOCK_TASK_NOT_IN_QUEUE;
    fc_list_del_init(&ftask->dlink);
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

static inline FLockTask *get_conflict_flock_task(FLockEntry *entry,
        FLockTask *ftask)
{
    FLockRegion *region;
    fc_list_for_each_entry(region, &entry->regions, dlink) {
        if (is_region_overlap(ftask->region, region)) {
            if ((region->locked.writes > 0) || (ftask->type == LOCK_EX &&
                        region->locked.reads > 0))
            {
                return fc_list_first_entry(&region->locked.head,
                        FLockTask, dlink);
            }
        } else if ((ftask->region->length > 0) && (ftask->region->offset +
                    ftask->region->length < region->offset))
        {
            return NULL;
        }
    }

    return NULL;
}

int flock_apply(FLockContext *ctx, FLockEntry *entry, const int64_t offset,
        const int64_t length, FLockTask *ftask)
{
    bool empty;

    empty = fc_list_empty(&entry->regions);
    if ((ftask->region=get_region(ctx, entry, offset,
                    length, ftask)) == NULL)
    {
        return ENOMEM;
    }

    if (empty || get_conflict_flock_task(entry, ftask) == NULL) {
        add_to_locked(ftask);
        return 0;
    }

    ftask->which_queue = FDIR_FLOCK_TASK_IN_WAITING_QUEUE;
    fc_list_add_tail(&ftask->dlink, &entry->waiting_tasks);
    return ENOLCK;
}

static inline void awake_waiting_task(FLockEntry *entry, FLockTask *ftask)
{
#define MAX_WAKED_TASK_COUNT_ONCE 64
    FLockTask *wait;
    struct {
        FLockTask *tasks[MAX_WAKED_TASK_COUNT_ONCE];
        int count;
    } waked;
    int i;

    do {
        waked.count = 0;
        fc_list_for_each_entry(wait, &entry->waiting_tasks, dlink) {
            if (get_conflict_flock_task(entry, wait) != NULL) {
                break;
            }

            waked.tasks[waked.count++] = wait;
            if (waked.count == MAX_WAKED_TASK_COUNT_ONCE) {
                break;
            }
        }

        for (i=0; i<waked.count; i++) {
            fc_list_del_init(&waked.tasks[i]->dlink);
            add_to_locked(waked.tasks[i]);
            //TODO notify task
        }
    } while (waked.count == MAX_WAKED_TASK_COUNT_ONCE);
}

void flock_release(FLockContext *ctx, FLockEntry *entry, FLockTask *ftask)
{
    switch (ftask->which_queue) {
        case FDIR_FLOCK_TASK_IN_LOCKED_QUEUE:
            remove_from_locked(ftask);
            if (!fc_list_empty(&entry->waiting_tasks)) {
                awake_waiting_task(entry, ftask);
            }
            break;
        case FDIR_FLOCK_TASK_IN_WAITING_QUEUE:
            ftask->which_queue = FDIR_FLOCK_TASK_NOT_IN_QUEUE;
            fc_list_del_init(&ftask->dlink);
            break;
        default:
            break;
    }
}

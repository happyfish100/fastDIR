
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

int flock_lock(FLockContext *ctx, FLockEntry *entry, const int64_t offset,
        const int64_t length, FLockTask *ftask)
{
    FLockRegion *region;

    if ((region=get_region(ctx, entry, offset, length, ftask)) == NULL) {
        return ENOMEM;
    }

    //TODO
    return 0;
}

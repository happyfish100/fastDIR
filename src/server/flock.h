
#ifndef _FDIR_FLOCK_H
#define _FDIR_FLOCK_H

#include "fastcommon/fc_list.h"
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

struct flock_region;

typedef struct flock_task {
    /* LOCK_SH for shared read lock, LOCK_EX for exclusive write lock  */
    short  type;
    struct flock_region *region;
    struct fast_task_info *task;
    struct fc_list_head dlink;
} FLockTask;

typedef struct flock_region {
    int64_t offset;   /* starting offset */
    int64_t length;   /* 0 means until end of file */

    struct {
        int reads;
        int writes;
        struct fc_list_head head;  //element: FLockTask
    } locked;

    struct fc_list_head dlink;
} FLockRegion;

typedef struct flock_entry {
    struct fc_list_head regions;        //element: FLockRegion
    struct fc_list_head waiting_tasks;  //element: FLockTask
} FLockEntry;

typedef struct flock_context {
    struct {
        struct fast_mblock_man entry;
        struct fast_mblock_man region;
    } allocators;
} FLockContext;

#ifdef __cplusplus
extern "C" {
#endif

    int flock_init(FLockContext *ctx);
    void flock_destroy(FLockContext *ctx);

    static inline FLockEntry *flock_alloc_init_entry(FLockContext *ctx)
    {
        FLockEntry *entry;
        entry = (FLockEntry *)fast_mblock_alloc_object(&ctx->allocators.entry);
        if (entry != NULL) {
            FC_INIT_LIST_HEAD(&entry->regions);
        }
        return entry;
    }

    static inline void flock_free_entry(FLockContext *ctx, FLockEntry *entry)
    {
        fast_mblock_free_object(&ctx->allocators.entry, entry);
    }

    int flock_lock(FLockContext *ctx, FLockEntry *entry, const int64_t offset,
            const int64_t length, FLockTask *ftask);

    int flock_unlock(FLockContext *ctx, FLockEntry *entry, FLockTask *ftask);

#ifdef __cplusplus
}
#endif

#endif

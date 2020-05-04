
#ifndef _FDIR_FLOCK_H
#define _FDIR_FLOCK_H

#include "fastcommon/fc_list.h"
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#define FDIR_FLOCK_TASK_NOT_IN_QUEUE             0
#define FDIR_FLOCK_TASK_IN_LOCKED_QUEUE          1
#define FDIR_FLOCK_TASK_IN_REGION_WAITING_QUEUE  2
#define FDIR_FLOCK_TASK_IN_GLOBAL_WAITING_QUEUE  3

struct flock_region;

typedef struct flock_task {
    /* LOCK_SH for shared read lock, LOCK_EX for exclusive write lock  */
    short type;
    short which_queue;
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
    struct fc_list_head waiting;  //element: FLockTask for local

    int ref_count;
    struct fc_list_head dlink;
} FLockRegion;

typedef struct flock_entry {
    struct fc_list_head regions; //FLockRegion order by offset and length
    struct fc_list_head waiting_tasks;  //element: FLockTask for global
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
            FC_INIT_LIST_HEAD(&entry->waiting_tasks);
        }
        return entry;
    }

    static inline void flock_free_entry(FLockContext *ctx, FLockEntry *entry)
    {
        fast_mblock_free_object(&ctx->allocators.entry, entry);
    }

    int flock_apply(FLockContext *ctx, FLockEntry *entry, const int64_t offset,
            const int64_t length, FLockTask *ftask);

    void flock_release(FLockContext *ctx, FLockEntry *entry, FLockTask *ftask);

#ifdef __cplusplus
}
#endif

#endif

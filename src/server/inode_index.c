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

#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "server_global.h"
#include "ns_manager.h"
#include "dentry.h"
#include "db/dentry_loader.h"
#include "db/dentry_lru.h"
#include "inode_index.h"

typedef struct {
    pthread_mutex_t lock;
    FLockContext flock_ctx;
} InodeSharedContext;

typedef struct {
    int count;
    InodeSharedContext *contexts;
} InodeSharedContextArray;

typedef struct {
    int64_t count;
    int64_t capacity;
    FDIRServerDentry **buckets;
} InodeHashtable;

static InodeSharedContextArray inode_shared_ctx_array = {0, NULL};
static InodeHashtable inode_hashtable = {0, 0, NULL};

static int init_inode_shared_ctx_array()
{
    int result;
    int bytes;
    InodeSharedContext *ctx;
    InodeSharedContext *end;

    inode_shared_ctx_array.count = INODE_SHARED_LOCKS_COUNT;
    bytes = sizeof(InodeSharedContext) * inode_shared_ctx_array.count;
    inode_shared_ctx_array.contexts = (InodeSharedContext *)fc_malloc(bytes);
    if (inode_shared_ctx_array.contexts == NULL) {
        return ENOMEM;
    }

    end = inode_shared_ctx_array.contexts + inode_shared_ctx_array.count;
    for (ctx=inode_shared_ctx_array.contexts; ctx<end; ctx++) {
        if ((result=init_pthread_lock(&ctx->lock)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "init_pthread_lock fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }

        if ((result=flock_init(&ctx->flock_ctx)) != 0) {
            return result;
        }
    }

    return 0;
}

static int init_inode_hashtable()
{
    int64_t bytes;

    inode_hashtable.capacity = INODE_HASHTABLE_CAPACITY;
    bytes = sizeof(FDIRServerDentry *) * inode_hashtable.capacity;
    inode_hashtable.buckets = (FDIRServerDentry **)fc_malloc(bytes);
    if (inode_hashtable.buckets == NULL) {
        return ENOMEM;
    }
    memset(inode_hashtable.buckets, 0, bytes);

    return 0;
}

int inode_index_init()
{
    int result;

    if ((result=init_inode_shared_ctx_array()) != 0) {
        return result;
    }

    if ((result=init_inode_hashtable()) != 0) {
        return result;
    }

    return 0;
}

void inode_index_destroy()
{
}

static FDIRServerDentry *find_dentry_for_update(FDIRServerDentry **bucket,
        const FDIRServerDentry *dentry, FDIRServerDentry **previous)
{
    int64_t cmpr;

    if (*bucket == NULL) {
        *previous = NULL;
        return NULL;
    }

    cmpr = dentry->inode - (*bucket)->inode;
    if (cmpr == 0) {
        *previous = NULL;
        return *bucket;
    } else if (cmpr < 0) {
        *previous = NULL;
        return NULL;
    }

    *previous = *bucket;
    while ((*previous)->ht_next != NULL) {
        cmpr = dentry->inode - (*previous)->ht_next->inode;
        if (cmpr == 0) {
            return (*previous)->ht_next;
        } else if (cmpr < 0) {
            break;
        }

        *previous = (*previous)->ht_next;
    }

    return NULL;
}

static FDIRServerDentry *find_inode_entry(FDIRServerDentry
        **bucket, const int64_t inode)
{
    int64_t cmpr;
    FDIRServerDentry *dentry;

    if (*bucket == NULL) {
        return NULL;
    }

    dentry = *bucket;
    while (dentry != NULL) {
        cmpr = inode - dentry->inode;
        if (cmpr == 0) {
            return dentry;
        } else if (cmpr < 0) {
            break;
        }

        dentry = dentry->ht_next;
    }

    return NULL;
}

#define SET_INODE_HASHTABLE_CTX(inode)  \
    uint64_t bucket_index;       \
    InodeSharedContext *ctx;    \
    do {  \
        bucket_index = ((uint64_t)inode) % inode_hashtable.capacity;  \
        ctx = inode_shared_ctx_array.contexts + bucket_index %    \
            inode_shared_ctx_array.count;   \
    } while (0)


#define SET_INODE_HT_BUCKET_AND_CTX(inode)  \
    FDIRServerDentry **bucket;  \
    SET_INODE_HASHTABLE_CTX(inode);  \
    do {  \
        bucket = inode_hashtable.buckets + bucket_index;   \
    } while (0)


int inode_index_add_dentry(FDIRServerDentry *dentry)
{
    int result;
    FDIRServerDentry *previous;

    SET_INODE_HT_BUCKET_AND_CTX(dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if (find_dentry_for_update(bucket, dentry, &previous) == NULL) {
        if (previous == NULL) {
            dentry->ht_next = *bucket;
            *bucket = dentry;
        } else {
            dentry->ht_next = previous->ht_next;
            previous->ht_next = dentry;
        }
        result = 0;
    } else {
        result = EEXIST;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return result;
}

int inode_index_del_dentry(FDIRServerDentry *dentry)
{
    int result;
    FDIRServerDentry *previous;
    FDIRServerDentry *deleted;

    SET_INODE_HT_BUCKET_AND_CTX(dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if ((deleted=find_dentry_for_update(bucket, dentry, &previous)) != NULL) {
        if (previous == NULL) {
            *bucket = (*bucket)->ht_next;
        } else {
            previous->ht_next = deleted->ht_next;
        }
        result = 0;
    } else {
        result = ENOENT;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    if (deleted != NULL && deleted->stat.alloc > 0) {
        fdir_namespace_inc_alloc_bytes(deleted->ns_entry,
                -1 * deleted->stat.alloc);
    }

    return result;
}

FDIRServerDentry *inode_index_find_dentry(const int64_t inode)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    dentry = find_inode_entry(bucket, inode);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    if (STORAGE_ENABLED && dentry != NULL) {
        dentry_lru_move_tail(dentry);
    }
    return dentry;
}

int inode_index_get_dentry(FDIRDataThreadContext *thread_ctx,
        const int64_t inode, FDIRServerDentry **dentry)
{
    if ((*dentry=inode_index_find_dentry(inode)) != NULL) {
        return 0;
    }

    if (STORAGE_ENABLED) {
        return dentry_load_inode(thread_ctx, NULL, inode, dentry);
    } else {
        return ENOENT;
    }
}

int inode_index_get_dentry_by_pname(FDIRDataThreadContext *thread_ctx,
        const int64_t parent_inode, const string_t *name,
        FDIRServerDentry **dentry)
{
    int result;
    FDIRServerDentry *parent_dentry;

    if ((result=inode_index_get_dentry(thread_ctx,
                    parent_inode, &parent_dentry)) != 0)
    {
        return result;
    }

    return dentry_find_by_pname(parent_dentry, name, dentry);
}

int inode_index_check_set_dentry_size(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    int flags;
    bool force;

    if ((result=inode_index_get_dentry(thread_ctx, record->inode,
                    &record->me.dentry)) != 0)
    {
        return result;
    }

    flags = record->options.flags;
    force = ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_FORCE) != 0);
    record->options.flags = 0;
    if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE)) {
        if (force || (record->me.dentry->stat.size < record->stat.size)) {
            if (record->me.dentry->stat.size != record->stat.size) {
                record->me.dentry->stat.size = record->stat.size;
                record->options.size = 1;
            }
        }

        flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME;
    }

    if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME)) {
        if (record->me.dentry->stat.mtime != g_current_time) {
            record->me.dentry->stat.mtime = g_current_time;
            record->stat.mtime = record->me.dentry->stat.mtime;
            record->options.mtime = 1;
        }
    }

    if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_SPACE_END)) {
        if (force || (record->me.dentry->stat.space_end < record->stat.size)) {
            if (record->me.dentry->stat.space_end != record->stat.size) {
                record->me.dentry->stat.space_end = record->stat.size;
                record->stat.space_end = record->me.dentry->stat.space_end;
                record->options.space_end = 1;
            }
        }
    }

    if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC)) {
        dentry_set_inc_alloc_bytes(record->me.dentry, record->stat.alloc);
        record->options.inc_alloc = 1;
    }

    /*
    logInfo("old size: %"PRId64", new size: %"PRId64", "
            "old mtime: %d, new mtime: %d, force: %d, flags: %u, "
            "modified_flags: %"PRId64, record->me.dentry->stat.size,
            record->stat.size, record->me.dentry->stat.mtime,
            (int)g_current_time, force, flags, record->options.flags);
     */

    return 0;
}

static int get_xattr(FDIRServerDentry *dentry, const string_t *name,
        key_value_pair_t **kv)
{
    int result;
    key_value_pair_t *end;

    if (STORAGE_ENABLED) {
        if ((result=dentry_load_xattr(dentry->context->
                        thread_ctx, dentry)) != 0)
        {
            *kv = NULL;
            return result;
        }
    }

    if (dentry->kv_array != NULL) {
        end = dentry->kv_array->elts + dentry->kv_array->count;
        for (*kv=dentry->kv_array->elts; *kv<end; (*kv)++) {
            if (fc_string_equal(name, &(*kv)->key)) {
                return 0;
            }
        }
    }

    *kv = NULL;
    return ENODATA;
}

int inode_index_remove_xattr(FDIRServerDentry *dentry, const string_t *name)
{
    int result;
    key_value_pair_t *kv;
    key_value_pair_t *end;

    if ((result=get_xattr(dentry, name, &kv)) != 0) {
        return result;
    }

    dentry_strfree(dentry->context, &kv->key);
    dentry_strfree(dentry->context, &kv->value);

    end = dentry->kv_array->elts + dentry->kv_array->count;
    for (kv=kv+1; kv<end; kv++) {
        *(kv - 1) = *kv;
    }
    dentry->kv_array->count--;

    return 0;
}

static key_value_pair_t *check_alloc_kvpair(FDIRDentryContext *context,
        FDIRServerDentry *dentry, int *err_no)
{
    struct fast_mblock_man *allocator;
    SFKeyValueArray *new_array;

    if (dentry->kv_array == NULL) {
        allocator = context->kvarray_allocators + 0;
    } else if (dentry->kv_array->count == dentry->kv_array->alloc) {
        if ((allocator=dentry_get_kvarray_allocator_by_capacity(context,
                        dentry->kv_array->alloc * 2)) == NULL)
        {
            *err_no = EOVERFLOW;
            return NULL;
        }
    } else {
        allocator = NULL;
    }

    if (allocator != NULL) {
        new_array = (SFKeyValueArray *)fast_mblock_alloc_object(allocator);
        if (new_array == NULL) {
            *err_no = ENOMEM;
            return NULL;
        }

        if (dentry->kv_array == NULL) {
            new_array->count = 0;
        } else {
            memcpy(new_array->elts, dentry->kv_array->elts,
                    sizeof(key_value_pair_t) * dentry->kv_array->count);
            new_array->count = dentry->kv_array->count;
            fast_mblock_free_object(allocator - 1, dentry->kv_array);
        }

        dentry->kv_array = new_array;
    }

    *err_no = 0;
    return dentry->kv_array->elts + dentry->kv_array->count;
}

int inode_index_set_xattr(FDIRServerDentry *dentry,
        const FDIRBinlogRecord *record)
{
    int result;
    bool new_create;
    key_value_pair_t *kv;
    string_t value;

    if ((result=get_xattr(dentry, &record->xattr.key, &kv)) == 0) {
        if ((record->flags & FDIR_FLAGS_XATTR_CREATE)) {
            return EEXIST;
        }
        new_create = false;
    } else if (result != ENODATA) {
        return result;
    } else {
        if ((record->flags & FDIR_FLAGS_XATTR_REPLACE)) {
            return result;
        }

        if ((kv=check_alloc_kvpair(dentry->context,
                        dentry, &result)) == NULL)
        {
            return result;
        }
        if ((result=dentry_strdup(dentry->context, &kv->key,
                        &record->xattr.key)) != 0)
        {
            return result;
        }

        new_create = true;
    }

    if ((result=dentry_strdup(dentry->context, &value,
                    &record->xattr.value)) != 0)
    {
        if (new_create) {
            dentry_strfree(dentry->context, &kv->key);
        }
        return result;
    }

    if (new_create) {
        dentry->kv_array->count++;
    } else {
        dentry_strfree(dentry->context, &kv->value);
    }
    kv->value = value;

    return 0;
}

int inode_index_get_xattr(FDIRServerDentry *dentry,
        const string_t *name, string_t *value)
{
    int result;
    key_value_pair_t *kv;

    if ((result=get_xattr(dentry, name, &kv)) == 0) {
        *value = kv->value;
    }

    return result;
}

FLockTask *inode_index_flock_apply(FDIRDataThreadContext *thread_ctx,
        const int64_t inode, const FlockParams *params, const bool block,
        struct fast_task_info *task, int *result)
{
    FDIRServerDentry *dentry;
    FLockTask *ftask;

    if ((*result=inode_index_get_dentry(thread_ctx, inode, &dentry)) != 0) {
        return NULL;
    }

    {
        SET_INODE_HASHTABLE_CTX(inode);
        PTHREAD_MUTEX_LOCK(&ctx->lock);
        do {
            if (dentry->flock_entry == NULL) {
                dentry->flock_entry = flock_alloc_entry(&ctx->flock_ctx);
                if (dentry->flock_entry == NULL) {
                    *result = ENOMEM;
                    ftask = NULL;
                    break;
                }
            }

            if ((ftask=flock_alloc_ftask(&ctx->flock_ctx)) == NULL) {
                *result = ENOMEM;
                ftask = NULL;
                break;
            }

            ftask->type = params->type;
            ftask->owner = params->owner;
            ftask->dentry = dentry;
            ftask->task = task;
            *result = flock_apply(&ctx->flock_ctx, params->offset,
                    params->length, ftask, block);
            if (*result == 0 || *result == EINPROGRESS) {
                dentry_hold(dentry);
            } else {
                flock_free_ftask(&ctx->flock_ctx, ftask);
                ftask = NULL;
            }
        } while (0);
        PTHREAD_MUTEX_UNLOCK(&ctx->lock);
    }

    return ftask;
}

int inode_index_flock_getlk(const int64_t inode, FLockTask *ftask)
{
    int result;

    if ((ftask->dentry=inode_index_find_dentry(inode)) == NULL) {
        return ENOENT;
    }

    {
        SET_INODE_HASHTABLE_CTX(inode);
        PTHREAD_MUTEX_LOCK(&ctx->lock);
        if (ftask->dentry->flock_entry == NULL) {
            result = ENOENT;
        } else {
            result = flock_get_conflict_lock(&ctx->flock_ctx, ftask);
        }
        PTHREAD_MUTEX_UNLOCK(&ctx->lock);
    }

    return result;
}

void inode_index_flock_release(FLockTask *ftask)
{
    SET_INODE_HASHTABLE_CTX(ftask->dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if (ftask->dentry->flock_entry != NULL) {
        flock_release(&ctx->flock_ctx, ftask->dentry->flock_entry, ftask);
    }
    dentry_release(ftask->dentry);
    flock_free_ftask(&ctx->flock_ctx, ftask);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);
}

SysLockTask *inode_index_sys_lock_apply(FDIRDataThreadContext *thread_ctx,
        const int64_t inode, const bool block,
        struct fast_task_info *task, int *result)
{
    FDIRServerDentry *dentry;
    SysLockTask  *sys_task;

    if ((*result=inode_index_get_dentry(thread_ctx, inode, &dentry)) != 0) {
        return NULL;
    }

    {
        SET_INODE_HASHTABLE_CTX(inode);
        PTHREAD_MUTEX_LOCK(&ctx->lock);
        do {
            if (dentry->flock_entry == NULL) {
                dentry->flock_entry = flock_alloc_entry(&ctx->flock_ctx);
                if (dentry->flock_entry == NULL) {
                    *result = ENOMEM;
                    sys_task = NULL;
                    break;
                }
            }

            if ((sys_task=flock_alloc_sys_task(&ctx->flock_ctx)) == NULL) {
                *result = ENOMEM;
                break;
            }

            sys_task->dentry = dentry;
            sys_task->task = task;
            *result = sys_lock_apply(dentry->flock_entry, sys_task, block);
            if (*result == 0 || *result == EINPROGRESS) {
                dentry_hold(dentry);
            } else {
                flock_free_sys_task(&ctx->flock_ctx, sys_task);
                sys_task = NULL;
            }
        } while (0);
        PTHREAD_MUTEX_UNLOCK(&ctx->lock);
    }

    return sys_task;
}

int inode_index_sys_lock_release(SysLockTask *sys_task)
{
    int result;
    SET_INODE_HASHTABLE_CTX(sys_task->dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if (sys_task->dentry->flock_entry != NULL) {
        result = sys_lock_release(sys_task->dentry->flock_entry, sys_task);
    } else {
        result = ENOENT;
    }
    dentry_release(sys_task->dentry);
    flock_free_sys_task(&ctx->flock_ctx, sys_task);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return result;
}

void inode_index_free_flock_entry(FDIRServerDentry *dentry)
{
    SET_INODE_HASHTABLE_CTX(dentry->inode);

    PTHREAD_MUTEX_LOCK(&ctx->lock);
    flock_free_entry(&ctx->flock_ctx, dentry->flock_entry);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);
}

int inode_index_xattrs_copy(const key_value_array_t *kv_array,
        FDIRServerDentry *dentry)
{
    int result;
    const key_value_pair_t *src;
    const key_value_pair_t *end;
    key_value_pair_t *dest;

    end = kv_array->kv_pairs + kv_array->count;
    for (src=kv_array->kv_pairs; src<end; src++) {
        if ((dest=check_alloc_kvpair(dentry->context,
                        dentry, &result)) == NULL)
        {
            return result;
        }
        if ((result=dentry_strdup(dentry->context,
                        &dest->key, &src->key)) != 0)
        {
            return result;
        }

        if ((result=dentry_strdup(dentry->context,
                        &dest->value, &src->value)) != 0)
        {
            return result;
        }
        dentry->kv_array->count++;
    }

    return 0;
}

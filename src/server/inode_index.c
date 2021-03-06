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
#include <sys/xattr.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "server_global.h"
#include "dentry.h"
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

static FDIRServerDentry *find_inode_entry(FDIRServerDentry **bucket,
        const int64_t inode)
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

    return result;
}

FDIRServerDentry *inode_index_get_dentry(const int64_t inode)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    dentry = find_inode_entry(bucket, inode);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return dentry;
}

FDIRServerDentry *inode_index_get_dentry_by_pname(
        const int64_t parent_inode, const string_t *name)
{
    FDIRServerDentry *parent_dentry;
    FDIRServerDentry *dentry;

    if ((parent_dentry=inode_index_get_dentry(parent_inode)) == NULL) {
        return NULL;
    }

    dentry_find_by_pname(parent_dentry, name, &dentry);
    return dentry;
}

FDIRServerDentry *inode_index_check_set_dentry_size(
        const FDIRSetDEntrySizeInfo *dsize,
        const bool need_lock, int *modified_flags)
{
    FDIRServerDentry *dentry;
    int flags;

    SET_INODE_HT_BUCKET_AND_CTX(dsize->inode);
    flags = dsize->flags;
    *modified_flags = 0;
    if (need_lock) {
        PTHREAD_MUTEX_LOCK(&ctx->lock);
    }
    dentry = find_inode_entry(bucket, dsize->inode);
    if (dentry != NULL) {
        if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE)) {
            if (dsize->force || (dentry->stat.size < dsize->file_size)) {
                if (dentry->stat.size != dsize->file_size) {
                    dentry->stat.size = dsize->file_size;
                    *modified_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_FILE_SIZE;
                }
            }

            flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME;
        }

        if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME)) {
            if (dentry->stat.mtime != g_current_time) {
                dentry->stat.mtime = g_current_time;
                *modified_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME;
            }
        }

        if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_SPACE_END)) {
            if (dsize->force || (dentry->stat.space_end < dsize->file_size)) {
                if (dentry->stat.space_end != dsize->file_size) {
                    dentry->stat.space_end = dsize->file_size;
                    *modified_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_SPACE_END;
                }
            }
        }

        if ((flags & FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC)) {
            dentry->stat.alloc += dsize->inc_alloc;
            *modified_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_INC_ALLOC;
        }

        /*
        logInfo("old size: %"PRId64", new size: %"PRId64", "
                "old mtime: %d, new mtime: %d, modified_flags: %d",
                dentry->stat.size, dsize->file_size, dentry->stat.mtime,
                (int)g_current_time, *modified_flags);
         */
    }
    if (need_lock) {
        PTHREAD_MUTEX_UNLOCK(&ctx->lock);
    }

    return dentry;
}

static void update_dentry(FDIRServerDentry *dentry,
        const FDIRBinlogRecord *record)
{
    if (record->options.mode) {
        dentry->stat.mode = record->stat.mode;
    }
    if (record->options.atime) {
        dentry->stat.atime = record->stat.atime;
    }
    if (record->options.ctime) {
        dentry->stat.ctime = record->stat.ctime;
    }
    if (record->options.mtime) {
        dentry->stat.mtime = record->stat.mtime;
    }
    if (record->options.uid) {
        dentry->stat.uid = record->stat.uid;
    }
    if (record->options.gid) {
        dentry->stat.gid = record->stat.gid;
    }
    if (record->options.size) {
        dentry->stat.size = record->stat.size;
    }
    if (record->options.space_end) {
        dentry->stat.space_end = record->stat.space_end;
    }
    if (record->options.inc_alloc) {
        dentry->stat.alloc += record->stat.alloc;
    }
}

FDIRServerDentry *inode_index_update_dentry(
        const FDIRBinlogRecord *record)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(record->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    dentry = find_inode_entry(bucket, record->inode);
    if (dentry != NULL) {
        update_dentry(dentry, record);
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return dentry;
}

static key_value_pair_t *get_xattr(FDIRServerDentry *dentry,
        const string_t *name)
{
    key_value_pair_t *kv;
    key_value_pair_t *end;

    if (dentry->kv_array == NULL) {
        return NULL;
    }

    end = dentry->kv_array->elts + dentry->kv_array->count;
    for (kv=dentry->kv_array->elts; kv<end; kv++) {
        if (fc_string_equal(name, &kv->key)) {
            return kv;
        }
    }

    return NULL;
}

static int remove_xattr(FDIRServerDentry *dentry, const string_t *name)
{
    key_value_pair_t *kv;
    key_value_pair_t *end;

    if (dentry->kv_array == NULL) {
        return ENODATA;
    }

    end = dentry->kv_array->elts + dentry->kv_array->count;
    for (kv=dentry->kv_array->elts; kv<end; kv++) {
        if (fc_string_equal(name, &kv->key)) {
            break;
        }
    }

    if (kv == end) {
        return ENODATA;
    }

    server_delay_free_str(dentry->context, kv->key.str);
    server_delay_free_str(dentry->context, kv->value.str);
    for (kv=kv+1; kv<end; kv++) {
        *(kv - 1) = *kv;
    }
    dentry->kv_array->count--;

    return 0;
}

static key_value_pair_t *check_alloc_kvpair(FDIRDentryContext
        *context, FDIRServerDentry *dentry, int *err_no)
{
    struct fast_mblock_man *allocator;
    SFKeyValueArray *new_array;

    if (dentry->kv_array == NULL) {
        allocator = context->kvarray_allocators + 0;
    } else if (dentry->kv_array->count == dentry->kv_array->alloc) {
        if ((allocator=dentry_next_kvarray_allocator(context,
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
            fast_mblock_delay_free_object(allocator - 1,
                    dentry->kv_array, FDIR_DELAY_FREE_SECONDS);
        }

        dentry->kv_array = new_array;
    }

    *err_no = 0;
    return dentry->kv_array->elts + dentry->kv_array->count;
}

static int set_xattr(FDIRServerDentry *dentry, const FDIRBinlogRecord *record)
{
    int result;
    bool new_create;
    key_value_pair_t *kv;
    string_t value;

    if ((kv=get_xattr(dentry, &record->xattr.key)) != NULL) {
        if (record->flags == XATTR_CREATE) {
            return EEXIST;
        }
        new_create = false;
    } else {
        if (record->flags == XATTR_REPLACE) {
            return ENODATA;
        }

        if ((kv=check_alloc_kvpair(dentry->context,
                        dentry, &result)) == NULL)
        {
            return result;
        }
        if ((result=dentry_strdup(dentry->context,
                        &kv->key, &record->xattr.key)) != 0)
        {
            return result;
        }

        new_create = true;
    }

    if ((result=dentry_strdup(dentry->context,
                    &value, &record->xattr.value)) != 0)
    {
        if (new_create) {
            dentry_strfree(dentry->context, &kv->key);
        }
        return result;
    }

    if (new_create) {
        dentry->kv_array->count++;
    } else {
        server_delay_free_str(dentry->context, kv->value.str);
    }
    kv->value = value;

    return 0;
}

FDIRServerDentry *inode_index_set_xattr(
        const FDIRBinlogRecord *record, int *err_no)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(record->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    dentry = find_inode_entry(bucket, record->inode);
    if (dentry != NULL) {
        if ((*err_no=set_xattr(dentry, record)) != 0) {
            dentry = NULL;
        }
    } else {
        *err_no = ENOENT;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return dentry;
}

FDIRServerDentry *inode_index_remove_xattr(const int64_t inode,
        const string_t *name, int *err_no)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    dentry = find_inode_entry(bucket, inode);
    if (dentry != NULL) {
        if ((*err_no=remove_xattr(dentry, name)) != 0) {
            dentry = NULL;
        }
    } else {
        *err_no = ENOENT;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return dentry;
}

int inode_index_get_xattr(FDIRServerDentry *dentry,
        const string_t *name, string_t *value)
{
    int result;
    key_value_pair_t *kv;

    SET_INODE_HASHTABLE_CTX(dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if ((kv=get_xattr(dentry, name)) != NULL) {
        *value = kv->value;
        result = 0;
    } else {
        result = ENODATA;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return result;
}

void inode_index_list_xattr(FDIRServerDentry *dentry,
        FDIRXAttrIterator *it)
{
    SET_INODE_HASHTABLE_CTX(dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if (dentry->kv_array == NULL) {
        it->kv = it->end = NULL;
    } else {
        it->kv = dentry->kv_array->elts;
        it->end = dentry->kv_array->elts + dentry->kv_array->count;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);
}

FLockTask *inode_index_flock_apply(const int64_t inode, const short type,
        const int64_t offset, const int64_t length, const bool block,
        const FlockOwner *owner, struct fast_task_info *task, int *result)
{
    FDIRServerDentry *dentry;
    FLockTask *ftask;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    do {
        if ((dentry=find_inode_entry(bucket, inode)) == NULL) {
            *result = ENOENT;
            ftask = NULL;
            break;
        }

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

        ftask->type = type;
        ftask->owner = *owner;
        ftask->dentry = dentry;
        ftask->task = task;
        *result = flock_apply(&ctx->flock_ctx, offset, length, ftask, block);
        if (!(*result == 0 || *result == EINPROGRESS)) {
            flock_free_ftask(&ctx->flock_ctx, ftask);
            ftask = NULL;
        }
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return ftask;
}

int inode_index_flock_getlk(const int64_t inode, FLockTask *ftask)
{
    int result;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    do {
        if ((ftask->dentry=find_inode_entry(bucket, inode)) == NULL) {
            result = ENOENT;
            break;
        }

        if (ftask->dentry->flock_entry == NULL) {
            result = ENOENT;
            break;
        }

        result = flock_get_conflict_lock(&ctx->flock_ctx, ftask);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);
    return result;
}

void inode_index_flock_release(FLockTask *ftask)
{
    SET_INODE_HASHTABLE_CTX(ftask->dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if (ftask->dentry->flock_entry != NULL) {
        flock_release(&ctx->flock_ctx, ftask->dentry->flock_entry, ftask);
    }
    flock_free_ftask(&ctx->flock_ctx, ftask);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);
}

SysLockTask *inode_index_sys_lock_apply(const int64_t inode, const bool block,
        struct fast_task_info *task, int *result)
{
    FDIRServerDentry *dentry;
    SysLockTask  *sys_task;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    do {
        if ((dentry=find_inode_entry(bucket, inode)) == NULL) {
            *result = ENOENT;
            sys_task = NULL;
            break;
        }

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
        if (!(*result == 0 || *result == EINPROGRESS)) {
            flock_free_sys_task(&ctx->flock_ctx, sys_task);
            sys_task = NULL;
        }
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return sys_task;
}

int inode_index_sys_lock_release_ex(SysLockTask *sys_task,
        sys_lock_release_callback callback, void *args)
{
    int result;
    SET_INODE_HASHTABLE_CTX(sys_task->dentry->inode);
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    if (sys_task->dentry->flock_entry != NULL) {
        result = sys_lock_release(sys_task->dentry->flock_entry,
                sys_task, callback, args);
    } else {
        result = ENOENT;
    }
    flock_free_sys_task(&ctx->flock_ctx, sys_task);
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    return result;
}

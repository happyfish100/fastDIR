#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "server_global.h"
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
    inode_shared_ctx_array.contexts = (InodeSharedContext *)malloc(bytes);
    if (inode_shared_ctx_array.contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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
    inode_hashtable.buckets = (FDIRServerDentry **)malloc(bytes);
    if (inode_hashtable.buckets == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %"PRId64" bytes fail", __LINE__, bytes);
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
    int64_t bucket_index;       \
    InodeSharedContext *ctx;    \
    do {  \
        bucket_index =  inode % inode_hashtable.capacity;  \
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
    pthread_mutex_lock(&ctx->lock);
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
    pthread_mutex_unlock(&ctx->lock);

    return result;
}

int inode_index_del_dentry(FDIRServerDentry *dentry)
{
    int result;
    FDIRServerDentry *previous;
    FDIRServerDentry *deleted;

    SET_INODE_HT_BUCKET_AND_CTX(dentry->inode);
    pthread_mutex_lock(&ctx->lock);
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
    pthread_mutex_unlock(&ctx->lock);

    return result;
}

FDIRServerDentry *inode_index_get_dentry(const int64_t inode)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    pthread_mutex_lock(&ctx->lock);
    dentry = find_inode_entry(bucket, inode);
    pthread_mutex_unlock(&ctx->lock);

    return dentry;
}

FDIRServerDentry *inode_index_check_set_dentry_size(const int64_t inode,
        const int64_t new_size, const bool force, int *modified_flags)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(inode);
    *modified_flags = 0;
    pthread_mutex_lock(&ctx->lock);
    dentry = find_inode_entry(bucket, inode);
    if (dentry != NULL) {
        if (force || (dentry->stat.size < new_size)) {
            if (dentry->stat.size != new_size) {
                dentry->stat.size = new_size;
                *modified_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_SIZE;
            }
        }

        if (dentry->stat.mtime != g_current_time) {
            dentry->stat.mtime = g_current_time;
            *modified_flags |= FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME;
        }
    }
    pthread_mutex_unlock(&ctx->lock);

    return dentry;
}

static void update_dentry(FDIRServerDentry *dentry,
        const FDIRBinlogRecord *record)
{
    if (record->options.mode) {
        dentry->stat.mode = record->stat.mode;
    }

    if (record->options.ctime) {
        dentry->stat.ctime = record->stat.ctime;
    }

    if (record->options.mtime) {
        dentry->stat.mtime = record->stat.mtime;
    }

    if (record->options.size) {
        dentry->stat.size = record->stat.size;
    }
}

FDIRServerDentry *inode_index_update_dentry(
        const FDIRBinlogRecord *record)
{
    FDIRServerDentry *dentry;

    SET_INODE_HT_BUCKET_AND_CTX(record->inode);
    pthread_mutex_lock(&ctx->lock);
    dentry = find_inode_entry(bucket, record->inode);
    if (dentry != NULL) {
        update_dentry(dentry, record);
    }
    pthread_mutex_unlock(&ctx->lock);

    return dentry;
}

int inode_index_flock_apply(FLockTask *ftask, const int64_t offset,
        const int64_t length)
{
    int result;

    SET_INODE_HASHTABLE_CTX(ftask->dentry->inode);
    pthread_mutex_lock(&ctx->lock);
    do {
        if (ftask->dentry->flock_entry == NULL) {
            ftask->dentry->flock_entry = flock_alloc_init_entry(
                    &ctx->flock_ctx);
            if (ftask->dentry->flock_entry == NULL) {
                result = ENOMEM;
                break;
            }
        }

        result = flock_apply(&ctx->flock_ctx, ftask->dentry->flock_entry,
                offset, length, ftask);
    } while (0);
    pthread_mutex_unlock(&ctx->lock);

    return result;
}

void inode_index_flock_release(FLockTask *ftask)
{
    SET_INODE_HASHTABLE_CTX(ftask->dentry->inode);
    pthread_mutex_lock(&ctx->lock);
    if (ftask->dentry->flock_entry != NULL) {
        flock_release(&ctx->flock_ctx, ftask->dentry->flock_entry, ftask);
    }
    pthread_mutex_unlock(&ctx->lock);
}

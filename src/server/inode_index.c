#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "server_global.h"
#include "inode_index.h"

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
        if ((result=fast_mblock_init_ex1(&ctx->inode_allocator,
                        "inode_entry", sizeof(FDIRServerDentry), 16 * 1024,
                        NULL, NULL, false)) != 0)
        {
            return result;
        }

        if ((result=init_pthread_lock(&ctx->lock)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "init_pthread_lock fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

static int init_inode_hashtable()
{
    int bytes;

    inode_hashtable.capacity = INODE_HASHTABLE_CAPACITY;
    bytes = sizeof(FDIRServerDentry *) * inode_hashtable.capacity;
    inode_hashtable.buckets = (FDIRServerDentry **)malloc(bytes);
    if (inode_hashtable.buckets == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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

static int compare_inode(const FDIRServerDentry *dentry1, const FDIRServerDentry *dentry2)
{
    int64_t sub;

    sub = dentry1->inode - dentry2->inode;
    if (sub < 0) {
        return -1;
    } else if (sub > 0) {
        return 1;
    }

   return 0;
}

static FDIRServerDentry *get_inode_entry(InodeSharedContext *ctx,
        FDIRServerDentry **bucket, const FDIRServerDentry *dentry,
        const bool create_flag)
{
    const int init_level_count = 2;
    FDIRServerDentry *previous;
    FDIRServerDentry *dentry;
    int cmpr;

    if (*bucket == NULL) {
        if (!create_flag) {
            return NULL;
        }
        previous = NULL;
    } else {
        cmpr = compare_inode(dentry, &(*bucket)->dentry);
        if (cmpr == 0) {
            return *bucket;
        } else if (cmpr < 0) {
            previous = NULL;
        } else {
            previous = *bucket;
            while (previous->ht_next != NULL) {
                cmpr = compare_inode(dentry, &previous->ht_next->dentry);
                if (cmpr == 0) {
                    return previous->ht_next;
                } else if (cmpr < 0) {
                    break;
                }

                previous = previous->ht_next;
            }
        }

        if (!create_flag) {
            return NULL;
        }
    }

    dentry = fast_mblock_alloc_object(&ctx->inode_allocator);
    if (dentry == NULL) {
        return NULL;
    }

    if (previous == NULL) {
        dentry->ht_next = *bucket;
        *bucket = dentry;
    } else {
        dentry->ht_next = previous->ht_next;
        previous->ht_next = dentry;
    }
    return dentry;
}

int inode_index_add_dentry(FDIRServerDentry *dentry);
{
    InodeSharedContext *ctx;
    int64_t bucket_index;
    int result;

    bucket_index =  dentry->inode % inode_hashtable.capacity;
    ctx = inode_shared_ctx_array.contexts + bucket_index %
        inode_shared_ctx_array.count;

    pthread_mutex_lock(&ctx->lock);
    //result = add_slice(ctx, slice->ob, slice);
    pthread_mutex_unlock(&ctx->lock);

    return result;
}

//int inode_index_del_dentry(FDIRServerDentry *dentry);

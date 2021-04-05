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
#include <unistd.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/hash.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "common/fdir_types.h"
#include "server_global.h"
#include "data_thread.h"
#include "ns_manager.h"

typedef struct fdir_namespace_hashtable {
    int count;
    FDIRNamespaceEntry **buckets;
} FDIRNamespaceHashtable;

typedef struct fdir_manager {
    struct {
        FDIRNamespaceEntry *head;
        FDIRNamespaceEntry *tail;
    } chain;

    FDIRNamespaceHashtable hashtable;

    struct fast_mblock_man ns_allocator;  //element: FDIRNamespaceEntry
    pthread_mutex_t lock;  //for create namespace
} FDIRManager;

static FDIRManager fdir_manager = {{NULL, NULL}, {0, NULL}};

int ns_manager_init()
{
    int result;
    int bytes;

    memset(&fdir_manager, 0, sizeof(fdir_manager));
    if ((result=fast_mblock_init_ex1(&fdir_manager.ns_allocator,
                    "ns_htable", sizeof(FDIRNamespaceEntry), 4096,
                    0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    fdir_manager.hashtable.count = 0;
    bytes = sizeof(FDIRNamespaceEntry *) * g_server_global_vars.
        namespace_hashtable_capacity;
    fdir_manager.hashtable.buckets = (FDIRNamespaceEntry **)fc_malloc(bytes);
    if (fdir_manager.hashtable.buckets == NULL) {
        return ENOMEM;
    }
    memset(fdir_manager.hashtable.buckets, 0, bytes);

    if ((result=init_pthread_lock(&fdir_manager.lock)) != 0) {
        return result;
    }

    return 0;
}

void ns_manager_destroy()
{
}

int fdir_namespace_stat(const string_t *ns, FDIRNamespaceStat *stat)
{
    int result;
    FDIRNamespaceEntry *ns_entry;

    if ((ns_entry=fdir_namespace_get(NULL, ns, false, &result)) == NULL) {
        return ENOENT;
    }
    stat->used_inodes = __sync_add_and_fetch(&ns_entry->dentry_count, 0);
    stat->used_bytes = __sync_add_and_fetch(&ns_entry->used_bytes, 0);
    return 0;
}

int fdir_namespace_inc_alloc_bytes(FDIRNamespaceEntry *ns_entry,
        const int64_t inc_alloc)
{
    __sync_add_and_fetch(&ns_entry->used_bytes, inc_alloc);
    //TODO
    return 0;
}

static FDIRNamespaceEntry *create_namespace(FDIRDentryContext *context,
        FDIRNamespaceEntry **bucket, const string_t *ns, int *err_no)
{
    FDIRNamespaceEntry *entry;

    entry = (FDIRNamespaceEntry *)fast_mblock_alloc_object(
            &fdir_manager.ns_allocator);
    if (entry == NULL) {
        *err_no = ENOMEM;
        return NULL;
    }

    if ((*err_no=dentry_strdup(context, &entry->name, ns)) != 0) {
        return NULL;
    }

    /*
    logInfo("ns: %.*s, create_namespace: %.*s", ns->len, ns->str,
            entry->name.len, entry->name.str);
            */

    entry->dentry_count = 0;
    entry->used_bytes = 0;
    entry->dentry_root = NULL;
    entry->nexts.htable = *bucket;
    *bucket = entry;

    entry->nexts.list = NULL;
    if (fdir_manager.chain.head == NULL) {
        fdir_manager.chain.head = entry;
    } else {
        fdir_manager.chain.tail->nexts.list = entry;
    }
    fdir_manager.chain.tail = entry;

    context->counters.ns++;
    *err_no = 0;
    return entry;
}

FDIRNamespaceEntry *fdir_namespace_get(FDIRDentryContext *context,
        const string_t *ns, const bool create_ns, int *err_no)
{
    FDIRNamespaceEntry *entry;
    FDIRNamespaceEntry **bucket;
    int hash_code;

    hash_code = simple_hash(ns->str, ns->len);
    bucket = fdir_manager.hashtable.buckets + ((unsigned int)hash_code) %
        g_server_global_vars.namespace_hashtable_capacity;

    entry = *bucket;
    while (entry != NULL && !fc_string_equal(ns, &entry->name)) {
        entry = entry->nexts.htable;
    }

    if (entry != NULL) {
        return entry;
    }
    if (!create_ns) {
        *err_no = ENOENT;
        return NULL;
    }

    PTHREAD_MUTEX_LOCK(&fdir_manager.lock);
    entry = *bucket;
    while (entry != NULL && !fc_string_equal(ns, &entry->name)) {
        entry = entry->nexts.htable;
    }

    if (entry == NULL) {
        entry = create_namespace(context, bucket, ns, err_no);
    } else {
        *err_no = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.lock);

    return entry;
}

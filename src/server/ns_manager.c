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

#define NAMESPACE_DUMP_FILENAME  "namespaces.dat"

#define NAMESPACE_FIELD_MAX               8
#define NAMESPACE_FIELD_COUNT             5

#define NAMESPACE_FIELD_INDEX_ID          0
#define NAMESPACE_FIELD_INDEX_ROOT        1
#define NAMESPACE_FIELD_INDEX_DIRS        2
#define NAMESPACE_FIELD_INDEX_FILES       3
#define NAMESPACE_FIELD_INDEX_USED_BYTES  4

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
    int current_id;
} FDIRManager;

static FDIRManager fdir_manager = {{NULL, NULL}, {0, NULL}};

static int ns_alloc_init_func(FDIRNamespaceEntry *ns_entry, void *args)
{
    FDIRNSSubscribeEntry *subs;
    FDIRNSSubscribeEntry *end;

    end = ns_entry->subs_entries + FDIR_MAX_NS_SUBSCRIBERS;
    for (subs=ns_entry->subs_entries; subs<end; subs++) {
        subs->ns = ns_entry;
    }
    return 0;
}

int ns_manager_init()
{
    int result;
    int element_size;
    int bytes;

    memset(&fdir_manager, 0, sizeof(fdir_manager));
    element_size = sizeof(FDIRNamespaceEntry) +
        sizeof(FDIRNSSubscribeEntry) * FDIR_MAX_NS_SUBSCRIBERS;
    if ((result=fast_mblock_init_ex1(&fdir_manager.ns_allocator,
                    "ns_entry", element_size, 4096, 0,
                    (fast_mblock_alloc_init_func)ns_alloc_init_func,
                    NULL, false)) != 0)
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
    stat->used_inodes = FC_ATOMIC_GET(ns_entry->current.counts.dir) +
        FC_ATOMIC_GET(ns_entry->current.counts.file);
    stat->used_bytes = FC_ATOMIC_GET(ns_entry->current.used_bytes);
    return 0;
}

void fdir_namespace_inc_alloc_bytes(FDIRNamespaceEntry *ns_entry,
        const int64_t inc_alloc)
{
    __sync_add_and_fetch(&ns_entry->current.used_bytes, inc_alloc);
    ns_subscribe_notify_all(ns_entry);
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

    entry->current.counts.dir = 0;
    entry->current.counts.file = 0;
    entry->current.used_bytes = 0;
    entry->current.root.ptr = NULL;
    entry->id = ++fdir_manager.current_id;
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

void fdir_namespace_push_all_to_holding_queue(FDIRNSSubscriber *subscriber)
{
    FDIRNamespaceEntry *ns_entry;
    FDIRNSSubscribeEntry *entry;
    FDIRNSSubscribeEntry *head;
    FDIRNSSubscribeEntry *tail;

    head = tail = NULL;
    PTHREAD_MUTEX_LOCK(&fdir_manager.lock);
    ns_entry = fdir_manager.chain.head;
    while (ns_entry != NULL) {
        entry = ns_entry->subs_entries + subscriber->index;
        if (__sync_bool_compare_and_swap(&entry->entries[
                    FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING].
                    in_queue, 0, 1))
        {
            if (head == NULL) {
                head = entry;
            } else {
                tail->entries[FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING].
                    next = entry;
            }
            tail = entry;
        }

        ns_entry = ns_entry->nexts.list;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.lock);

    if (head != NULL) {
        struct fc_queue_info qinfo;

        tail->entries[FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING].next = NULL;
        qinfo.head = head;
        qinfo.tail = tail;
        fc_queue_push_queue_to_tail_silence(subscriber->queues +
                FDIR_NS_SUBSCRIBE_QUEUE_INDEX_HOLDING, &qinfo);
    }
}

static int realloc_namespace_ptr_array(FDIRNamespaceDumpContext *ctx)
{
    FDIRNamespaceEntry **entries;

    if (ctx->alloc == 0) {
        ctx->alloc = 64;
    }
    do {
        ctx->alloc *= 2;
    } while (ctx->alloc < fdir_manager.ns_allocator.
            info.element_used_count);

    entries = (FDIRNamespaceEntry **)fc_malloc(sizeof(
                FDIRNamespaceEntry *) * ctx->alloc);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (ctx->entries != NULL) {
        free(ctx->entries);
    }
    ctx->entries = entries;

    return 0;
}

static int realloc_buffer(BufferInfo *buffer, const int alloc_bytes)
{
    char *buff;

    if (buffer->alloc_size == 0) {
        buffer->alloc_size = 512;
    }
    do {
        buffer->alloc_size *= 2;
    } while (buffer->alloc_size < alloc_bytes);

    buff = (char *)fc_malloc(buffer->alloc_size);
    if (buff == NULL) {
        return ENOMEM;
    }

    if (buffer->buff != NULL) {
        free(buffer->buff);
    }
    buffer->buff = buff;

    return 0;
}

static int dump_namespaces(FDIRNamespaceDumpContext *ctx)
{
    char filename[PATH_MAX];
    int result;
    int alloc_bytes;
    char *p;
    FDIRNamespaceEntry **entry;
    FDIRNamespaceEntry **end;

    alloc_bytes = ctx->count * 64 + 32;
    if (ctx->buffer.alloc_size < alloc_bytes) {
        if ((result=realloc_buffer(&ctx->buffer, alloc_bytes)) != 0) {
            return result;
        }
    }

    p = ctx->buffer.buff;
    p += sprintf(p, "%d %"PRId64"\n", ctx->count, ctx->last_version);

    end = ctx->entries + ctx->count;
    for (entry=ctx->entries; entry<end; entry++) {
        p += sprintf(p, "%d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
                (*entry)->id, (*entry)->delay.root.inode,
                (*entry)->delay.counts.dir, (*entry)->delay.counts.file,
                (*entry)->delay.used_bytes);
    }
    ctx->buffer.length = p - ctx->buffer.buff;

    snprintf(filename, sizeof(filename), "%s/%s",
            STORAGE_PATH_STR, NAMESPACE_DUMP_FILENAME);
    return safeWriteToFile(filename, ctx->buffer.buff, ctx->buffer.length);
}

int fdir_namespace_dump(FDIRNamespaceDumpContext *ctx)
{
    int result;
    FDIRNamespaceEntry *ns_entry;
    FDIRNamespaceEntry **dest;

    PTHREAD_MUTEX_LOCK(&fdir_manager.lock);
    do {
        if (ctx->alloc <= fdir_manager.ns_allocator.info.element_used_count) {
            if ((result=realloc_namespace_ptr_array(ctx)) != 0) {
                break;
            }
        }

        dest = ctx->entries;
        ns_entry = fdir_manager.chain.head;
        while (ns_entry != NULL) {
            *dest++ = ns_entry;
            ns_entry = ns_entry->nexts.list;
        }
        ctx->count = dest - ctx->entries;
        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.lock);

    if (result != 0) {
        return result;
    }
    return dump_namespaces(ctx);
}

int fdir_namespace_load(int64_t *last_version)
{
    const bool ignore_empty = true;
    char filename[PATH_MAX];
    string_t content;
    int64_t file_size;
    string_t *rows;
    string_t *line;
    string_t *end;
    string_t cols[NAMESPACE_FIELD_MAX];
    int row_count;
    int col_count;
    int header_count;
    int count;
    int result;

    snprintf(filename, sizeof(filename), "%s/%s",
            STORAGE_PATH_STR, NAMESPACE_DUMP_FILENAME);
    if (access(filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
    }

    if ((result=getFileContent(filename, &content.str, &file_size)) != 0) {
        return result;
    }

    row_count = getOccurCount(content.str, '\n');
    if (row_count < 1) {
        logError("file: "__FILE__", line: %d, "
                "namespace file: %s, invalid data format",
                __LINE__, filename);
        return EINVAL;
    }

    rows = (string_t *)fc_malloc(sizeof(string_t) * row_count);
    if (rows == NULL) {
        return ENOMEM;
    }

    count = split_string_ex(&content, '\n', rows, row_count, ignore_empty);
    col_count = split_string_ex(rows + 0, ' ', cols,
            NAMESPACE_FIELD_MAX, ignore_empty);
    if (col_count != 2) {
        logError("file: "__FILE__", line: %d, "
                "namespace file: %s, first line is invalid, column "
                "count: %d != 2", __LINE__, filename, col_count);
        return EINVAL;
    }

    header_count = strtol(cols[0].str, NULL, 10);
    if (count - 1 != header_count) {
        logError("file: "__FILE__", line: %d, "
                "namespace file: %s, record count: %d != "
                "header count: %d", __LINE__, filename,
                count - 1, header_count);
        return EINVAL;
    }
    *last_version = strtoll(cols[1].str, NULL, 10);

    end = rows + count;
    for (line=rows+1; line<end; line++) {
        col_count = split_string_ex(line, ' ', cols,
                NAMESPACE_FIELD_MAX, ignore_empty);
        if (col_count != NAMESPACE_FIELD_COUNT) {
            logError("file: "__FILE__", line: %d, "
                    "namespace file: %s, line no. %d, "
                    "column count: %d != %d", __LINE__,
                    filename, (int)(line - rows) + 1,
                    col_count, NAMESPACE_FIELD_COUNT);
            return EINVAL;
        }

        //TODO
    }

    free(content.str);
    free(rows);
    return 0;
}

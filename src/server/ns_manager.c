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
#include "fastcommon/char_converter.h"
#include "sf/sf_binlog_index.h"
#include "common/fdir_types.h"
#include "server_global.h"
#include "data_thread.h"
#include "dentry.h"
#include "db/dentry_loader.h"
#include "ns_manager.h"

#define NAMESPACE_DUMP_FILENAME    "namespaces.dump"
#define NAMESPACE_BINLOG_FILENAME  "namespaces.binlog"

#define NAMESPACE_FIELD_MAX               8
#define NAMESPACE_FIELD_COUNT             5

#define NAMESPACE_FIELD_INDEX_ID          0
#define NAMESPACE_FIELD_INDEX_ROOT        1
#define NAMESPACE_FIELD_INDEX_DIRS        2
#define NAMESPACE_FIELD_INDEX_FILES       3
#define NAMESPACE_FIELD_INDEX_USED_BYTES  4

#define BINLOG_FIELD_COUNT             2
#define BINLOG_FIELD_INDEX_ID          0
#define BINLOG_FIELD_INDEX_NAME        1

typedef struct fdir_namespace_hashtable {
    int count;
    FDIRNamespaceEntry **buckets;
} FDIRNamespaceHashtable;

typedef struct fdir_manager {
    FDIRNamespaceHashtable hashtable;
    FDIRNamespacePtrArray array;  //sorted by id

    struct fast_mblock_man ns_allocator;  //element: FDIRNamespaceEntry
    pthread_mutex_t lock;  //for create namespace
    FastCharConverter char_converter;
    int fd;  //for namespace create/delete binlog write
    int current_id;
} FDIRManager;

static FDIRManager fdir_manager = {{0, NULL}};

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

static inline void get_binlog_filename(char *filename, const int size)
{
    snprintf(filename, size, "%s/%s", STORAGE_PATH_STR,
            NAMESPACE_BINLOG_FILENAME);
}

int ns_manager_init()
{
    int result;
    int element_size;
    int bytes;
    char filename[PATH_MAX];

    memset(&fdir_manager, 0, sizeof(fdir_manager));
    element_size = sizeof(FDIRNamespaceEntry) +
        sizeof(FDIRNSSubscribeEntry) * FDIR_MAX_NS_SUBSCRIBERS;
    if ((result=fast_mblock_init_ex1(&fdir_manager.ns_allocator,
                    "ns_entry", element_size, 4096, 0,
                    (fast_mblock_object_init_func)ns_alloc_init_func,
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

    if ((result=std_spaces_add_backslash_converter_init(
                    &fdir_manager.char_converter)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        get_binlog_filename(filename, sizeof(filename));
        if ((fdir_manager.fd=open(filename, O_WRONLY |
                        O_CREAT | O_APPEND, 0644)) < 0)
        {
            result = errno != 0 ? errno : EPERM;
            logError("file: "__FILE__", line: %d, "
                    "open binlog file: %s to write fail, "
                    "errno: %d, error info: %s", __LINE__,
                    filename, result, STRERROR(result));
            return result;
        }
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

static int write_binlog(const FDIRNamespaceEntry *entry)
{
    char filename[PATH_MAX];
    char buff[2 * NAME_MAX + 64];
    char escaped[2 * NAME_MAX];
    string_t name;
    int len;
    int result;

    if (!STORAGE_ENABLED) {
        return 0;
    }

    name.str = escaped;
    fast_char_escape(&fdir_manager.char_converter,  entry->name.str,
            entry->name.len, name.str, &name.len, sizeof(escaped));
    len = sprintf(buff, "%d %.*s\n", entry->id, name.len, name.str);
    if (fc_safe_write(fdir_manager.fd, buff, len) != len) {
        result = errno != 0 ? errno : EIO;
        get_binlog_filename(filename, sizeof(filename));
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    if (fsync(fdir_manager.fd) != 0) {
        result = errno != 0 ? errno : EIO;
        get_binlog_filename(filename, sizeof(filename));
        logError("file: "__FILE__", line: %d, "
                "fsync file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    return 0;
}

static int realloc_namespace_array(FDIRNamespacePtrArray *array)
{
    FDIRNamespaceEntry **namespaces;
    int alloc;
    int bytes;

    if (array->alloc == 0) {
        alloc = 64;
    } else {
        alloc = array->alloc * 2;
    }

    bytes = sizeof(FDIRNamespaceEntry *) * alloc;
    namespaces = (FDIRNamespaceEntry **)fc_malloc(bytes);
    if (namespaces == NULL) {
        return ENOMEM;
    }

    if (array->namespaces != NULL) {
        bytes = sizeof(FDIRNamespaceEntry *) * array->count;
        memcpy(namespaces, array->namespaces, bytes);
        sched_delay_free_ptr(array->namespaces, 30);
    }

    array->namespaces = namespaces;
    array->alloc = alloc;
    return 0;
}

static FDIRNamespaceEntry *create_namespace(FDIRDataThreadContext *thread_ctx,
        FDIRNamespaceEntry **bucket, const int id, const string_t *name,
        const unsigned int hash_code, int *err_no)
{
    FDIRNamespaceEntry *entry;

    if (fdir_manager.array.count == fdir_manager.array.alloc) {
        if ((*err_no=realloc_namespace_array(&fdir_manager.array)) != 0) {
            return NULL;
        }
    }

    entry = (FDIRNamespaceEntry *)fast_mblock_alloc_object(
            &fdir_manager.ns_allocator);
    if (entry == NULL) {
        *err_no = ENOMEM;
        return NULL;
    }

    memset(entry, 0, sizeof(*entry));
    if ((*err_no=dentry_strdup(&thread_ctx->dentry_context,
                    &entry->name, name)) != 0)
    {
        return NULL;
    }

    /*
    logInfo("ns: %.*s(%d), create_namespace: %.*s(%d)", name->len, name->str,
            name->len, entry->name.len, entry->name.str, entry->name.len);
            */

    entry->id = id;
    entry->hash_code = hash_code;
    entry->thread_ctx = thread_ctx;
    entry->nexts.htable = *bucket;
    *bucket = entry;

    fdir_manager.array.namespaces[fdir_manager.array.count] = entry;
    fdir_manager.array.count++;
    thread_ctx->dentry_context.counters.ns++;
    return entry;
}

#define NAMESPACE_SET_HT_BUCKET(ns) \
    FDIRNamespaceEntry **bucket; \
    unsigned int hash_code; \
    \
    hash_code = fc_simple_hash((ns)->str, (ns)->len);  \
    bucket = fdir_manager.hashtable.buckets + (hash_code) % \
        g_server_global_vars.namespace_hashtable_capacity


FDIRNamespaceEntry *fdir_namespace_get(FDIRDataThreadContext *thread_ctx,
        const string_t *ns, const bool create_ns, int *err_no)
{
    FDIRNamespaceEntry *entry;

    NAMESPACE_SET_HT_BUCKET(ns);
    entry = *bucket;
    while (entry != NULL && !fc_string_equal(ns, &entry->name)) {
        entry = entry->nexts.htable;
    }

    if (entry != NULL) {
        *err_no = 0;
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
        if ((entry=create_namespace(thread_ctx, bucket, ++fdir_manager.
                        current_id, ns, hash_code, err_no)) != NULL)
        {
            if ((*err_no=write_binlog(entry)) != 0) {
                entry = NULL;
            }
        }
    } else {
        *err_no = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.lock);

    return entry;
}

const FDIRNamespacePtrArray *fdir_namespace_get_all()
{
    return &fdir_manager.array;
}

static int namespace_compare_by_id(const FDIRNamespaceEntry **ns1,
        const FDIRNamespaceEntry **ns2)
{
    return (int)(*ns1)->id - (int)(*ns2)->id;
}

FDIRNamespaceEntry *fdir_namespace_get_by_id(const int id)
{
    FDIRNamespaceEntry holder;
    FDIRNamespaceEntry *target;
    FDIRNamespaceEntry **found;

    if ((id >= 1 && id <= fdir_manager.array.count) && (fdir_manager.
                array.namespaces[id - 1]->id == id))
    {
        return fdir_manager.array.namespaces[id - 1];
    }

    PTHREAD_MUTEX_LOCK(&fdir_manager.lock);
    target = &holder;
    target->id = id;
    found = bsearch(&target, fdir_manager.array.namespaces,
            fdir_manager.array.count, sizeof(FDIRNamespaceEntry *),
            (int (*)(const void *, const void *))namespace_compare_by_id);
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.lock);
    return (found != NULL ? (*found) : NULL);
}

void fdir_namespace_push_all_to_holding_queue(FDIRNSSubscriber *subscriber)
{
    FDIRNamespaceEntry **ns_entry;
    FDIRNamespaceEntry **ns_end;
    FDIRNSSubscribeEntry *entry;
    FDIRNSSubscribeEntry *head;
    FDIRNSSubscribeEntry *tail;

    head = tail = NULL;
    PTHREAD_MUTEX_LOCK(&fdir_manager.lock);
    ns_end = fdir_manager.array.namespaces + fdir_manager.array.count;
    for (ns_entry=fdir_manager.array.namespaces;
            ns_entry<ns_end; ns_entry++)
    {
        entry = (*ns_entry)->subs_entries + subscriber->index;
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

static int realloc_namespace_ptr_array(FDIRNamespaceDumpContext *ctx,
        const int target_count)
{
    FDIRNamespaceEntry **entries;

    if (ctx->alloc == 0) {
        ctx->alloc = 64;
    }
    do {
        ctx->alloc *= 2;
    } while (ctx->alloc < target_count);

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
    int bytes;

    PTHREAD_MUTEX_LOCK(&fdir_manager.lock);
    do {
        if (ctx->alloc <= fdir_manager.array.count) {
            if ((result=realloc_namespace_ptr_array(ctx,
                            fdir_manager.array.count)) != 0)
            {
                break;
            }
        }

        bytes = sizeof(FDIRNamespaceEntry *) * fdir_manager.array.count;
        memcpy(ctx->entries, fdir_manager.array.namespaces, bytes);
        ctx->count = fdir_manager.array.count;
        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.lock);

    if (result != 0) {
        return result;
    }
    return dump_namespaces(ctx);
}

static int parse_binlog_line(const string_t *line, char *error_info)
{
    const bool ignore_empty = true;
    int result;
    int id;
    string_t name;
    int col_count;
    char *endptr;
    string_t cols[NAMESPACE_FIELD_MAX];
    char buff[2 * NAME_MAX];

    col_count = split_string_ex(line, ' ', cols,
            NAMESPACE_FIELD_MAX, ignore_empty);
    if (col_count != BINLOG_FIELD_COUNT) {
        sprintf(error_info, "column count: %d != %d",
                col_count, BINLOG_FIELD_COUNT);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(id, "namespace id",
            BINLOG_FIELD_INDEX_ID, ' ', 1);

    if (cols[BINLOG_FIELD_INDEX_NAME].len > sizeof(buff)) {
        sprintf(error_info, "namespace length: %d is too large",
                cols[BINLOG_FIELD_INDEX_NAME].len);
        return EOVERFLOW;
    }

    memcpy(buff, cols[BINLOG_FIELD_INDEX_NAME].str,
            cols[BINLOG_FIELD_INDEX_NAME].len);
    FC_SET_STRING_EX(name, buff, cols[BINLOG_FIELD_INDEX_NAME].len);
    fast_char_unescape(&fdir_manager.char_converter, name.str, &name.len);

    {
        FDIRDataThreadContext *thread_ctx;
        NAMESPACE_SET_HT_BUCKET(&name);
        thread_ctx = get_data_thread_context(hash_code);
        if (create_namespace(thread_ctx, bucket, id, &name,
                    hash_code, &result) != NULL)
        {
            fdir_manager.current_id = id;
        }
    }

    return result;
}

static int do_load(const char *filename, const string_t *content)
{
    int result;
    int line_count;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;
    char error_info[256];

    line_count = 0;
    result = 0;
    *error_info = '\0';
    line_start = content->str;
    buff_end = content->str + content->len;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        ++line_count;
        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=parse_binlog_line(&line, error_info)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "parse namespace binlog fail, filename: %s, "
                    "line no: %d, error info: %s", __LINE__,
                    filename, line_count, error_info);
            break;
        }

        line_start = line_end + 1;
    }

    return result;
}

static int alloc_namespace_array(FDIRNamespacePtrArray *array,
        const int target_count)
{
    array->alloc = 128;
    while (array->alloc < target_count) {
        array->alloc *= 2;
    }

    array->namespaces = (FDIRNamespaceEntry **)fc_malloc(
            sizeof(FDIRNamespaceEntry *) * array->alloc);
    return (array->namespaces != NULL ? 0 : ENOMEM);
}

static int load_namespaces_from_binlog()
{
    char filename[PATH_MAX];
    string_t content;
    int64_t file_size;
    int row_count;
    int result;

    get_binlog_filename(filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
    }

    if ((result=getFileContent(filename, &content.str, &file_size)) != 0) {
        return result;
    }

    if (file_size > 0) {
        content.len = file_size;
        row_count = getOccurCount(content.str, '\n');
        if ((result=alloc_namespace_array(&fdir_manager.
                        array, row_count)) == 0)
        {
            result = do_load(filename, &content);
        }
    }
    free(content.str);
    return result;
}

static int parse_dumped_line(const string_t *line, char *error_info)
{
    const bool ignore_empty = true;
    int id;
    int col_count;
    char *endptr;
    FDIRNamespaceEntry *entry;
    string_t cols[NAMESPACE_FIELD_MAX];

    col_count = split_string_ex(line, ' ', cols,
            NAMESPACE_FIELD_MAX, ignore_empty);
    if (col_count != NAMESPACE_FIELD_COUNT) {
        sprintf(error_info, "column count: %d != %d",
                col_count, NAMESPACE_FIELD_COUNT);
        return EINVAL;
    }

    SF_BINLOG_PARSE_INT_SILENCE(id, "namespace id",
            NAMESPACE_FIELD_INDEX_ID, ' ', 1);

    if ((entry=fdir_namespace_get_by_id(id)) == NULL) {
        sprintf(error_info, "namespace id: %d not exist", id);
        return ENOENT;
    }

    SF_BINLOG_PARSE_INT_SILENCE(entry->delay.root.inode,
            "root inode", NAMESPACE_FIELD_INDEX_ROOT, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(entry->delay.counts.dir,
            "directory count", NAMESPACE_FIELD_INDEX_DIRS, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(entry->delay.counts.file,
            "file count", NAMESPACE_FIELD_INDEX_FILES, ' ', 0);
    SF_BINLOG_PARSE_INT_SILENCE(entry->delay.used_bytes,
            "used bytes", NAMESPACE_FIELD_INDEX_USED_BYTES, '\n', 0);

    entry->current.counts.dir = entry->delay.counts.dir;
    entry->current.counts.file = entry->delay.counts.file;
    entry->current.used_bytes = entry->delay.used_bytes;
    entry->thread_ctx->dentry_context.counters.dir +=
        entry->current.counts.dir;
    entry->thread_ctx->dentry_context.counters.file +=
        entry->current.counts.file;

    return 0;
}

int fdir_namespace_load(int64_t *last_version)
{
    const bool ignore_empty = true;
    char filename[PATH_MAX];
    char error_info[256];
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

    if ((result=load_namespaces_from_binlog()) != 0) {
        return result;
    }

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

    content.len = file_size;
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

    *error_info = '\0';
    end = rows + count;
    for (line=rows+1; line<end; line++) {
        if ((result=parse_dumped_line(line, error_info)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "namespace dump file: %s, line no. %d, %s", __LINE__,
                    filename, (int)(line - rows) + 1, error_info);
            break;
        }
    }

    free(content.str);
    free(rows);
    return result;
}

int fdir_namespace_load_root()
{
    int result;
    FDIRNamespaceEntry **entry;
    FDIRNamespaceEntry **end;

    end = fdir_manager.array.namespaces + fdir_manager.array.count;
    for (entry=fdir_manager.array.namespaces; entry<end; entry++) {
        if ((*entry)->delay.root.inode != 0) {
            if ((result=dentry_load_root(*entry, (*entry)->delay.root.inode,
                            &(*entry)->current.root.ptr)) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "namespace: %.*s, load root inode: %"PRId64" "
                        "fail, errno: %d, error info: %s", __LINE__,
                        (*entry)->name.len, (*entry)->name.str,
                        (*entry)->delay.root.inode,
                        result, STRERROR(result));
                return result;
            }
        }
    }

    return 0;
}

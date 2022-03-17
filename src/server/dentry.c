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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/hash.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "common/fdir_types.h"
#include "server_global.h"
#include "service_handler.h"
#include "inode_generator.h"
#include "inode_index.h"
#include "db/change_notify.h"
#include "db/dentry_loader.h"
#include "dentry.h"

typedef struct {
    string_t holder;
    string_t *ptr;
} StringHolderPtrPair;

const int max_level_count = 20;

static int dentry_init_obj_with_db(FDIRServerDentry *dentry, void *init_args)
{
    dentry_lru_init_dlink(dentry);
    return 0;
}

int dentry_init()
{
    const int min_bits = 6;
    const int max_bits = 16;
    int result;
    int element_size;
    fast_mblock_object_init_func init_func;

    if ((result=ptr_array_allocator_init(&DENTRY_PARRAY_ALLOCATOR,
                    min_bits, max_bits)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        element_size = sizeof(FDIRServerDentry) +
            sizeof(FDIRServerDentryDBArgs);
        init_func = (fast_mblock_object_init_func)dentry_init_obj_with_db;
    } else {
        element_size = sizeof(FDIRServerDentry);
        init_func = NULL;
    }
    if ((result=fast_mblock_init_ex1(&DENTRY_ALLOCATOR,
                    "dentry", element_size, 8 * 1024, 0,
                    init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=ns_manager_init()) != 0) {
        return result;
    }
    return inode_index_init();
}

void dentry_destroy()
{
    ns_manager_destroy();
}

/*
static void dentry_children_print(FDIRServerDentry *dentry)
{
    FDIRServerDentry *current;
    UniqSkiplistIterator it;
    int i = 0;

    if (dentry->children == NULL) {
        logInfo("not skiplist");
        return;
    }

    uniq_skiplist_iterator(dentry->children, &it);
    while ((current=(FDIRServerDentry *)uniq_skiplist_next(&it)) != NULL) {
        logInfo("%d. %"PRId64", %.*s(%d)", ++i, current->inode,
                current->name.len, current->name.str, current->name.len);
    }
}
*/

/* alloc dentry */
static inline FDIRServerDentry *dentry_pool_pop(FDIRDentryPool *pool)
{
    struct fast_mblock_node *node;
    FDIRServerDentry *dentry;

    node = pool->chain.head;
    if (--(pool->count) > 0) {
        pool->chain.head = node->next;
    }

    dentry = (FDIRServerDentry *)node->data;
    dentry->context = &pool->thread_ctx->dentry_context;
    return dentry;
}

/* free dentry */
static inline void dentry_pool_push(FDIRDentryPool *pool,
        FDIRServerDentry *dentry)
{
    struct fast_mblock_node *node;

    node = fast_mblock_to_node_ptr(dentry);
    if (pool->count == 0) {
        pool->chain.head = node;
    } else {
        pool->chain.tail->next = node;
    }
    pool->chain.tail = node;
    ++(pool->count);
}

FDIRServerDentry *dentry_alloc_object(FDIRDataThreadContext *thread)
{
    if (thread->dentry_context.pools.batch_free.count > 0) {
        return dentry_pool_pop(&thread->dentry_context.pools.batch_free);
    }

    if (thread->dentry_context.pools.local_alloc.count > 0) {
        return dentry_pool_pop(&thread->dentry_context.pools.local_alloc);
    }

    if (fast_mblock_batch_alloc(&DENTRY_ALLOCATOR, thread->dentry_context.
                pools.local_alloc.limit, &thread->dentry_context.pools.
                local_alloc.chain) == 0)
    {
        if (STORAGE_ENABLED) {
            thread->lru_ctx.total_count += thread->
                dentry_context.pools.local_alloc.limit;
        }

        thread->dentry_context.pools.local_alloc.count = thread->
            dentry_context.pools.local_alloc.limit;
        return dentry_pool_pop(&thread->dentry_context.pools.local_alloc);
    } else {
        return NULL;
    }
}

static inline void dentry_free_object(FDIRServerDentry *dentry)
{
    if (dentry->context->pools.local_alloc.count < dentry->
            context->pools.local_alloc.limit)
    {
        dentry_pool_push(&dentry->context->pools.local_alloc, dentry);
        return;
    }

    dentry_pool_push(&dentry->context->pools.batch_free, dentry);
    if (dentry->context->pools.batch_free.count >=
            dentry->context->pools.batch_free.limit)
    {
        if (STORAGE_ENABLED) {
            dentry->context->thread_ctx->lru_ctx.total_count -=
                dentry->context->pools.batch_free.count;
        }

        dentry->context->pools.batch_free.chain.tail->next = NULL;
        fast_mblock_batch_free(&DENTRY_ALLOCATOR, &dentry->
                context->pools.batch_free.chain);
        dentry->context->pools.batch_free.count = 0;
    }
}

static int dentry_compare(const void *p1, const void *p2)
{
    return fc_string_compare(&((FDIRServerDentry *)p1)->name,
            &((FDIRServerDentry *)p2)->name);
}

static void dentry_free_xattrs(FDIRServerDentry *dentry)
{
    key_value_pair_t *kv;
    key_value_pair_t *end;
    struct fast_mblock_man *allocator;

    if (dentry->kv_array == NULL) {
        return;
    }

    end = dentry->kv_array->elts + dentry->kv_array->count;
    for (kv=dentry->kv_array->elts; kv<end; kv++) {
        fast_allocator_free(&dentry->context->name_acontext, kv->key.str);
        fast_allocator_free(&dentry->context->name_acontext, kv->value.str);
    }

    dentry->kv_array->count = 0;
    if ((allocator=dentry_get_kvarray_allocator_by_capacity(
                    dentry->context, dentry->kv_array->alloc)) != NULL)
    {
        fast_mblock_free_object(allocator, dentry->kv_array);
    }

    dentry->kv_array = NULL;
}

void dentry_free_for_elimination(FDIRServerDentry *dentry)
{
    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        dentry->src_dentry = NULL;
    } else if (S_ISLNK(dentry->stat.mode) && dentry->link.str != NULL) {
        fast_allocator_free(&dentry->context->
                name_acontext, dentry->link.str);
        FC_SET_STRING_NULL(dentry->link);
    }
    dentry_free_xattrs(dentry);

    if (dentry->flock_entry != NULL) {
        inode_index_free_flock_entry(dentry);
        dentry->flock_entry = NULL;
    }

    if (STORAGE_ENABLED) {
        if (dentry->db_args->children != NULL) {
            id_name_pair_t *pair;
            id_name_pair_t *end;

            end = dentry->db_args->children->elts +
                dentry->db_args->children->count;
            for (pair=dentry->db_args->children->elts; pair<end; pair++) {
                dentry_strfree(dentry->context, &pair->name);
            }
            id_name_array_allocator_free(&ID_NAME_ARRAY_ALLOCATOR_CTX,
                    dentry->db_args->children);
            dentry->db_args->children = NULL;
        }

        if ((dentry->db_args->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_BASIC)) {
            dentry_lru_del(dentry);
        }
    }
}

bool dentry_free_ex(FDIRServerDentry *dentry, const int dec_count)
{
    if (__sync_sub_and_fetch(&dentry->reffer_count, dec_count) != 0) {
        return false;
    }

    if (!(STORAGE_ENABLED && dentry->db_args->loaded_flags == 0)) {
        if (dentry->children != NULL) {
            uniq_skiplist_free(dentry->children);
            dentry->children = NULL;
        }
        dentry_free_for_elimination(dentry);
    }

    fast_allocator_free(&dentry->context->name_acontext, dentry->name.str);
    dentry_free_object(dentry);
    return true;
}

#define dentry_delay_free(dentry) server_delay_free_dentry( \
            (dentry)->context->thread_ctx, dentry)

static void dentry_immediate_free(void *ctx, void *ptr)
{
    dentry_free_ex(ptr, (long)ctx);
}

static void dentry_free_func(FDIRServerDentry *dentry,
        const int delay_seconds)
{
    dentry_delay_free(dentry);
}

static void dentry_free_func_with_db(FDIRServerDentry *dentry,
        const int delay_seconds)
{
    if ((dentry->db_args->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_BASIC)) {
        dentry_delay_free(dentry);
    } else {
        dentry_free_ex(dentry, 1);
    }
}

void dentry_release_ex(FDIRServerDentry *dentry, const int dec_count)
{
    server_add_to_immediate_free_queue_ex(&dentry->context->
            thread_ctx->free_context, (void *)(long)dec_count,
            dentry, dentry_immediate_free);
}

static int init_name_allocators(struct fast_allocator_context *name_acontext)
{
#define NAME_REGION_COUNT 4
    struct fast_region_info regions[NAME_REGION_COUNT];
    int count;

    FAST_ALLOCATOR_INIT_REGION(regions[0], 0, 64, 8, 8 * 1024);
    if (DENTRY_MAX_DATA_SIZE <= NAME_MAX + 1) {
        FAST_ALLOCATOR_INIT_REGION(regions[1], 64, NAME_MAX + 1, 8, 4 * 1024);
        count = 2;
    } else {
        FAST_ALLOCATOR_INIT_REGION(regions[1],  64,  256,  8, 4 * 1024);
        if (DENTRY_MAX_DATA_SIZE <= 1024) {
            FAST_ALLOCATOR_INIT_REGION(regions[2], 256, DENTRY_MAX_DATA_SIZE,
                    16, 2 * 1024);
            count = 3;
        } else {
            FAST_ALLOCATOR_INIT_REGION(regions[2], 256, 1024, 16, 2 * 1024);
            FAST_ALLOCATOR_INIT_REGION(regions[3], 1024,
                    DENTRY_MAX_DATA_SIZE, 32, 1024);
            count = 4;
        }
    }

    return fast_allocator_init_ex(name_acontext, "name",
            regions, count, 0, 0.00, 0, false);
}

static int kvarray_alloc_init(SFKeyValueArray *kv_array,
        struct fast_mblock_man *allocator)
{
    kv_array->elts = (key_value_pair_t *)(kv_array + 1);
    kv_array->alloc = (allocator->info.element_size -
            sizeof(SFKeyValueArray)) / sizeof(key_value_pair_t);
    return 0;
}

static int init_kvarray_allocators(struct fast_mblock_man
        *kvarray_allocators, const int count)
{
    struct fast_mblock_man *mblock;
    struct fast_mblock_man *end;
    char name[64];
    int n;
    int alloc_count;
    int alloc_elements_once;
    int element_size;
    int result;

    alloc_elements_once = 8 * 1024;
    alloc_count = 1;
    end = kvarray_allocators + count;
    for (mblock=kvarray_allocators, n=1; mblock<end; mblock++, n++) {
        alloc_count *= 2;
        sprintf(name, "kvarray-%d-elts", alloc_count);
        element_size = sizeof(SFKeyValueArray) +
            sizeof(key_value_pair_t) * alloc_count;
        if ((result=fast_mblock_init_ex1(mblock, name, element_size,
                        alloc_elements_once, 0, (fast_mblock_object_init_func)
                        kvarray_alloc_init, mblock, false)) != 0)
        {
            return result;
        }

        if (n % 2 == 0) {
            alloc_elements_once /= 2;
        }
    }

    return 0;
}

struct fast_mblock_man *dentry_get_kvarray_allocator_by_capacity(
        FDIRDentryContext *context, const int alloc_elts)
{
    switch (alloc_elts) {
        case 2:
            return context->kvarray_allocators + 0;
        case 4:
            return context->kvarray_allocators + 1;
        case 8:
            return context->kvarray_allocators + 2;
        case 16:
            return context->kvarray_allocators + 3;
        case 32:
            return context->kvarray_allocators + 4;
        case 64:
            return context->kvarray_allocators + 5;
        default:
            return NULL;
    }
}

static inline void dentry_init_pool(FDIRDentryPool *pool,
        FDIRDataThreadContext *thread_ctx, const int limit)
{
    pool->thread_ctx = thread_ctx;
    pool->limit = limit;
    pool->chain.head = pool->chain.tail = NULL;
    pool->count = 0;
}

int dentry_init_context(FDIRDataThreadContext *thread_ctx)
{
    const int delay_free_seconds = 0;
    FDIRDentryContext *context;
    uniq_skiplist_free_func free_func;
    int result;

    context = &thread_ctx->dentry_context;
    context->thread_ctx = thread_ctx;

    if (STORAGE_ENABLED) {
        free_func = (uniq_skiplist_free_func)dentry_free_func_with_db;
    } else {
        free_func = (uniq_skiplist_free_func)dentry_free_func;
    }
    if ((result=uniq_skiplist_init_ex(&context->factory, max_level_count,
                    dentry_compare, free_func, 16 * 1024,
                    SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE,
                    delay_free_seconds)) != 0)
    {
        return result;
    }

    if ((result=init_name_allocators(&context->name_acontext)) != 0) {
        return result;
    }

    if ((result=init_kvarray_allocators(context->kvarray_allocators,
                    FDIR_XATTR_KVARRAY_ALLOCATOR_COUNT)) != 0)
    {
        return result;
    }

    dentry_init_pool(&context->pools.local_alloc, thread_ctx, 4096);
    dentry_init_pool(&context->pools.batch_free, thread_ctx, 1024);
    return 0;
}

static inline int find_child(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *parent, const string_t *name,
        FDIRServerDentry **child)
{
    int result;
    FDIRServerDentry target;

    if (STORAGE_ENABLED) {
        if ((result=dentry_check_load(thread_ctx, parent)) != 0) {
            *child = NULL;
            return result;
        }
    }

    if (!S_ISDIR(parent->stat.mode)) {
        *child = NULL;
        return ENOTDIR;
    }

    target.name = *name;
    if ((*child=(FDIRServerDentry *)uniq_skiplist_find(
                    parent->children, &target)) == NULL)
    {
        return ENOENT;
    }

    if (STORAGE_ENABLED) {
        if ((result=dentry_check_load(thread_ctx, *child)) != 0) {
            *child = NULL;
            return result;
        }
    }

    return 0;
}

static inline int do_find_ex(FDIRNamespaceEntry *ns_entry,
        const string_t *paths, const int count,
        FDIRServerDentry **dentry)
{
    int result;
    const string_t *p;
    const string_t *end;

    *dentry = ns_entry->current.root.ptr;
    end = paths + count;
    for (p=paths; p<end; p++) {
        if ((result=find_child(ns_entry->thread_ctx,
                        *dentry, p, dentry)) != 0)
        {
            return result;
        }
    }

    return 0;
}

void dentry_set_inc_alloc_bytes(FDIRServerDentry *dentry,
        const int64_t inc_alloc)
{
    dentry->stat.alloc += inc_alloc;
    fdir_namespace_inc_alloc_bytes(dentry->ns_entry, inc_alloc);
}

int dentry_find_parent(const FDIRDEntryFullName *fullname,
    FDIRServerDentry **parent, string_t *my_name)
{
    FDIRPathInfo path_info;
    FDIRNamespaceEntry *ns_entry;
    int result;

    if (fullname->path.len == 0 || fullname->path.str[0] != '/') {
        *parent = NULL;
        return EINVAL;
    }

    ns_entry = fdir_namespace_get(NULL, &fullname->ns, false, &result);
    if (ns_entry == NULL) {
        my_name->len = 0;
        if (fullname->path.len == 1) {
            my_name->str = "";
        } else {
            my_name->str = NULL;
        }
        *parent = NULL;
        return result;
    }

    if (ns_entry->current.root.ptr == NULL) {
        *parent = NULL;
        my_name->len = 0;
        my_name->str = "";
        return ENOENT;
    }

    path_info.count = split_string_ex(&fullname->path, '/',
        path_info.paths, FDIR_MAX_PATH_COUNT, true);
    if (path_info.count == 0) {
        *parent = NULL;
        my_name->len = 0;
        my_name->str = "";
        return 0;
    }

    *my_name = path_info.paths[path_info.count - 1];
    if (path_info.count == 1) {
        *parent = ns_entry->current.root.ptr;
    } else {
        if ((result=do_find_ex(ns_entry, path_info.paths,
                        path_info.count - 1, parent)) != 0)
        {
            return result;
        }
    }

    if (!S_ISDIR((*parent)->stat.mode)) {
        *parent = NULL;
        return ENOTDIR;
    }

    return 0;
}

static int dentry_find_parent_and_me(const FDIRDEntryFullName *fullname,
        FDIRPathInfo *path_info, string_t *my_name,
        FDIRNamespaceEntry **ns_entry, FDIRServerDentry **parent,
        FDIRServerDentry **me)
{
    int result;

    if (fullname->path.len == 0 || fullname->path.str[0] != '/') {
        *ns_entry = NULL;
        *parent = *me = NULL;
        return EINVAL;
    }

    *ns_entry = fdir_namespace_get(NULL, &fullname->ns, false, &result);
    if (*ns_entry == NULL) {
        *parent = *me = NULL;
        return result;
    }

    if ((*ns_entry)->current.root.ptr == NULL) {
        *parent = *me = NULL;
        return ENOENT;
    }

    path_info->count = split_string_ex(&fullname->path, '/',
            path_info->paths, FDIR_MAX_PATH_COUNT, true);
    if (path_info->count == 0) {
        *parent = NULL;
        *me = (*ns_entry)->current.root.ptr;
        my_name->len = 0;
        my_name->str = "";
        return 0;
    }

    *my_name = path_info->paths[path_info->count - 1];
    if (path_info->count == 1) {
        *parent = (*ns_entry)->current.root.ptr;
    } else {
        if ((result=do_find_ex(*ns_entry, path_info->paths,
                        path_info->count - 1, parent)) != 0)
        {
            *me = NULL;
            return result;
        }
    }

    return find_child((*ns_entry)->thread_ctx, *parent, my_name, me);
}

int dentry_resolve_symlink(FDIRServerDentry **dentry)
{
    FDIRPathInfo path_info;
    FDIRNamespaceEntry *ns_entry;
    char buff[PATH_MAX];
    BufferInfo buffer;
    char path1[PATH_MAX];
    char path2[PATH_MAX];
    char *full_paths[2];
    char *full_fname;
    SFErrorInfo error_info;
    string_t path;
    int result;
    int loop;

    ns_entry = (*dentry)->ns_entry;
    if (ns_entry->current.root.ptr == NULL) {
        *dentry = NULL;
        return ENOENT;
    }

    *(error_info.message) = '\0';
    buffer.buff = buff;
    buffer.alloc_size = sizeof(buff);
    path.str = NULL;
    full_paths[0] = path1;
    full_paths[1] = path2;
    loop = 0;

    do {
        if (*(*dentry)->link.str == '/') {
            path = (*dentry)->link;
        } else {
            if (path.str == NULL) {
                if ((result=dentry_get_full_path(*dentry,
                                &buffer, &error_info)) != 0)
                {
                    logError("file: "__FILE__", line: %d, "
                            "get dentry path fail, error info: %s",
                            __LINE__, error_info.message);
                    *dentry = NULL;
                    return result;
                }

                path.str = buffer.buff;
            }

            full_fname = full_paths[loop % 2];
            path.len = normalize_path(path.str,
                    (*dentry)->link.str,
                    full_fname, PATH_MAX);
            path.str = full_fname;
        }

        path_info.count = split_string_ex(&path, '/',
                path_info.paths, FDIR_MAX_PATH_COUNT, true);
        if ((result=do_find_ex(ns_entry, path_info.paths,
                        path_info.count, dentry)) != 0)
        {
            return result;
        }

        FDIR_SET_HARD_LINK_DENTRY(*dentry);
        if (!S_ISLNK((*dentry)->stat.mode)) {
            return 0;
        }
    } while (++loop < FDIR_FOLLOW_SYMLINK_MAX);

    *dentry = NULL;
    return ELOOP;
}

static int dentry_find_me(FDIRDataThreadContext *thread_ctx,
        const string_t *ns, FDIRRecordDEntry *rec_entry,
        FDIRNamespaceEntry **ns_entry, const bool create_ns)
{
    int result;

    if (rec_entry->parent == NULL) {
        *ns_entry = fdir_namespace_get(thread_ctx, ns, create_ns, &result);
        if (*ns_entry == NULL) {
            return result;
        }

        if ((*ns_entry)->current.root.ptr == NULL) {
            return (rec_entry->pname.name.len == 0 ? ENOENT : EINVAL);
        }

        if (rec_entry->pname.name.len == 0) {
            rec_entry->dentry = (*ns_entry)->current.root.ptr;
            return 0;
        } else {
            return EINVAL;
        }
    } else {
        *ns_entry = rec_entry->parent->ns_entry;
    }

    return find_child(thread_ctx, rec_entry->parent,
            &rec_entry->pname.name, &rec_entry->dentry);
}

#define AFFECTED_DENTRIES_ADD(record, _dentry, _op_type) \
    do { \
        record->affected.entries[record->affected.count].dentry = _dentry;   \
        record->affected.entries[record->affected.count].op_type = _op_type; \
        record->affected.count++;  \
    } while (0)

int dentry_create(FDIRDataThreadContext *thread_ctx, FDIRBinlogRecord *record)
{
    FDIRNamespaceEntry *ns_entry;
    FDIRServerDentry *current;
    bool is_dir;
    int result;

    if ((record->stat.mode & S_IFMT) == 0 &&
            !FDIR_IS_DENTRY_HARD_LINK(record->stat.mode))
    {
        logError("file: "__FILE__", line: %d, "
                "invalid file mode: %d",
                __LINE__, record->stat.mode);
        return EINVAL;
    }

    if ((result=dentry_find_me(thread_ctx, &record->ns,
                    &record->me, &ns_entry, true)) != ENOENT)
    {
        return (result == 0 ? EEXIST : result);
    }

    if ((current=dentry_alloc_object(thread_ctx)) == NULL) {
        return ENOMEM;
    }
    __sync_add_and_fetch(&current->reffer_count, 1);

    is_dir = S_ISDIR(record->stat.mode);
    if (is_dir) {
        current->children = uniq_skiplist_new(&thread_ctx->dentry_context.
                factory, DENTRY_SKIPLIST_INIT_LEVEL_COUNT);
        if (current->children == NULL) {
            return ENOMEM;
        }
    } else {
        current->children = NULL;
    }

    current->parent = record->me.parent;
    if ((result=dentry_strdup(&thread_ctx->dentry_context,
                    &current->name, &record->me.pname.name)) != 0)
    {
        return result;
    }

    if (FDIR_IS_DENTRY_HARD_LINK(record->stat.mode)) {
        current->src_dentry = record->hdlink.src.dentry;
    } else if (S_ISLNK(record->stat.mode)) {
        if ((result=dentry_strdup(&thread_ctx->dentry_context,
                        &current->link, &record->link)) != 0)
        {
            return result;
        }
    }

    /*
    {
        char hex1[256];
        char hex2[256];
        bin2hex(current->name.str, current->name.len, hex1);
        bin2hex(record->me.pname.name.str, record->me.pname.name.len, hex2);
        logInfo("file: "__FILE__", line: %d, "
            "current name: %p => %.*s(%d), hex1: %s, input name: %.*s(%d), hex2: %s",
            __LINE__, current->name.str, current->name.len,
            current->name.str, current->name.len, hex1, record->me.pname.name.len,
            record->me.pname.name.str, record->me.pname.name.len, hex2);
    }
    */

    if (record->inode == 0) {
        current->inode = inode_generator_next();
    } else {
        current->inode = record->inode;
    }

    current->ns_entry = ns_entry;
    current->stat.mode = record->stat.mode;
    current->stat.atime = record->stat.atime;
    current->stat.btime = record->stat.btime;
    current->stat.ctime = record->stat.ctime;
    current->stat.mtime = record->stat.mtime;
    current->stat.uid = record->stat.uid;
    current->stat.gid = record->stat.gid;
    current->stat.rdev = record->stat.rdev;
    current->stat.size = record->stat.size;
    current->stat.nlink = 1;
    current->stat.alloc = 0;
    current->stat.space_end = 0;

    if (STORAGE_ENABLED) {
        current->db_args->loaded_flags = FDIR_DENTRY_LOADED_FLAGS_ALL;
        dentry_lru_add(current);
    }

    if (FDIR_IS_DENTRY_HARD_LINK(current->stat.mode)) {
        current->src_dentry->stat.nlink++;
        AFFECTED_DENTRIES_ADD(record, current->src_dentry,
                da_binlog_op_type_update);
    } else {
        if ((result=inode_index_add_dentry(current)) != 0) {
            dentry_delay_free(current);
            return result;
        }
    }

    if (current->parent == NULL) {
        ns_entry->current.root.ptr = current;
    } else if ((result=uniq_skiplist_insert(current->
                    parent->children, current)) == 0)
    {
        current->parent->stat.nlink++;
    } else {
        logError("file: "__FILE__", line: %d, parent inode: %"PRId64", "
                "insert child {inode: %"PRId64", name: %.*s} to "
                "skiplist fail, errno: %d, error info: %s", __LINE__,
                current->parent->inode, current->inode, current->name.len,
                current->name.str, result, STRERROR(result));
        return result;
    }

    record->me.dentry = current;
    if (record->inode == 0) {
        record->inode = current->inode;
    }

    if (is_dir) {
        thread_ctx->dentry_context.counters.dir++;
        __sync_add_and_fetch(&ns_entry->current.counts.dir, 1);
    } else {
        thread_ctx->dentry_context.counters.file++;
        __sync_add_and_fetch(&ns_entry->current.counts.file, 1);
    }
    return 0;
}

static inline int remove_src_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *dentry)
{
    int result;

    if ((result=inode_index_del_dentry(dentry)) != 0) {
        return result;
    }

    if (S_ISDIR(dentry->stat.mode)) {
        __sync_sub_and_fetch(&dentry->ns_entry->current.counts.dir, 1);
        thread_ctx->dentry_context.counters.dir--;
    } else {
        __sync_sub_and_fetch(&dentry->ns_entry->current.counts.file, 1);
        thread_ctx->dentry_context.counters.file--;
    }

    dentry_delay_free(dentry);
    return 0;
}

static int do_remove_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record, FDIRServerDentry *dentry,
        bool *free_dentry)
{
    int result;
    DABinlogOpType op_type;

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        if (--(dentry->src_dentry->stat.nlink) == 0) {
            /*
               logInfo("file: "__FILE__", line: %d, "
               "remove hard link src dentry: %"PRId64, __LINE__,
               dentry->src_dentry->inode);
             */

            if ((result=remove_src_dentry(thread_ctx,
                            dentry->src_dentry)) != 0)
            {
                return result;
            }

            op_type = da_binlog_op_type_remove;
        } else {
            op_type = da_binlog_op_type_update;
        }

        AFFECTED_DENTRIES_ADD(record, dentry->src_dentry, op_type);
        AFFECTED_DENTRIES_ADD(record, dentry, da_binlog_op_type_remove);
        *free_dentry = true;
    } else {
        if (--(dentry->stat.nlink) == 0) {
            if ((result=inode_index_del_dentry(dentry)) != 0) {
                return result;
            }

            op_type = da_binlog_op_type_remove;
            *free_dentry = true;
        } else {
            /*
               logInfo("file: "__FILE__", line: %d, "
               "dentry: %"PRId64", nlink: %d > 0, skip remove",
               __LINE__, dentry->inode, dentry->stat.nlink);
             */

            op_type = da_binlog_op_type_update;
            *free_dentry = false;
        }
        AFFECTED_DENTRIES_ADD(record, dentry, op_type);
    }

    if (*free_dentry) {
        if (S_ISDIR(dentry->stat.mode)) {
            thread_ctx->dentry_context.counters.dir--;
            __sync_sub_and_fetch(&dentry->ns_entry->current.counts.dir, 1);
        } else {
            thread_ctx->dentry_context.counters.file--;
            __sync_sub_and_fetch(&dentry->ns_entry->current.counts.file, 1);
        }
    }

    return 0;
}

int dentry_remove(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    FDIRNamespaceEntry *ns_entry;
    bool free_dentry;
    int result;

    if ((result=dentry_find_me(thread_ctx, &record->ns,
                    &record->me, &ns_entry, false)) != 0)
    {
        return result;
    }

    if ((record->flags & FDIR_UNLINK_FLAGS_MATCH_ENABLED)) {
        if ((record->flags & FDIR_UNLINK_FLAGS_MATCH_DIR) ==
                FDIR_UNLINK_FLAGS_MATCH_DIR)
        {
            if (!S_ISDIR(record->me.dentry->stat.mode)) {
                return ENOTDIR;
            }
        } else if ((record->flags & FDIR_UNLINK_FLAGS_MATCH_FILE) ==
                FDIR_UNLINK_FLAGS_MATCH_FILE)
        {
            if (S_ISDIR(record->me.dentry->stat.mode)) {
                return EISDIR;
            }
        } else {
            return EINVAL;
        }
    }

    if (S_ISDIR(record->me.dentry->stat.mode)) {
        if (!uniq_skiplist_empty(record->me.dentry->children)) {
            return ENOTEMPTY;
        }
    }

    record->inode = record->me.dentry->inode;
    if ((result=do_remove_dentry(thread_ctx, record,
                    record->me.dentry, &free_dentry)) != 0)
    {
        return result;
    }

    if (record->me.parent == NULL) {
        ns_entry->current.root.ptr = NULL;
        if (free_dentry) {
            dentry_delay_free(record->me.dentry);
        }
    } else if ((result=uniq_skiplist_delete_ex(record->me.parent->
                    children, record->me.dentry, free_dentry)) == 0)
    {
        record->me.parent->stat.nlink--;
    } else {
        logError("file: "__FILE__", line: %d, parent inode: %"PRId64", "
                "delete child {inode: %"PRId64", name: %.*s} from "
                "skiplist fail, errno: %d, error info: %s", __LINE__,
                record->me.parent->inode, record->me.dentry->inode,
                record->me.dentry->name.len, record->me.dentry->name.str,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

static bool dentry_is_ancestor(FDIRServerDentry *dentry, FDIRServerDentry *parent)
{
    while (parent != NULL) {
        if (parent == dentry) {
            return true;
        }
        parent = parent->parent;
    }
    return false;
}

static int rename_check(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;

    if (record->rename.src.parent == NULL ||
            record->rename.dest.parent == NULL)
    {
        return EINVAL;
    }

    if ((result=find_child(thread_ctx, record->rename.src.parent,
                    &record->rename.src.pname.name,
                    &record->rename.src.dentry)) != 0)
    {
        return result;
    }

    if ((result=find_child(thread_ctx, record->rename.dest.parent,
                    &record->rename.dest.pname.name,
                    &record->rename.dest.dentry)) != 0)
    {
        if ((record->flags & RENAME_EXCHANGE) != 0) {
            return result;
        } else {
            return (result == ENOENT ? 0 : result);
        }
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "record->flags: %d, RENAME_NOREPLACE: %d, RENAME_EXCHANGE: %d",
            __LINE__, record->flags, RENAME_NOREPLACE, RENAME_EXCHANGE);
            */

    if ((record->flags & RENAME_NOREPLACE)) {
        return EEXIST;
    }
    if ((record->flags & RENAME_EXCHANGE)) {
        return 0;
    }

    if ((record->rename.dest.dentry->stat.mode & S_IFMT) !=
            (record->rename.src.dentry->stat.mode & S_IFMT))
    {
        return EINVAL;
    }

    if (S_ISDIR(record->rename.dest.dentry->stat.mode)) {
        if (!uniq_skiplist_empty(record->rename.dest.dentry->children)) {
            return ENOTEMPTY;
        }
    }

    return 0;
}

static inline void restore_dentry_name(FDIRServerDentry *dentry,
        string_t *old_name)
{
    char *name_to_free;

    name_to_free = dentry->name.str;
    dentry->name = *old_name;
    fast_allocator_free(&dentry->context->name_acontext, name_to_free);
}

static int set_and_store_dentry_name(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *dentry, const string_t *new_name,
        const bool name_changed, StringHolderPtrPair *pair)
{
    int result;
    string_t cloned_name;

    if (!name_changed) {
        pair->ptr = &dentry->name;
        return 0;
    }

    if ((result=dentry_strdup(dentry->context,
                    &cloned_name, new_name)) != 0)
    {
        return result;
    }

    pair->ptr = &pair->holder;
    pair->holder = dentry->name;
    dentry->name = cloned_name;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "dentry old name: %.*s, new name: %.*s",
            __LINE__, pair->ptr->len, pair->ptr->str,
            dentry->name.len, dentry->name.str);
            */

    return 0;
}

static int exchange_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record, const bool name_changed)
{
    int result;
    StringHolderPtrPair old_src_pair;
    StringHolderPtrPair old_dest_pair;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "src parent inode: %"PRId64", dest parent inode: %"PRId64,
            __LINE__, record->rename.dest.parent->inode,
            record->rename.src.parent->inode);
            */

    if ((result=uniq_skiplist_delete_ex(record->rename.src.parent->
                    children, record->rename.src.dentry, false)) != 0) {
        return result;
    }

    do {
        old_src_pair.ptr = NULL;
        if ((result=set_and_store_dentry_name(thread_ctx,
                        record->rename.src.dentry, &record->
                        rename.dest.pname.name, name_changed,
                        &old_src_pair)) != 0)
        {
            break;
        }

        if ((result=uniq_skiplist_replace_ex(record->rename.dest.parent->
                        children, record->rename.src.dentry, false)) != 0)
        {
            break;
        }

        if ((result=set_and_store_dentry_name(thread_ctx,
                        record->rename.dest.dentry, &record->
                        rename.src.pname.name, name_changed,
                        &old_dest_pair)) != 0)
        {
            uniq_skiplist_replace_ex(record->rename.dest.parent->
                    children, record->rename.dest.dentry, false);  //rollback
            break;
        }

        if ((result=uniq_skiplist_insert(record->rename.src.parent->
                        children, record->rename.dest.dentry)) != 0)
        {
            if (name_changed) {
                restore_dentry_name(record->rename.dest.dentry,
                        old_dest_pair.ptr);
            }

            uniq_skiplist_replace_ex(record->rename.dest.parent->
                    children, record->rename.dest.dentry, false);  //rollback
            break;
        }

        record->rename.src.dentry->parent = record->rename.dest.parent;
        record->rename.dest.dentry->parent = record->rename.src.parent;
        record->inode = record->rename.src.dentry->inode;
        if (name_changed) {
            dentry_strfree(record->rename.src.dentry->
                    context, old_src_pair.ptr);
            dentry_strfree(record->rename.dest.
                    dentry->context, old_dest_pair.ptr);
        }
    } while (0);

    if (result != 0) {  //rollback
        if (name_changed && old_src_pair.ptr != NULL) {
            restore_dentry_name(record->rename.src.dentry,
                    old_src_pair.ptr);
        }

        uniq_skiplist_insert(record->rename.src.parent->children,
                record->rename.src.dentry);
    }

    return result;
}

static int move_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record, const bool name_changed)
{
    int result;
    StringHolderPtrPair old_src_pair;

    if ((result=uniq_skiplist_delete_ex(record->rename.src.parent->
                    children, record->rename.src.dentry, false)) != 0) {
        return result;
    }

    do {
        if ((result=set_and_store_dentry_name(thread_ctx,
                        record->rename.src.dentry,
                        &record->rename.dest.pname.name,
                        name_changed, &old_src_pair)) != 0)
        {
            break;
        }

        /*
        logInfo("file: "__FILE__", line: %d, "
                "old src dentry name: %.*s, "
                "new src dentry name: %.*s, dest dentry: %p",
                __LINE__, old_src_pair.ptr->len,
                old_src_pair.ptr->str,
                record->rename.src.dentry->name.len,
                record->rename.src.dentry->name.str,
                record->rename.dest.dentry);
                */

        record->rename.overwritten = record->rename.dest.dentry;
        if (record->rename.dest.dentry != NULL) {
            bool free_dentry;
            if ((result=do_remove_dentry(thread_ctx, record, record->
                            rename.dest.dentry, &free_dentry)) == 0)
            {
                result = uniq_skiplist_replace_ex(record->rename.dest.parent->
                        children, record->rename.src.dentry, free_dentry);
            }
        } else {
            result = uniq_skiplist_insert(record->rename.dest.
                    parent->children, record->rename.src.dentry);
        }

        if (result != 0) {
            if (name_changed) {
                restore_dentry_name(record->rename.src.dentry,
                        old_src_pair.ptr);
            }
            break;
        }

        if (record->rename.overwritten != NULL) {
            record->rename.src.parent->stat.nlink--;
        } else {
            if (record->rename.dest.parent != record->rename.src.parent) {
                record->rename.src.parent->stat.nlink--;
                record->rename.dest.parent->stat.nlink++;
            }
        }

        record->rename.src.dentry->parent = record->rename.dest.parent;
        record->inode = record->rename.src.dentry->inode;
        if (name_changed) {
            dentry_strfree(record->rename.src.dentry->
                    context, old_src_pair.ptr);
        }
    } while (0);

    if (result != 0) {  //rollback
        uniq_skiplist_insert(record->rename.src.parent->children,
                record->rename.src.dentry);
    }

    return result;
}

int dentry_rename(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    bool name_changed;

    if ((result=rename_check(thread_ctx, record)) != 0) {
        return result;
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "record->flags: %d, dest.dentry: %p, src.dentry: %p",
            __LINE__, record->flags, record->rename.dest.dentry,
            record->rename.src.dentry);
            */

    if (record->rename.dest.dentry == record->rename.src.dentry) {
        return EEXIST;
    }

    if (record->rename.dest.parent != record->rename.src.parent) {
        if (dentry_is_ancestor(record->rename.src.dentry,
                    record->rename.dest.parent))
        {
            return ELOOP;
        }

        if (dentry_is_ancestor(record->rename.dest.dentry != NULL ?
                    record->rename.dest.dentry : record->rename.dest.parent,
                    record->rename.src.parent))
        {
            return ELOOP;
        }
    }

    name_changed = !fc_string_equal(&record->rename.dest.pname.name,
            &record->rename.src.pname.name);

    /*
    logInfo("new src dentry name: %.*s(%d), dest dentry name: %.*s(%d), rename_exchange: %d",
            record->rename.src.dentry->name.len, record->rename.src.dentry->name.str,
            record->rename.src.dentry->name.len,
            record->rename.dest.pname.name.len, record->rename.dest.pname.name.str,
            record->rename.dest.pname.name.len, (record->flags & RENAME_EXCHANGE));
            */

    if ((record->flags & RENAME_EXCHANGE)) {
        return exchange_dentry(thread_ctx, record, name_changed);
    } else {
        //dentry_children_print(record->rename.src.parent);
        return move_dentry(thread_ctx, record, name_changed);
    }
}

int dentry_find_ex(const FDIRDEntryFullName *fullname,
        FDIRServerDentry **dentry, const bool hdlink_follow)
{
    FDIRPathInfo path_info;
    FDIRNamespaceEntry *ns_entry;
    FDIRServerDentry *parent;
    string_t my_name;
    int result;

    if ((result=dentry_find_parent_and_me(fullname, &path_info,
                    &my_name, &ns_entry, &parent, dentry)) != 0)
    {
        return result;
    }

    if (hdlink_follow) {
        FDIR_SET_HARD_LINK_DENTRY(*dentry);
    }
    return 0;
}

int dentry_find_by_pname(FDIRServerDentry *parent, const string_t *name,
        FDIRServerDentry **dentry)
{
    int result;

    if ((result=find_child(parent->ns_entry->thread_ctx,
                    parent, name, dentry)) == 0)
    {
        FDIR_SET_HARD_LINK_DENTRY(*dentry);
    }
    return result;
}

int dentry_list(FDIRServerDentry *dentry, PointerArray **parray)
{
    FDIRServerDentry *current;
    FDIRServerDentry **pp;
    UniqSkiplistIterator iterator;
    int count;

    if (!S_ISDIR(dentry->stat.mode)) {
        count = 1;
    } else {
        count = uniq_skiplist_count(dentry->children);
    }

    if ((*parray=ptr_array_allocator_alloc(
                    &DENTRY_PARRAY_ALLOCATOR,
                    count)) == NULL)
    {
        return ENOMEM;
    }

    if (!S_ISDIR(dentry->stat.mode)) {
        (*parray)->elts[(*parray)->count++] = dentry;
    } else {
        pp = (FDIRServerDentry **)(*parray)->elts;
        uniq_skiplist_iterator(dentry->children, &iterator);
        while ((current=(FDIRServerDentry *)uniq_skiplist_next(&iterator)) != NULL) {
            *pp++ = current;
        }
        (*parray)->count = pp - (FDIRServerDentry **)(*parray)->elts;
    }

    return 0;
}

int dentry_get_full_path(const FDIRServerDentry *dentry,
        BufferInfo *full_path, SFErrorInfo *error_info)
{
    FDIRServerDentry *current;
    string_t *parts[FDIR_MAX_PATH_COUNT];
    char *p;
    int count;
    int i;

    count = 0;
    current = (FDIRServerDentry *)dentry;
    while (current->parent != NULL && count < FDIR_MAX_PATH_COUNT) {
        parts[count++] = &current->name;
        current = current->parent;
    }
    if (count == FDIR_MAX_PATH_COUNT && current->parent != NULL) {
        error_info->length = sprintf(error_info->message,
                "the depth of path exceeds %d", FDIR_MAX_PATH_COUNT);
        return EOVERFLOW;
    }

    p = full_path->buff;
    for (i=count-1; i>=0; i--) {
        if ((p - full_path->buff) + parts[i]->len + 4 >
                full_path->alloc_size)
        {
            error_info->length = sprintf(error_info->message,
                "path length exceeds buff size: %d",
                full_path->alloc_size);
            return ENOSPC;
        }

        *p++ = '/';
        memcpy(p, parts[i]->str, parts[i]->len);
        p += parts[i]->len;
    }

    if (S_ISDIR(dentry->stat.mode)) {
        *p++ = '/';
    }

    *p = '\0';
    full_path->length = p - full_path->buff;
    return 0;
}

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
#include "service_handler.h"
#include "inode_generator.h"
#include "inode_index.h"
#include "ns_manager.h"
#include "dentry.h"

#define INIT_LEVEL_COUNT 2

typedef struct {
    string_t holder;
    string_t *ptr;
} StringHolderPtrPair;

const int max_level_count = 20;

#define SET_HARD_LINK_DENTRY(dentry)  \
    do { \
        if (FDIR_IS_DENTRY_HARD_LINK((dentry)->stat.mode)) {  \
            dentry = (dentry)->src_dentry;  \
        } \
    } while (0)


int dentry_init()
{
    int result;

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
    UniqSkiplistIterator iterator;
    int i = 0;

    if (dentry->children == NULL) {
        logInfo("not skiplist");
        return;
    }

    uniq_skiplist_iterator(dentry->children, &iterator);
    while ((current=(FDIRServerDentry *)uniq_skiplist_next(&iterator)) != NULL) {
        logInfo("%d. %.*s(%d)", ++i, current->name.len,
                current->name.str, current->name.len);
    }
}
*/

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

    if ((allocator=dentry_get_kvarray_allocator_by_capacity(
                    dentry->context, dentry->kv_array->alloc)) != NULL)
    {
        fast_mblock_free_object(allocator, dentry->kv_array);
    }

    dentry->kv_array = NULL;
    dentry->kv_array->count = 0;
}

static void dentry_do_free(void *ptr)
{
    FDIRServerDentry *dentry;
    dentry = (FDIRServerDentry *)ptr;

    if (dentry->children != NULL) {
        uniq_skiplist_free(dentry->children);
    }

    fast_allocator_free(&dentry->context->name_acontext, dentry->name.str);
    if ((!FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode) &&
            S_ISLNK(dentry->stat.mode)) && dentry->link.str != NULL)
    {
        fast_allocator_free(&dentry->context->name_acontext,
                dentry->link.str);
    }
    dentry_free_xattrs(dentry);
    fast_mblock_free_object(&dentry->context->dentry_allocator, dentry);
}

static void dentry_free_func(void *ptr, const int delay_seconds)
{
    FDIRServerDentry *dentry;
    dentry = (FDIRServerDentry *)ptr;

    if (delay_seconds > 0) {
        server_add_to_delay_free_queue(&dentry->context->db_context->
                delay_free_context, ptr, dentry_do_free, delay_seconds);
    } else {
        dentry_do_free(ptr);
    }
}

int dentry_init_obj(void *element, void *init_args)
{
    FDIRServerDentry *dentry;
    dentry = (FDIRServerDentry *)element;
    dentry->context = (FDIRDentryContext *)init_args;
    return 0;
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
                        alloc_elements_once, 0, (fast_mblock_alloc_init_func)
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

int dentry_init_context(FDIRDataThreadContext *db_context)
{
    FDIRDentryContext *context;
    int result;

    context = &db_context->dentry_context;
    context->db_context = db_context;
    if ((result=uniq_skiplist_init_ex(&context->factory,
                    max_level_count, dentry_compare, dentry_free_func,
                    16 * 1024, SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE,
                    FDIR_DELAY_FREE_SECONDS)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&context->dentry_allocator,
                    "dentry", sizeof(FDIRServerDentry), 8 * 1024,
                    0, dentry_init_obj, context, false)) != 0)
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

    return 0;
}

static const FDIRServerDentry *do_find_ex(FDIRNamespaceEntry *ns_entry,
        const string_t *paths, const int count)
{
    const string_t *p;
    const string_t *end;
    FDIRServerDentry *current;
    FDIRServerDentry target;

    current = ns_entry->dentry_root;
    end = paths + count;
    for (p=paths; p<end; p++) {
        if (!S_ISDIR(current->stat.mode)) {
            return NULL;
        }

        target.name = *p;
        current = (FDIRServerDentry *)uniq_skiplist_find(
                current->children, &target);
        if (current == NULL) {
            return NULL;
        }
    }

    return current;
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

    if (ns_entry->dentry_root == NULL) {
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
        *parent = ns_entry->dentry_root;
    } else {
        *parent = (FDIRServerDentry *)do_find_ex(ns_entry,
                path_info.paths, path_info.count - 1);
        if (*parent == NULL) {
            return ENOENT;
        }
    }

    if (!S_ISDIR((*parent)->stat.mode)) {
        *parent = NULL;
        return ENOTDIR;
    }

    return 0;
}

static int dentry_find_parent_and_me(FDIRDentryContext *context,
        const FDIRDEntryFullName *fullname, FDIRPathInfo *path_info,
        string_t *my_name, FDIRNamespaceEntry **ns_entry,
        FDIRServerDentry **parent, FDIRServerDentry **me,
        const bool create_ns)
{
    FDIRServerDentry target;
    int result;

    if (fullname->path.len == 0 || fullname->path.str[0] != '/') {
        *ns_entry = NULL;
        *parent = *me = NULL;
        return EINVAL;
    }

    *ns_entry = fdir_namespace_get(context, &fullname->ns,
            create_ns, &result);
    if (*ns_entry == NULL) {
        *parent = *me = NULL;
        return result;
    }

    if ((*ns_entry)->dentry_root == NULL) {
        *parent = *me = NULL;
        return ENOENT;
    }

    path_info->count = split_string_ex(&fullname->path, '/',
            path_info->paths, FDIR_MAX_PATH_COUNT, true);
    if (path_info->count == 0) {
        *parent = NULL;
        *me = (*ns_entry)->dentry_root;
        my_name->len = 0;
        my_name->str = "";
        return 0;
    }

    *my_name = path_info->paths[path_info->count - 1];
    if (path_info->count == 1) {
        *parent = (*ns_entry)->dentry_root;
    } else {
        *parent = (FDIRServerDentry *)do_find_ex(*ns_entry,
                path_info->paths, path_info->count - 1);
        if (*parent == NULL) {
            *me = NULL;
            return ENOENT;
        }
    }

    if (!S_ISDIR((*parent)->stat.mode)) {
        *parent = NULL;
        *me = NULL;
        return ENOTDIR;
    }

    target.name = *my_name;
    *me = (FDIRServerDentry *)uniq_skiplist_find((*parent)->children, &target);
    return 0;
}

static int dentry_find_me(FDIRDentryContext *context, const string_t *ns,
        FDIRRecordDEntry *rec_entry, FDIRNamespaceEntry **ns_entry,
        const bool create_ns)
{
    FDIRServerDentry target;
    int result;

    if (rec_entry->parent == NULL) {
        *ns_entry = fdir_namespace_get(context, ns, create_ns, &result);
        if (*ns_entry == NULL) {
            return result;
        }

        if ((*ns_entry)->dentry_root == NULL) {
            return ENOENT;
        }

        if (rec_entry->pname.name.len == 0) {
            rec_entry->dentry = (*ns_entry)->dentry_root;
            return 0;
        } else {
            return ENOENT;
        }
    } else {
        *ns_entry = rec_entry->parent->ns_entry;
    }

    target.name = rec_entry->pname.name;
    rec_entry->dentry = (FDIRServerDentry *)uniq_skiplist_find(
            rec_entry->parent->children, &target);
    return 0;
}

int dentry_create(FDIRDataThreadContext *db_context, FDIRBinlogRecord *record)
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

    if ((result=dentry_find_me(&db_context->dentry_context, &record->ns,
                    &record->me, &ns_entry, true)) != 0)
    {
        bool is_root_path;
        is_root_path = (record->me.parent == NULL &&
                record->me.pname.name.len == 0);
        if (!(is_root_path && ns_entry != NULL && result == ENOENT)) {
            return result;
        }
    }

    if (record->me.dentry != NULL) {
        return EEXIST;
    }

    current = (FDIRServerDentry *)fast_mblock_alloc_object(
            &db_context->dentry_context.dentry_allocator);
    if (current == NULL) {
        return ENOMEM;
    }

    is_dir = S_ISDIR(record->stat.mode);
    if (is_dir) {
        current->children = uniq_skiplist_new(&db_context->
                dentry_context.factory, INIT_LEVEL_COUNT);
        if (current->children == NULL) {
            return ENOMEM;
        }
    } else {
        current->children = NULL;
    }

    current->parent = record->me.parent;
    if ((result=dentry_strdup(&db_context->dentry_context,
                    &current->name, &record->me.pname.name)) != 0)
    {
        return result;
    }

    if (FDIR_IS_DENTRY_HARD_LINK(record->stat.mode)) {
        current->src_dentry = record->hdlink.src_dentry;
    } else if (S_ISLNK(record->stat.mode)) {
        if ((result=dentry_strdup(&db_context->dentry_context,
                        &current->link, &record->link)) != 0)
        {
            return result;
        }
    } else {
        FC_SET_STRING_NULL(current->link);
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
    current->stat.size = record->stat.size;
    current->stat.nlink = 1;
    current->stat.alloc = 0;
    current->stat.space_end = 0;

    if (FDIR_IS_DENTRY_HARD_LINK(current->stat.mode)) {
        current->src_dentry->stat.nlink++;
    } else {
        if ((result=inode_index_add_dentry(current)) != 0) {
            dentry_do_free(current);
            return result;
        }
    }

    if (current->parent == NULL) {
        ns_entry->dentry_root = current;
    } else if ((result=uniq_skiplist_insert(current->parent->children,
                    current)) == 0)
    {
        current->parent->stat.nlink++;
    } else {
        return result;
    }

    record->me.dentry = current;
    if (record->inode == 0) {
        record->inode = current->inode;
    }

    if (is_dir) {
        db_context->dentry_context.counters.dir++;
    } else {
        db_context->dentry_context.counters.file++;
    }
    __sync_add_and_fetch(&ns_entry->dentry_count, 1);
    return 0;
}

static inline int remove_src_dentry(FDIRDataThreadContext *db_context,
        FDIRServerDentry *dentry)
{
    int result;

    if ((result=inode_index_del_dentry(dentry)) != 0) {
        return result;
    }

    dentry_free_func(dentry, FDIR_DELAY_FREE_SECONDS);
    db_context->dentry_context.counters.file--;
    __sync_sub_and_fetch(&dentry->ns_entry->dentry_count, 1);
    return 0;
}

static int do_remove_dentry(FDIRDataThreadContext *db_context,
        FDIRServerDentry *dentry, bool *free_dentry)
{
    int result;

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        if (--(dentry->src_dentry->stat.nlink) == 0) {
            /*
               logInfo("file: "__FILE__", line: %d, "
               "remove hard link src dentry: %"PRId64, __LINE__,
               dentry->src_dentry->inode);
             */

            if ((result=remove_src_dentry(db_context,
                            dentry->src_dentry)) != 0)
            {
                return result;
            }
        }
        *free_dentry = true;
    } else {
        if (--(dentry->stat.nlink) == 0) {
            if ((result=inode_index_del_dentry(dentry)) != 0) {
                return result;
            }

            *free_dentry = true;
        } else {
            /*
            logInfo("file: "__FILE__", line: %d, "
                    "dentry: %"PRId64", nlink: %d > 0, skip remove",
                    __LINE__, dentry->inode, dentry->stat.nlink);
                    */
            *free_dentry = false;
        }
    }

    if (*free_dentry) {
        if (S_ISDIR(dentry->stat.mode)) {
            db_context->dentry_context.counters.dir--;
        } else {
            db_context->dentry_context.counters.file--;
        }

        __sync_sub_and_fetch(&dentry->ns_entry->dentry_count, 1);
    }

    return 0;
}

int dentry_remove(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    FDIRNamespaceEntry *ns_entry;
    bool free_dentry;
    int result;

    if ((result=dentry_find_me(&db_context->dentry_context, &record->ns,
                    &record->me, &ns_entry, false)) != 0)
    {
        return result;
    }

    if (record->me.dentry == NULL) {
        return ENOENT;
    }

    if (S_ISDIR(record->me.dentry->stat.mode)) {
        if (!uniq_skiplist_empty(record->me.dentry->children)) {
            return ENOTEMPTY;
        }
    }

    record->inode = record->me.dentry->inode;
    if ((result=do_remove_dentry(db_context, record->me.
                    dentry, &free_dentry)) != 0)
    {
        return result;
    }

    if (record->me.parent == NULL) {
        ns_entry->dentry_root = NULL;
    } else if ((result=uniq_skiplist_delete_ex(record->me.parent->children,
                    record->me.dentry, free_dentry)) == 0)
    {
        record->me.parent->stat.nlink--;
    } else {
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

static int rename_check(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    FDIRServerDentry target;

    if (record->rename.src.parent == NULL ||
            record->rename.dest.parent == NULL)
    {
        return EINVAL;
    }

    target.name = record->rename.src.pname.name;
    if ((record->rename.src.dentry=(FDIRServerDentry *)uniq_skiplist_find(
                    record->rename.src.parent->children, &target)) == NULL)
    {
        return ENOENT;
    }

    target.name = record->rename.dest.pname.name;
    if ((record->rename.dest.dentry=(FDIRServerDentry *)uniq_skiplist_find(
                    record->rename.dest.parent->children, &target)) == NULL)
    {
        if ((record->flags & RENAME_EXCHANGE)) {
            return ENOENT;
        }

        return 0;
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

static inline void free_dname(FDIRServerDentry *dentry, string_t *old_name)
{
    server_delay_free_str(dentry->context, old_name->str);
}

static inline void restore_dentry_name(FDIRServerDentry *dentry,
        string_t *old_name)
{
    char *name_to_free;

    name_to_free = dentry->name.str;
    dentry->name = *old_name;
    server_delay_free_str(dentry->context, name_to_free);
}

static int set_and_store_dentry_name(FDIRDataThreadContext *db_context,
        FDIRServerDentry *dentry, const string_t *new_name,
        const bool name_changed, StringHolderPtrPair *pair)
{
    int result;
    string_t cloned_name;

    if (!name_changed) {
        pair->ptr = &dentry->name;
        return 0;
    }

    if ((result=dentry_strdup(&dentry->context->db_context->
                    dentry_context, &cloned_name, new_name)) != 0)
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

static int exchange_dentry(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record, const bool name_changed)
{
    int result;
    StringHolderPtrPair old_src_pair;
    StringHolderPtrPair old_dest_pair;

    if (record->rename.dest.parent == record->rename.src.parent) {
        record->inode = record->rename.src.dentry->inode;
        return 0;
    }

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
        if ((result=set_and_store_dentry_name(db_context,
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

        if ((result=set_and_store_dentry_name(db_context,
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
            free_dname(record->rename.src.dentry, old_src_pair.ptr);
            free_dname(record->rename.dest.dentry, old_dest_pair.ptr);
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

static int move_dentry(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record, const bool name_changed)
{
    int result;
    StringHolderPtrPair old_src_pair;

    if ((result=uniq_skiplist_delete_ex(record->rename.src.parent->
                    children, record->rename.src.dentry, false)) != 0) {
        return result;
    }

    do {
        if ((result=set_and_store_dentry_name(db_context, record->rename.src.dentry,
                        &record->rename.dest.pname.name, name_changed,
                        &old_src_pair)) != 0)
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
            if ((result=do_remove_dentry(db_context, record->rename.
                            dest.dentry, &free_dentry)) == 0)
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

        record->rename.src.dentry->parent = record->rename.dest.parent;
        record->inode = record->rename.src.dentry->inode;
        if (name_changed) {
            free_dname(record->rename.src.dentry, old_src_pair.ptr);
        }
    } while (0);

    if (result != 0) {  //rollback
        uniq_skiplist_insert(record->rename.src.parent->children,
                record->rename.src.dentry);
    }

    return result;
}

int dentry_rename(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    int result;
    bool name_changed;

    if ((result=rename_check(db_context, record)) != 0) {
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
        return exchange_dentry(db_context, record, name_changed);
    } else {
        //dentry_children_print(record->rename.src.parent);
        return move_dentry(db_context, record, name_changed);
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

    if ((result=dentry_find_parent_and_me(NULL, fullname, &path_info,
                    &my_name, &ns_entry, &parent, dentry, false)) != 0)
    {
        return result;
    }

    if (*dentry == NULL) {
        return ENOENT;
    }

    if (hdlink_follow) {
        SET_HARD_LINK_DENTRY(*dentry);
    }
    return 0;
}

int dentry_find_by_pname(FDIRServerDentry *parent, const string_t *name,
        FDIRServerDentry **dentry)
{
    FDIRServerDentry target;

    if (!S_ISDIR(parent->stat.mode)) {
        *dentry = NULL;
        return ENOENT;
    }

    target.name = *name;
    if ((*dentry=(FDIRServerDentry *)uniq_skiplist_find(
                    parent->children, &target)) != NULL)
    {
        SET_HARD_LINK_DENTRY(*dentry);
        return 0;
    } else {
        return ENOENT;
    }
}

static int check_alloc_dentry_array(FDIRServerDentryArray *array, const int target_count)
{
    FDIRServerDentry **entries;
    int new_alloc;
    int bytes;

    if (array->alloc >= target_count) {
        return 0;
    }

    new_alloc = (array->alloc > 0) ? array->alloc : 4 * 1024;
    while (new_alloc < target_count) {
        new_alloc *= 2;
    }

    bytes = sizeof(FDIRServerDentry *) * new_alloc;
    entries = (FDIRServerDentry **)fc_malloc(bytes);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (array->entries != NULL) {
        if (array->count > 0) {
            memcpy(entries, array->entries,
                    sizeof(FDIRServerDentry *) * array->count);
        }
        free(array->entries);
    }

    array->alloc = new_alloc;
    array->entries = entries;
    return 0;
}

int dentry_list(FDIRServerDentry *dentry, FDIRServerDentryArray *array)
{
    FDIRServerDentry *current;
    FDIRServerDentry **pp;
    UniqSkiplistIterator iterator;
    int result;
    int count;

    if (!S_ISDIR(dentry->stat.mode)) {
        count = 1;
    } else {
        count = uniq_skiplist_count(dentry->children);
    }

    if ((result=check_alloc_dentry_array(array, count)) != 0) {
        return result;
    }

    if (!S_ISDIR(dentry->stat.mode)) {
        array->entries[array->count++] = dentry;
    } else {
        pp = array->entries;
        uniq_skiplist_iterator(dentry->children, &iterator);
        while ((current=(FDIRServerDentry *)uniq_skiplist_next(&iterator)) != NULL) {
            *pp++ = current;
        }
        array->count = pp - array->entries;
    }

    return 0;
}

int dentry_get_full_path(const FDIRServerDentry *dentry, BufferInfo *full_path,
        SFErrorInfo *error_info)
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
        if ((p - full_path->buff) + parts[i]->len + 2 > full_path->alloc_size) {
            error_info->length = sprintf(error_info->message,
                "path length exceeds buff size: %d",
                full_path->alloc_size);
            return ENOSPC;
        }

        *p++ = '/';
        memcpy(p, parts[i]->str, parts[i]->len);
        p += parts[i]->len;
    }

    *p = '\0';
    full_path->length = p - full_path->buff;
    return 0;
}

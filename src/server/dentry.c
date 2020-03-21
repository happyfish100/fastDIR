
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
#include "dentry.h"

#define INIT_LEVEL_COUNT 2

typedef struct fdir_namespace_entry {
    string_t name;
    FDIRServerDentry dentry_root;
    struct fdir_namespace_entry *next;  //for hashtable
} FDIRNamespaceEntry;

typedef struct fdir_namespace_hashtable {
    int count;
    FDIRNamespaceEntry **buckets;
    struct fast_mblock_man allocator;
    pthread_mutex_t lock;  //for create namespace
} FDIRNamespaceHashtable;

typedef struct fdir_manager {
    FDIRNamespaceHashtable hashtable;
} FDIRManager;

const int max_level_count = 20;
//const int delay_free_seconds = 3600;
const int delay_free_seconds = 60;
static FDIRManager fdir_manager;

#define dentry_strdup_ex(context, dest, src, len) \
    fast_allocator_alloc_string_ex(&(context)->name_acontext, dest, src, len)

#define dentry_strdup(context, dest, src) \
    fast_allocator_alloc_string(&(context)->name_acontext, dest, src)

int dentry_init()
{

    int result;
    int bytes;

    memset(&fdir_manager, 0, sizeof(fdir_manager));

    if ((result=fast_mblock_init(&fdir_manager.hashtable.allocator,
                    sizeof(FDIRNamespaceEntry), 4096)) != 0)
    {
        return result;
    }

    fdir_manager.hashtable.count = 0;
    bytes = sizeof(FDIRNamespaceEntry *) * g_server_global_vars.
        namespace_hashtable_capacity;
    fdir_manager.hashtable.buckets = (FDIRNamespaceEntry **)malloc(bytes);
    if (fdir_manager.hashtable.buckets == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(fdir_manager.hashtable.buckets, 0, bytes);

    if ((result=init_pthread_lock(&fdir_manager.hashtable.lock)) != 0) {
        return result;
    }

    return 0;
}

void dentry_destroy()
{
}

static int dentry_compare(const void *p1, const void *p2)
{
    return fc_string_compare(&((FDIRServerDentry *)p1)->name,
            &((FDIRServerDentry *)p2)->name);
}

static void dentry_do_free(void *ptr)
{
    FDIRServerDentry *dentry;
    dentry = (FDIRServerDentry *)ptr;

    if (dentry->children != NULL) {
        uniq_skiplist_free(dentry->children);
    }

    fast_allocator_free(&dentry->context->name_acontext, dentry->name.str);
    fast_mblock_free_object(&dentry->context->dentry_allocator,
            (void *)dentry);
}

static void dentry_free_func(void *ptr, const int delay_seconds)
{
    FDIRServerDentry *dentry;
    dentry = (FDIRServerDentry *)ptr;

    if (delay_seconds > 0) {
        server_add_to_delay_free_queue(&dentry->context->db_context->
                delay_free_context, ptr, dentry_do_free, delay_free_seconds);
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

int dentry_init_context(FDIRDataThreadContext *db_context)
{
#define NAME_REGION_COUNT 4

    FDIRDentryContext *context;
    struct fast_region_info regions[NAME_REGION_COUNT];
    int count;
    int result;

    context = &db_context->dentry_context;
    context->db_context = db_context;
    if ((result=uniq_skiplist_init_ex(&context->factory,
                    max_level_count, dentry_compare, dentry_free_func,
                    16 * 1024, SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE,
                    delay_free_seconds)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex(&context->dentry_allocator,
                    sizeof(FDIRServerDentry), 8 * 1024,
                    dentry_init_obj, context, false)) != 0)
    {
        return result;
    }

    FAST_ALLOCATOR_INIT_REGION(regions[0], 0, 64, 4, 8 * 1024);
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

    if ((result=fast_allocator_init_ex(&context->name_acontext,
                    regions, count, 0, 0.00, 0, false)) != 0)
    {
        return result;
    }

    return 0;
}

static FDIRNamespaceEntry *create_namespace(FDIRDentryContext *context,
        FDIRNamespaceEntry **bucket, const string_t *ns, int *err_no)
{
    FDIRNamespaceEntry *entry;

    entry = (FDIRNamespaceEntry *)fast_mblock_alloc_object(
            &fdir_manager.hashtable.allocator);
    if (entry == NULL) {
        *err_no = ENOMEM;
        return NULL;
    }

    if ((*err_no=dentry_strdup(context, &entry->name, ns)) != 0) {
        return NULL;
    }

    if ((*err_no=dentry_strdup_ex(context, &entry->dentry_root.name,
                    "/", 1)) != 0)
    {
        return NULL;
    }
    entry->dentry_root.stat.mode |= S_IFDIR;
    entry->dentry_root.children = uniq_skiplist_new(&context->factory,
            INIT_LEVEL_COUNT);
    if (entry->dentry_root.children == NULL) {
        *err_no = ENOMEM;
        return NULL;
    }

    /*
    logInfo("ns: %.*s, create_namespace: %.*s", ns->len, ns->str,
            entry->name.len, entry->name.str);
            */

    entry->next = *bucket;
    *bucket = entry;
    *err_no = 0;

    context->counters.ns++;
    return entry;
}

static FDIRNamespaceEntry *get_namespace(FDIRDentryContext *context,
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
        entry = entry->next;
    }

    if (entry != NULL) {
        return entry;
    }
    if (!create_ns) {
        *err_no = ENOENT;
        return NULL;
    }

    pthread_mutex_lock(&fdir_manager.hashtable.lock);
    entry = *bucket;
    while (entry != NULL && !fc_string_equal(ns, &entry->name)) {
        entry = entry->next;
    }

    if (entry == NULL) {
        entry = create_namespace(context, bucket, ns, err_no);
    } else {
        *err_no = 0;
    }
    pthread_mutex_unlock(&fdir_manager.hashtable.lock);

    return entry;
}

static const FDIRServerDentry *dentry_find_ex(FDIRNamespaceEntry *ns_entry,
        const string_t *paths, const int count)
{
    const string_t *p;
    const string_t *end;
    FDIRServerDentry *current;
    FDIRServerDentry target;

    current = &ns_entry->dentry_root;
    end = paths + count;
    for (p=paths; p<end; p++) {
        if ((current->stat.mode & S_IFDIR) == 0) {
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

static int dentry_find_parent_and_me(FDIRDentryContext *context,
        const FDIRDEntryFullName *fullname, FDIRPathInfo *path_info,
        string_t *my_name, FDIRServerDentry **parent, FDIRServerDentry **me,
        const bool create_ns)
{
    FDIRServerDentry target;
    FDIRNamespaceEntry *ns_entry;
    int result;

    if (fullname->path.len == 0 || fullname->path.str[0] != '/') {
        *parent = *me = NULL;
        my_name->len = 0;
        my_name->str = NULL;
        return EINVAL;
    }

    if ((ns_entry=get_namespace(context, &fullname->ns,
                    create_ns, &result)) == NULL)
    {
        *parent = *me = NULL;
        return result;
    }

    path_info->count = split_string_ex(&fullname->path, '/',
        path_info->paths, FDIR_MAX_PATH_COUNT, true);
    if (path_info->count == 0) {
        *parent = NULL;
        *me = &ns_entry->dentry_root;
        my_name->len = 0;
        my_name->str = "";
        return 0;
    }

    *my_name = path_info->paths[path_info->count - 1];
    if (path_info->count == 1) {
        *parent = &ns_entry->dentry_root;
    } else {
        *parent = (FDIRServerDentry *)dentry_find_ex(ns_entry,
                path_info->paths, path_info->count - 1);
        if (*parent == NULL) {
            *me = NULL;
            return ENOENT;
        }
        if (((*parent)->stat.mode & S_IFDIR) == 0) {
            *me = NULL;
            return ENOENT;
        }
    }

    target.name = *my_name;
    *me = (FDIRServerDentry *)uniq_skiplist_find((*parent)->children, &target);
    return 0;
}

int dentry_create(FDIRDataThreadContext *db_context, FDIRBinlogRecord *record)
{
    FDIRPathInfo path_info;
    FDIRServerDentry *parent;
    FDIRServerDentry *current;
    string_t my_name;
    bool is_dir;
    int result;

    if ((record->stat.mode & S_IFMT) == 0) {
        logError("file: "__FILE__", line: %d, "
                "invalid file mode: %d", __LINE__, record->stat.mode);
        return EINVAL;
    }

    if ((result=dentry_find_parent_and_me(&db_context->dentry_context,
                    &record->fullname, &path_info, &my_name, &parent,
                    &current, true)) != 0)
    {
        return result;
    }

    if (path_info.count == 0) {
        return EEXIST;
    }

    if (parent == NULL) {
        return ENOENT;
    }
    if (current != NULL) {
        return EEXIST;
    }

    if (uniq_skiplist_count(parent->children) >= MAX_ENTRIES_PER_PATH) {
        char *parent_end;
        parent_end = (char *)fc_memrchr(record->fullname.path.str, '/',
                record->fullname.path.len);
        logError("file: "__FILE__", line: %d, "
                "too many entries in path %.*s, exceed %d",
                __LINE__, (int)(parent_end - record->fullname.path.str),
                record->fullname.path.str, MAX_ENTRIES_PER_PATH);
        return ENOSPC;
    }
    
    current = (FDIRServerDentry *)fast_mblock_alloc_object(
            &db_context->dentry_context.dentry_allocator);
    if (current == NULL) {
        return ENOMEM;
    }

    is_dir = (record->stat.mode & S_IFDIR) != 0;
    if (is_dir) {
        current->children = uniq_skiplist_new(&db_context->
                dentry_context.factory, INIT_LEVEL_COUNT);
        if (current->children == NULL) {
            return ENOMEM;
        }
    } else {
        current->children = NULL;
    }

    if ((result=dentry_strdup(&db_context->dentry_context,
                    &current->name, &my_name)) != 0)
    {
        return result;
    }

    if (record->inode == 0) {
        current->inode = inode_generator_next();
    } else {
        current->inode = record->inode;
    }
    current->stat.mode = record->stat.mode;
    current->stat.ctime = record->stat.ctime;
    current->stat.mtime = record->stat.mtime;
    current->stat.size = record->stat.size;
    if ((result=uniq_skiplist_insert(parent->children, current)) != 0) {
        return result;
    }

    if (record->inode == 0) {
        record->inode = current->inode;
    }

    if (is_dir) {
        db_context->dentry_context.counters.dir++;
    } else {
        db_context->dentry_context.counters.file++;
    }
    return 0;
}

int dentry_remove(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    FDIRPathInfo path_info;
    FDIRServerDentry *parent;
    FDIRServerDentry *current;
    string_t my_name;
    bool is_dir;
    int result;

    if ((result=dentry_find_parent_and_me(&db_context->dentry_context,
                    &record->fullname, &path_info, &my_name, &parent,
                    &current, false)) != 0)
    {
        return result;
    }

    if (current == NULL) {
        return ENOENT;
    }

    if ((current->stat.mode & S_IFDIR) != 0) {
        if (uniq_skiplist_count(current->children) > 0) {
            return ENOTEMPTY;
        }
        is_dir = true;
    } else {
        is_dir = false;
    }

    record->inode = current->inode;
    if ((result=uniq_skiplist_delete(parent->children, current)) != 0) {
        return result;
    }

    if (is_dir) {
        db_context->dentry_context.counters.dir--;
    } else {
        db_context->dentry_context.counters.file--;
    }
    return result;
}

int dentry_find(const FDIRDEntryFullName *fullname, FDIRServerDentry **dentry)
{
    FDIRPathInfo path_info;
    FDIRServerDentry *parent;
    string_t my_name;
    int result;

    if ((result=dentry_find_parent_and_me(NULL, fullname, &path_info,
                    &my_name, &parent, dentry, false)) != 0)
    {
        return result;
    }

    if (*dentry == NULL) {
        return ENOENT;
    }

    return 0;
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
    entries = (FDIRServerDentry **)malloc(bytes);
    if (entries == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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

int dentry_list(const FDIRDEntryFullName *fullname,
        FDIRServerDentryArray *array)
{
    FDIRServerDentry *dentry;
    FDIRServerDentry *current;
    FDIRServerDentry **pp;
    UniqSkiplistIterator iterator;
    int result;
    int count;

    array->count = 0;
    if ((result=dentry_find(fullname, &dentry)) != 0) {
        return result;
    }

    if ((dentry->stat.mode & S_IFDIR) == 0) {
        count = 1;
    } else {
        count = uniq_skiplist_count(dentry->children);
    }

    if ((result=check_alloc_dentry_array(array, count)) != 0) {
        return result;
    }

    if ((dentry->stat.mode & S_IFDIR) == 0) {
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


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
#include "dentry.h"

#define INIT_LEVEL_COUNT 2

typedef struct fdir_namespace_entry {
    string_t name;
    FDIRServerDentry *dentry_root;
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
    if ((result=fast_mblock_init_ex2(&fdir_manager.hashtable.allocator,
                    "ns_htable", sizeof(FDIRNamespaceEntry), 4096,
                    NULL, NULL, true, NULL, NULL, NULL)) != 0)
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

    return inode_index_init();
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

    if ((result=fast_mblock_init_ex2(&context->dentry_allocator,
                    "dentry", sizeof(FDIRServerDentry), 8 * 1024,
                    dentry_init_obj, context, false, NULL, NULL, NULL)) != 0)
    {
        return result;
    }

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

    if ((result=fast_allocator_init_ex(&context->name_acontext,
                    "name", regions, count, 0, 0.00, 0, false)) != 0)
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

    /*
    logInfo("ns: %.*s, create_namespace: %.*s", ns->len, ns->str,
            entry->name.len, entry->name.str);
            */

    entry->dentry_root = NULL;
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

    PTHREAD_MUTEX_LOCK(&fdir_manager.hashtable.lock);
    entry = *bucket;
    while (entry != NULL && !fc_string_equal(ns, &entry->name)) {
        entry = entry->next;
    }

    if (entry == NULL) {
        entry = create_namespace(context, bucket, ns, err_no);
    } else {
        *err_no = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_manager.hashtable.lock);

    return entry;
}

static const FDIRServerDentry *dentry_find_ex(FDIRNamespaceEntry *ns_entry,
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

    ns_entry = get_namespace(NULL, &fullname->ns, false, &result);
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
        *parent = (FDIRServerDentry *)dentry_find_ex(ns_entry,
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

    *ns_entry = get_namespace(context, &fullname->ns,
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
        *parent = (FDIRServerDentry *)dentry_find_ex(*ns_entry,
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
        *ns_entry = get_namespace(context, ns, create_ns, &result);
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
        *ns_entry = NULL;
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

    if ((record->stat.mode & S_IFMT) == 0) {
        logError("file: "__FILE__", line: %d, "
                "invalid file mode: %d", __LINE__, record->stat.mode);
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

    if (record->inode == 0) {
        current->inode = inode_generator_next();
    } else {
        current->inode = record->inode;
    }

    current->stat.mode = record->stat.mode;
    current->stat.atime = record->stat.atime;
    current->stat.ctime = record->stat.ctime;
    current->stat.mtime = record->stat.mtime;
    current->stat.uid = record->stat.uid;
    current->stat.gid = record->stat.gid;
    current->stat.size = record->stat.size;

    if ((result=inode_index_add_dentry(current)) != 0) {
        dentry_do_free(current);
        return result;
    }

    if (record->me.parent == NULL) {
        ns_entry->dentry_root = current;
    } else if ((result=uniq_skiplist_insert(record->me.parent->children,
                    current)) != 0)
    {
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
    return 0;
}

int dentry_remove(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    FDIRNamespaceEntry *ns_entry;
    bool is_dir;
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
        is_dir = true;
    } else {
        is_dir = false;
    }

    if ((result=inode_index_del_dentry(record->me.dentry)) != 0) {
        return result;
    }

    record->inode = record->me.dentry->inode;
    if (record->me.parent == NULL) {
        ns_entry->dentry_root = NULL;
    } else if ((result=uniq_skiplist_delete(record->me.parent->children,
                    record->me.dentry)) != 0)
    {
        return result;
    }

    if (is_dir) {
        db_context->dentry_context.counters.dir--;
    } else {
        db_context->dentry_context.counters.file--;
    }

    return 0;
}

static int rename_check(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    FDIRNamespaceEntry *ns_entry;
    int result;

    if ((result=dentry_find_me(&db_context->dentry_context, &record->ns,
                    &record->rename.src, &ns_entry, false)) != 0)
    {
        return result;
    }

    if (record->rename.src.dentry == ns_entry->dentry_root) {
        return EINVAL;
    }

    if ((result=dentry_find_me(&db_context->dentry_context, &record->ns,
                    &record->rename.dest, &ns_entry, false)) != 0)
    {
        if ((record->rename.flags & RENAME_EXCHANGE)) {
            return result;
        }

        return result == ENOENT ? 0 : result;
    }

    if (record->rename.dest.dentry == ns_entry->dentry_root) {
        return EINVAL;
    }

    if ((record->rename.flags & RENAME_NOREPLACE)) {
        return EEXIST;
    }

    if ((record->rename.flags & RENAME_EXCHANGE)) {
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

int dentry_rename(FDIRDataThreadContext *db_context,
        FDIRBinlogRecord *record)
{
    int result;

    if ((result=rename_check(db_context, record)) != 0) {
        return result;
    }

    if (record->rename.dest.dentry == record->rename.src.dentry) {
        return EEXIST;
    }

    if ((record->rename.flags & RENAME_EXCHANGE)) {
        FDIRServerDentry *src_parent;

        if ((result=uniq_skiplist_replace_ex(record->rename.dest.parent->
                        children, record->rename.dest.dentry,
                        record->rename.src.dentry, false)) != 0)
        {
            return result;
        }
        src_parent = record->rename.src.dentry->parent;
        record->rename.src.dentry->parent = record->rename.dest.parent;

        if ((result=uniq_skiplist_replace_ex(record->rename.src.parent->
                        children, record->rename.src.dentry,
                        record->rename.dest.dentry, false)) != 0)
        {
            return result;
        }
        record->rename.dest.dentry->parent = src_parent;
        record->inode = record->rename.dest.dentry->inode;
        return 0;
    }

    record->rename.overwritten = record->rename.dest.dentry;
    if (record->rename.dest.dentry != NULL) {
        result = uniq_skiplist_replace_ex(record->rename.dest.parent->
                children, record->rename.dest.dentry,
                record->rename.src.dentry, true);
    } else {
        result = uniq_skiplist_insert(record->rename.dest.parent->children,
                record->rename.src.dentry);
    }

    if (result != 0) {
        return result;
    }

    record->rename.src.dentry->parent = record->rename.dest.parent;
    record->inode = record->rename.src.dentry->inode;
    return uniq_skiplist_delete_ex(record->rename.src.parent->
            children, record->rename.src.dentry, false);
}

int dentry_find(const FDIRDEntryFullName *fullname, FDIRServerDentry **dentry)
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
        FDIRErrorInfo *error_info)
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

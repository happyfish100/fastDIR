
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include "fastcommon/fast_allocator.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "common/fdir_types.h"
#include "dentry.h"

typedef struct fdir_manager {
    FDIRDentry dentry_root;
    struct fast_allocator_context name_acontext;
} FDIRManager;

static FDIRManager fdir_manager;

static int dentry_strdup(string_t *dest, const char *src, const int len)
{
    dest->str = (char *)fast_allocator_alloc(
            &fdir_manager.name_acontext, len + 1);
    if (dest->str == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d fail", __LINE__, len + 1);
        return ENOMEM;
    }

    memcpy(dest->str, src, len + 1);
    dest->len = len;
    return 0;
}

#define dentry_strdup_ex(dest, src) \
    dentry_strdup(dest, src->str, src->len)

int dentry_init()
{
#define NAME_REGION_COUNT 1

    int result;
    struct fast_region_info regions[NAME_REGION_COUNT];

    memset(&fdir_manager, 0, sizeof(fdir_manager));

    FAST_ALLOCATOR_INIT_REGION(regions[0], 0, NAME_MAX + 1, 8, 16 * 1024);
    if ((result=fast_allocator_init_ex(&fdir_manager.name_acontext,
            regions, NAME_REGION_COUNT, 0, 0.00, 0, true)) != 0)
    {
        return result;
    }

    if ((result=dentry_strdup(&fdir_manager.dentry_root.name, "/", 1)) != 0) {
        return result;
    }

    return 0;
}

void dentry_destroy()
{
}

static int dentry_compare(const void *p1, const void *p2)
{
    return fc_string_compare(&((FDIRDentry *)p1)->name, 
            &((FDIRDentry *)p2)->name);
}

void dentry_free(void *ptr, const int delay_seconds)
{
    FDIRDentry *dentry;
    dentry = (FDIRDentry *)ptr;

    if (delay_seconds > 0) {
        fast_mblock_delay_free_object(dentry->context->dentry_allocator,
                    (void *)dentry, delay_seconds);
    } else {
        fast_mblock_free_object(dentry->context->dentry_allocator,
                    (void *)dentry);
    }
}

int dentry_init_obj(void *element, void *init_args)
{
    FDIRDentry *dentry;
    dentry = (FDIRDentry *)element;
    dentry->context = (FDIRDentryContext *)init_args;
    return 0;
}

int dentry_init_context(FDIRDentryContext *context)
{
    int result;
    const int max_level_count = 20;
    const int delay_free_seconds = 3600;

    if ((result=uniq_skiplist_init_ex(&context->factory,
                    max_level_count, dentry_compare, dentry_free,
                    16 * 1024, SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE,
                    delay_free_seconds)) != 0)
    {
        return result;
    }

     if ((result=fast_mblock_init_ex(context->dentry_allocator,
                     sizeof(FDIRDentry), 64 * 1024,
                     dentry_init_obj, context, false)) != 0)
     {
        return result;
     }

     return 0;
}

static const FDIRDentry *dentry_find_ex(const string_t *paths, const int count)
{
    return NULL;
}

static int dentry_find_parent_and_me(const string_t *path, string_t *my_name,
        FDIRDentry **parent, FDIRDentry **me)
{
    FDIRDentry target;
    string_t paths[FDIR_MAX_PATH_COUNT];
    int count;

    if (path->len == 0 || path->str[0] != '/') {
        *parent = *me = NULL;
        my_name->len = 0;
        my_name->str = NULL;
        return EINVAL;
    }

    count = split_string_ex(path, '/', paths, FDIR_MAX_PATH_COUNT, true);
    if (count == 0) {
        *parent = NULL;
        *me = &fdir_manager.dentry_root;
        my_name->len = 0;
        my_name->str = "";
        return 0;
    }

    *my_name = paths[count - 1];
    if (count == 1) {
        *parent = &fdir_manager.dentry_root;
    } else {
        *parent = (FDIRDentry *)dentry_find_ex(paths, count - 1);
        if (*parent == NULL) {
            *me = NULL;
            return ENOENT;
        }
    }

    target.name = *my_name;
    *me = (FDIRDentry *)uniq_skiplist_find((*parent)->children, &target);
    return 0;
}

int dentry_create(FDIRDentryContext *context, const string_t *path,
        const int flags, const mode_t mode)
{
    FDIRDentry *parent;
    FDIRDentry *me;
    //UniqSkiplist *sl;
    string_t my_name;
    int result;
    
    if ((result=dentry_find_parent_and_me(path, &my_name,
                    &parent, &me)) != 0)
    {
        return result;
    }

    if (me != NULL) {
        return EEXIST;
    }

    //TODO
    return 0;
}

int dentry_remove(const string_t *path)
{
    return 0;
}

int dentry_find(const string_t *path, FDIRDentry **dentry)
{
    FDIRDentry *parent;
    string_t my_name;
    int result;

    
    if ((result=dentry_find_parent_and_me(path, &my_name,
                    &parent, dentry)) != 0)
    {
        return result;
    }

    if (*dentry == NULL) {
        return ENOENT;
    }

    return 0;
}

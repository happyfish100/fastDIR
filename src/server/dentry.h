
#ifndef _FDIR_DENTRY_H
#define _FDIR_DENTRY_H

#include "fastcommon/common_define.h"
#include "fastcommon/uniq_skiplist.h"

typedef struct fdir_dentry_context {
    UniqSkiplistFactory factory;
    struct fast_mblock_man *dentry_allocator;
} FDIRDentryContext;

typedef struct fdir_dstatus {
    mode_t mode;
    int ctime;  /* create time */
    int mtime;  /* modify time */
    int atime;  /* last access time */
    int64_t size;   /* file size in bytes */
} FDIRDStatus;

typedef struct fdir_dentry {
    string_t name;
    FDIRDStatus stat;
    FDIRDentryContext *context;
    UniqSkiplist *children;
} FDIRDentry;

typedef struct fdir_dentry_array {
    FDIRDentry **entries;
    int alloc;
    int count;
} FDIRDentryArray;

#ifdef __cplusplus
extern "C" {
#endif

    int dentry_init();
    void dentry_destroy();

    int dentry_init_context(FDIRDentryContext *context);

    int dentry_create(FDIRDentryContext *context, const string_t *path,
            const int flags, const mode_t mode);

    int dentry_remove(const string_t *path);

    int dentry_find(const string_t *path, FDIRDentry **dentry);

    int dentry_list(const string_t *path, FDIRDentryArray *array);
    void dentry_array_free(FDIRDentryArray *array);

#ifdef __cplusplus
}
#endif

#endif

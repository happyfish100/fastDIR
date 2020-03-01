
#ifndef _FDIR_DENTRY_H
#define _FDIR_DENTRY_H

#include "server_types.h"
#include "binlog/binlog_types.h"

#define MAX_ENTRIES_PER_PATH  (16 * 1024)

typedef struct fdir_server_dentry {
    int64_t inode;
    string_t name;
    FDIRDEntryStatus stat;
    string_t user_data;      //user defined data
    FDIRDentryContext *context;
    UniqSkiplist *children;
} FDIRServerDentry;

#ifdef __cplusplus
extern "C" {
#endif

    int dentry_init();
    void dentry_destroy();

    int dentry_init_context(FDIRServerContext *server_context);

    int dentry_create(FDIRServerContext *server_context,
            const FDIRPathInfo *path_info,
            FDIRBinlogRecord *record, const int flags);

    int dentry_remove(FDIRServerContext *server_context,
            const FDIRPathInfo *path_info,
            FDIRBinlogRecord *record);

    int dentry_find(FDIRServerContext *server_context,
            const FDIRPathInfo *path_info,
            FDIRServerDentry **dentry);

    int dentry_list(FDIRServerContext *server_context,
            const FDIRPathInfo *path_info, FDIRServerDentryArray *array);

    static inline void dentry_array_free(FDIRServerDentryArray *array)
    {
        if (array->entries != NULL) {
            free(array->entries);
            array->entries = NULL;
            array->alloc = array->count = 0;
        }
    }

#ifdef __cplusplus
}
#endif

#endif

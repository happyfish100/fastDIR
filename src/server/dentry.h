
#ifndef _FDIR_DENTRY_H
#define _FDIR_DENTRY_H

#include "server_types.h"
#include "data_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

    int dentry_init();
    void dentry_destroy();

    int dentry_init_context(FDIRDataThreadContext *db_context);

    int dentry_create(FDIRDataThreadContext *db_context,
            FDIRBinlogRecord *record);

    int dentry_remove(FDIRDataThreadContext *db_context,
            FDIRBinlogRecord *record);

    int dentry_find(const FDIRDEntryFullName *fullname,
            FDIRServerDentry **dentry);

    int dentry_find_by_pname(FDIRServerDentry *parent,
            const string_t *name, FDIRServerDentry **dentry);

    int dentry_list(const FDIRDEntryFullName *fullname,
            FDIRServerDentryArray *array);

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

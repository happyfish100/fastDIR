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


#ifndef _FDIR_DENTRY_H
#define _FDIR_DENTRY_H

#include "server_types.h"
#include "ns_manager.h"
#include "data_thread.h"
#include "db/dentry_lru.h"

#define FDIR_SET_HARD_LINK_DENTRY(dentry)  \
    do { \
        if (FDIR_IS_DENTRY_HARD_LINK((dentry)->stat.mode)) {  \
            dentry = (dentry)->src_dentry;  \
        } \
    } while (0)

#define FDIR_GET_REAL_DENTRY(dentry)  \
    (FDIR_IS_DENTRY_HARD_LINK((dentry)->stat.mode) ? \
    (dentry)->src_dentry : dentry)

#ifdef __cplusplus
extern "C" {
#endif

    int dentry_init();
    void dentry_destroy();

    void dentry_set_inc_alloc_bytes(FDIRServerDentry *dentry,
            const int64_t inc_alloc);

    int dentry_init_context(FDIRDataThreadContext *thread_ctx);

    int dentry_create(FDIRDataThreadContext *thread_ctx,
            FDIRBinlogRecord *record);

    int dentry_remove(FDIRDataThreadContext *thread_ctx,
            FDIRBinlogRecord *record);

    int dentry_rename(FDIRDataThreadContext *thread_ctx,
            FDIRBinlogRecord *record);

    int dentry_find_parent(const FDIRDEntryFullName *fullname,
            FDIRServerDentry **parent, string_t *my_name);

    int dentry_find_ex(const FDIRDEntryFullName *fullname,
            FDIRServerDentry **dentry, const bool hdlink_follow);

    static inline int dentry_find(const FDIRDEntryFullName *fullname,
            FDIRServerDentry **dentry)
    {
        const bool hdlink_follow = true;
        return dentry_find_ex(fullname, dentry, hdlink_follow);
    }

    int dentry_find_by_pname(FDIRServerDentry *parent,
            const string_t *name, FDIRServerDentry **dentry);

    int dentry_get_full_path(const FDIRServerDentry *dentry,
            BufferInfo *full_path, SFErrorInfo *error_info);

    int dentry_resolve_symlink(FDIRServerDentry **dentry);

    int dentry_list(FDIRServerDentry *dentry, PointerArray **parray);

    static inline int dentry_list_by_path(const FDIRDEntryFullName *fullname,
            PointerArray **parray)
    {
        const bool hdlink_follow = false;
        int result;
        FDIRServerDentry *dentry;

        if ((result=dentry_find_ex(fullname, &dentry, hdlink_follow)) != 0) {
            return result;
        }

        return dentry_list(dentry, parray);
    }

    static inline void dentry_array_free(PointerArray **parray)
    {
        if (*parray != NULL) {
            ptr_array_allocator_free(&DENTRY_PARRAY_ALLOCATOR, *parray);
            *parray = NULL;
        }
    }

    struct fast_mblock_man *dentry_get_kvarray_allocator_by_capacity(
            FDIRDentryContext *context, const int alloc_elts);

    FDIRServerDentry *dentry_alloc_object(FDIRDataThreadContext *thread);

    void dentry_free_for_elimination(FDIRServerDentry *dentry);

    bool dentry_free_ex(FDIRServerDentry *dentry, const int dec_count);

    static inline void dentry_free(FDIRServerDentry *dentry)
    {
        if (!dentry_free_ex(dentry, 1)) {
            if (STORAGE_ENABLED && (dentry->db_args->loaded_flags &
                        FDIR_DENTRY_LOADED_FLAGS_BASIC) != 0)
            {
                dentry_lru_del(dentry);
            }
        }
    }

    static inline void dentry_hold(FDIRServerDentry *dentry)
    {
        __sync_add_and_fetch(&dentry->reffer_count, 1);
    }

    void dentry_release_ex(FDIRServerDentry *dentry, const int dec_count);

    static inline void dentry_release(FDIRServerDentry *dentry)
    {
        dentry_release_ex(dentry, 1);
    }

#ifdef __cplusplus
}
#endif

#endif

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


#ifndef _INODE_INDEX_H
#define _INODE_INDEX_H

#include "fastcommon/fast_mblock.h"
#include "binlog/binlog_types.h"
#include "server_types.h"
#include "flock.h"
#include "data_thread.h"

typedef struct fdir_xattr_iterator {
    key_value_pair_t *kv;
    key_value_pair_t *end;
} FDIRXAttrIterator;

#ifdef __cplusplus
extern "C" {
#endif

    int inode_index_init();
    void inode_index_destroy();

    int inode_index_add_dentry(FDIRServerDentry *dentry);

    int inode_index_del_dentry(FDIRServerDentry *dentry);

    FDIRServerDentry *inode_index_get_dentry(const int64_t inode);

    FDIRServerDentry *inode_index_get_dentry_by_pname(
            const int64_t parent_inode, const string_t *name);

    FDIRServerDentry *inode_index_check_set_dentry_size(
            const FDIRSetDEntrySizeInfo *dsize,
            const bool need_lock, int *modified_flags);

    FDIRServerDentry *inode_index_update_dentry(
            const FDIRBinlogRecord *record);

    FDIRServerDentry *inode_index_set_xattr(
            FDIRDataThreadContext *db_context,
            const FDIRBinlogRecord *record, int *err_no);

    int inode_index_get_xattr(FDIRServerDentry *dentry,
            const string_t *name, string_t *value);

    void inode_index_list_xattr(FDIRServerDentry *dentry,
            FDIRXAttrIterator *it);

    static inline const key_value_pair_t *xattr_iterator_next(
            FDIRXAttrIterator *it)
    {
        if (it->kv < it->end) {
            return it->kv++;
        } else {
            return NULL;
        }
    }

    FLockTask *inode_index_flock_apply(const int64_t inode, const short type,
            const int64_t offset, const int64_t length, const bool block,
            const FlockOwner *owner, struct fast_task_info *task, int *result);

    void inode_index_flock_release(FLockTask *ftask);

    int inode_index_flock_getlk(const int64_t inode, FLockTask *ftask);

    SysLockTask *inode_index_sys_lock_apply(const int64_t inode, const bool block,
            struct fast_task_info *task, int *result);

    int inode_index_sys_lock_release_ex(SysLockTask *sys_task,
            sys_lock_release_callback callback, void *args);

    static inline int inode_index_sys_lock_release(SysLockTask *sys_task)
    {
        return inode_index_sys_lock_release_ex(sys_task, NULL, NULL);
    }

#ifdef __cplusplus
}
#endif

#endif

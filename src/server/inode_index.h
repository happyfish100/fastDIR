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

#ifdef __cplusplus
extern "C" {
#endif

    int inode_index_init();
    void inode_index_destroy();

    int inode_index_add_dentry(FDIRServerDentry *dentry);

    int inode_index_del_dentry(FDIRServerDentry *dentry);

    FDIRServerDentry *inode_index_find_dentry(const int64_t inode);

    int inode_index_get_dentry_ex(FDIRDataThreadContext *thread_ctx,
            const int64_t inode, FDIRServerDentry **dentry,
            const bool load_children);

#define inode_index_get_dentry(thread_ctx, inode, dentry) \
    inode_index_get_dentry_ex(thread_ctx, inode, dentry, false)

    int inode_index_get_dentry_by_pname(FDIRDataThreadContext *thread_ctx,
            const int64_t parent_inode, const string_t *name,
            FDIRServerDentry **dentry);

    int inode_index_check_set_dentry_size_ex(FDIRDataThreadContext
            *thread_ctx, FDIRBinlogRecord *record, const bool dry_run);

#define inode_index_check_set_dentry_size(thread_ctx, record) \
    inode_index_check_set_dentry_size_ex(thread_ctx, record, false)

    int inode_index_set_xattr_check(FDIRServerDentry *dentry,
            const FDIRBinlogRecord *record);
    int inode_index_set_xattr(FDIRServerDentry *dentry,
            const FDIRBinlogRecord *record);

    int inode_index_remove_xattr_check(FDIRServerDentry *dentry,
            const string_t *name);
    int inode_index_remove_xattr(FDIRServerDentry *dentry,
            const string_t *name);

    int inode_index_get_xattr(FDIRServerDentry *dentry,
            const string_t *name, string_t *value);

    FDIRFLockTask *inode_index_flock_apply(FDIRDataThreadContext *thread_ctx,
            const int64_t inode, const FDIRFlockParams *params, const bool block,
            struct fast_task_info *task, int *result);

    int inode_index_flock_unlock(FDIRDataThreadContext *thread_ctx,
            const int64_t inode, const FDIRFlockParams *params);

    void inode_index_flock_release(FDIRFLockTask *ftask);

    int inode_index_flock_getlk(const int64_t inode, FDIRFLockTask *ftask);

    FDIRSysLockTask *inode_index_sys_lock_apply(FDIRDataThreadContext *thread_ctx,
            const int64_t inode, const bool block,
            struct fast_task_info *task, int *result);

    int inode_index_sys_lock_release(FDIRSysLockTask *sys_task);

    void inode_index_free_flock_entry(FDIRServerDentry *dentry);

    int inode_index_xattrs_copy(const key_value_pair_t *xattrs,
            const int count, FDIRServerDentry *dentry);

#ifdef __cplusplus
}
#endif

#endif

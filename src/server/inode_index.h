
#ifndef _INODE_INDEX_H
#define _INODE_INDEX_H

#include "fastcommon/fast_mblock.h"
#include "binlog/binlog_types.h"
#include "server_types.h"
#include "flock.h"

#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_SIZE    1
#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME   2

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

    FDIRServerDentry *inode_index_check_set_dentry_size(const int64_t inode,
            const int64_t new_size, const bool force, int *modified_flags);

    FDIRServerDentry *inode_index_update_dentry(
            const FDIRBinlogRecord *record);

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

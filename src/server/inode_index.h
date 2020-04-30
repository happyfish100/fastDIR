
#ifndef _INODE_INDEX_H
#define _INODE_INDEX_H

#include "fastcommon/fast_mblock.h"
#include "binlog/binlog_types.h"
#include "server_types.h"

#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_SIZE    1
#define FDIR_DENTRY_FIELD_MODIFIED_FLAG_MTIME   2

typedef struct {
    pthread_mutex_t lock;
} InodeSharedContext;

#ifdef __cplusplus
extern "C" {
#endif

    int inode_index_init();
    void inode_index_destroy();

    int inode_index_add_dentry(FDIRServerDentry *dentry);
    int inode_index_del_dentry(FDIRServerDentry *dentry);
    FDIRServerDentry *inode_index_get_dentry(const int64_t inode);

    FDIRServerDentry *inode_index_check_set_dentry_size(const int64_t inode,
            const int64_t new_size, const bool force, int *modified_flags);

    FDIRServerDentry *inode_index_update_dentry(
            const FDIRBinlogRecord *record);

#ifdef __cplusplus
}
#endif

#endif

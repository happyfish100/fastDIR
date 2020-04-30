
#ifndef _INODE_INDEX_H
#define _INODE_INDEX_H

#include "fastcommon/fast_mblock.h"
#include "server_types.h"

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

#ifdef __cplusplus
}
#endif

#endif

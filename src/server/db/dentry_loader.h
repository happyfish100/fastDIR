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


#ifndef _FDIR_DENTRY_LOADER_H
#define _FDIR_DENTRY_LOADER_H

#include "../server_types.h"
#include "../ns_manager.h"
#include "../data_thread.h"
#include "dentry_serializer.h"

typedef struct {
    int64_t inode;
    FDIRServerDentry *dentry;
} DentryPair;

typedef struct {
    DentryPair current;
    DentryPair parent;
} DentryParentChildPair;

typedef struct {
    DentryParentChildPair *pairs;
    int count;
    int alloc;
} DentryParentChildArray;

#ifdef __cplusplus
extern "C" {
#endif

    int dentry_loader_init();

    int dentry_load_root(FDIRNamespaceEntry *ns_entry,
            const int64_t inode, FDIRServerDentry **dentry);

    int dentry_load_inode(FDIRDataThreadContext *thread_ctx,
            FDIRNamespaceEntry *ns_entry, const int64_t inode,
            FDIRServerDentry **dentry);

    int dentry_check_load(FDIRDataThreadContext *thread_ctx,
            FDIRServerDentry *dentry);

    int dentry_load_xattr(FDIRDataThreadContext *thread_ctx,
            FDIRServerDentry *dentry);

#ifdef __cplusplus
}
#endif

#endif

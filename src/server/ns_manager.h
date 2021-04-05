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


#ifndef _FDIR_NS_MANAGER_H
#define _FDIR_NS_MANAGER_H

#include "server_types.h"
#include "ns_subscribe.h"

typedef struct fdir_namespace_entry {
    string_t name;
    struct fdir_server_dentry *dentry_root;
    volatile int64_t dentry_count;
    volatile int64_t used_bytes;
    struct {
        struct fdir_namespace_entry *htable; //for hashtable
        struct fdir_namespace_entry *list;   //for chain list
    } nexts;

    FDIRNSSubscribeEntry subs_entries[0];
} FDIRNamespaceEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int ns_manager_init();
    void ns_manager_destroy();

    FDIRNamespaceEntry *fdir_namespace_get(FDIRDentryContext *context,
            const string_t *ns, const bool create_ns, int *err_no);

    int fdir_namespace_stat(const string_t *ns, FDIRNamespaceStat *stat);

    int fdir_namespace_inc_alloc_bytes(FDIRNamespaceEntry *ns_entry,
            const int64_t inc_alloc);

#ifdef __cplusplus
}
#endif

#endif

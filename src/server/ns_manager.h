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
#include "data_thread.h"

typedef struct fdir_namespace_info {
    union {
        struct fdir_server_dentry *ptr;  //for current
        int64_t inode;  //for delay
    } root;
    struct {
        volatile int64_t dir;
        volatile int64_t file;
    } counts;
    volatile int64_t used_bytes;
} FDIRNamespaceInfo;

typedef struct fdir_namespace_entry {
    int id;
    unsigned int hash_code;
    string_t name;
    FDIRNamespaceInfo current;
    FDIRNamespaceInfo delay;   //for storage engine
    FDIRDataThreadContext *thread_ctx;

    struct {
        struct fdir_namespace_entry *htable; //for hashtable
    } nexts;

    FDIRNSSubscribeEntry subs_entries[0];
} FDIRNamespaceEntry;

typedef struct fdir_namespace_ptr_array {
    FDIRNamespaceEntry **namespaces;
    int count;
    int alloc;
} FDIRNamespacePtrArray;

typedef struct fdir_namespace_dump_context {
    FDIRNamespaceEntry **entries;
    int alloc;
    int count;
    BufferInfo buffer;
    int64_t last_version;
} FDIRNamespaceDumpContext;

#ifdef __cplusplus
extern "C" {
#endif

    int ns_manager_init();
    void ns_manager_destroy();

    FDIRNamespaceEntry *fdir_namespace_get(FDIRDataThreadContext *thread_ctx,
            const string_t *ns, const bool create_ns, int *err_no);

    FDIRNamespaceEntry *fdir_namespace_get_by_id(const int id);

    const FDIRNamespacePtrArray *fdir_namespace_get_all();

    int fdir_namespace_stat(const string_t *ns, FDIRNamespaceStat *stat);

    void fdir_namespace_inc_alloc_bytes(FDIRNamespaceEntry *ns_entry,
            const int64_t inc_alloc);

    void fdir_namespace_push_all_to_holding_queue(
            FDIRNSSubscriber *subscriber);

    int fdir_namespace_dump(FDIRNamespaceDumpContext *ctx);
    int fdir_namespace_load(int64_t *last_version);

    int fdir_namespace_load_root();

#ifdef __cplusplus
}
#endif

#endif

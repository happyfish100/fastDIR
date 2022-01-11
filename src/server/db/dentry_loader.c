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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../dentry.h"
#include "../inode_index.h"
#include "dentry_lru.h"
#include "dentry_loader.h"

typedef struct {
    struct fast_mblock_man pair_array_allocator;  //element: DentryParentChildArray
} DentryLoaderContext;

static DentryLoaderContext dentry_loader_ctx;

#define PAIR_ARRAY_ALLOCATOR  dentry_loader_ctx.pair_array_allocator

static int pair_array_alloc_init_func(void *element, void *args)
{
    DentryParentChildArray *parray;

    parray = (DentryParentChildArray *)element;
    parray->alloc = 1024;
    parray->pairs = (DentryParentChildPair *)fc_malloc(
            sizeof(DentryParentChildPair) * parray->alloc);
    if (parray->pairs == NULL) {
        return ENOMEM;
    }

    return 0;
}

int dentry_loader_init()
{
    int result;
    if ((result=fast_mblock_init_ex1(&PAIR_ARRAY_ALLOCATOR,
                    "pc-pair-array", sizeof(DentryParentChildArray),
                    64, 0, pair_array_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    return 0;
}

static int alloc_init_dentry(FDIRNamespaceEntry *ns_entry,
        FDIRServerDentry *parent, const int64_t inode,
        const string_t *name, FDIRServerDentry **dentry)
{
    int result;

    if ((*dentry=dentry_alloc_object(ns_entry->thread_ctx)) == NULL) {
        return ENOMEM;
    }

    memset(&(*dentry)->stat, 0, sizeof((*dentry)->stat));
    (*dentry)->db_args->loaded_flags = 0;
    (*dentry)->inode = inode;
    if (name != NULL) {
        if ((result=dentry_strdup(&ns_entry->thread_ctx->dentry_context,
                        &(*dentry)->name, name)) != 0)
        {
            return result;
        }
    } else {
        FC_SET_STRING_NULL((*dentry)->name);
    }

    (*dentry)->parent = parent;
    if (parent != NULL) {
        if ((result=uniq_skiplist_insert(parent->children, *dentry)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "parent inode: %"PRId64", insert child {inode: %"PRId64", "
                    "name: %.*s} fail, errno: %d, error info: %s", __LINE__,
                    parent->inode, (*dentry)->inode, (*dentry)->name.len,
                    (*dentry)->name.str, result, STRERROR(result));
            return result;
        }
    }
    (*dentry)->ns_entry = ns_entry;
    __sync_add_and_fetch(&(*dentry)->reffer_count, 1);
    return 0;
}

static int dentry_load_children_ex(FDIRServerDentry *parent,
        DentryPair *current_pair)
{
    int result;
    string_t content;
    FDIRDataThreadContext *thread_ctx;
    id_name_array_t array_holder;
    const id_name_array_t *id_name_array;
    const id_name_pair_t *pair;
    const id_name_pair_t *end;
    FDIRServerDentry *child;
    UniqSkiplistIterator it;

    if ((parent->db_args->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_CHILDREN)) {
        if (current_pair->inode == 0) {
            return 0;
        }

        uniq_skiplist_iterator(parent->children, &it);
        while ((child=(FDIRServerDentry *)uniq_skiplist_next(&it)) != NULL) {
            if (current_pair->inode == child->inode) {
                current_pair->dentry = child;
                return 0;
            }
        }

        {
            int count = 0;
            logError("line: %d, parent inode: %"PRId64", %.*s", __LINE__,
                    parent->inode, parent->name.len, parent->name.str);
            uniq_skiplist_iterator(parent->children, &it);
            while ((child=(FDIRServerDentry *)uniq_skiplist_next(&it)) != NULL) {
                logError("%d. %"PRId64" => %.*s", ++count, child->inode,
                        child->name.len, child->name.str);
            }
        }

        return ENOENT;
    }

    thread_ctx = parent->ns_entry->thread_ctx;
    if ((result=STORAGE_ENGINE_FETCH_API(parent->inode,
                    FDIR_PIECE_FIELD_INDEX_CHILDREN,
                    &thread_ctx->db_fetch_ctx.read_ctx)) != 0)
    {
        if (result != ENODATA) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", load children fail, result: %d",
                    __LINE__, parent->inode, result);
            return result;
        }
        array_holder.elts = NULL;
        array_holder.count = 0;
        id_name_array = &array_holder;
    } else {
        FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(thread_ctx->db_fetch_ctx.
                    read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(thread_ctx->
                        db_fetch_ctx.read_ctx.op_ctx));
        if ((result=dentry_serializer_unpack_children(thread_ctx, &content,
                        parent->inode, &id_name_array)) != 0)
        {
            return result;
        }
    }

    parent->children = uniq_skiplist_new(&thread_ctx->dentry_context.
            factory, DENTRY_SKIPLIST_INIT_LEVEL_COUNT);
    if (parent->children == NULL) {
        return ENOMEM;
    }

    end = id_name_array->elts + id_name_array->count;
    for (pair=id_name_array->elts; pair<end; pair++) {
        if ((result=alloc_init_dentry(parent->ns_entry, parent,
                        pair->id, &pair->name, &child)) != 0)
        {
            return result;
        }

        if (current_pair->inode == child->inode) {
            current_pair->dentry = child;
        }
    }

    parent->stat.nlink += id_name_array->count;
    parent->db_args->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_CHILDREN;
    if (current_pair->inode == 0) {
        return 0;
    } else {

        if (current_pair->dentry == NULL) {
            int count = 0;
            logError("line: %d, parent inode: %"PRId64", %.*s", __LINE__,
                    parent->inode, parent->name.len, parent->name.str);
            uniq_skiplist_iterator(parent->children, &it);
            while ((child=(FDIRServerDentry *)uniq_skiplist_next(&it)) != NULL) {
                logError("%d. %"PRId64" => %.*s", ++count, child->inode,
                        child->name.len, child->name.str);
            }
        }

        return current_pair->dentry != NULL ? 0 : ENOENT;
    }
}

static inline int dentry_load_children(FDIRServerDentry *parent)
{
    DentryPair current_pair;

    current_pair.inode = 0;
    return dentry_load_children_ex(parent, &current_pair);
}

static int dentry_load_basic(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *dentry)
{
    int result;
    string_t content;
    int64_t src_inode;

    if ((result=STORAGE_ENGINE_FETCH_API(dentry->inode,
                    FDIR_PIECE_FIELD_INDEX_BASIC, &thread_ctx->
                    db_fetch_ctx.read_ctx)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", load basic fail, result: %d",
                __LINE__, dentry->inode, result);
        return result;
    }

    FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(thread_ctx->
                db_fetch_ctx.read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                    thread_ctx->db_fetch_ctx.read_ctx.op_ctx));
    if ((result=dentry_serializer_unpack_basic(thread_ctx,
                    &content, dentry, &src_inode)) != 0)
    {
        logCrit("file: "__FILE__", line: %d, "
                "inode: %"PRId64", unpack basic info fail, "
                "program exit!", __LINE__, dentry->inode);
        sf_terminate_myself();
        return result;
    }
    dentry->db_args->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_BASIC;
    dentry_lru_add(dentry);

    if (S_ISDIR(dentry->stat.mode)) {
        dentry->stat.nlink = 1;   //reset nlink for directory
    }
    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        if ((dentry->src_dentry=inode_index_find_dentry(src_inode)) == NULL) {
            result = dentry_load_inode(thread_ctx, dentry->ns_entry,
                    src_inode, &dentry->src_dentry);
        }
    } else {
        if ((result=inode_index_add_dentry(dentry)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "inode_index_add_dentry {inode: %"PRId64", name: %.*s} "
                    "fail, errno: %d, error info: %s", __LINE__,
                    dentry->inode, dentry->name.len, dentry->name.str,
                    result, STRERROR(result));
        }
    }

    return result;
}

int dentry_load_xattr(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *dentry)
{
    int result;
    string_t content;
    const key_value_array_t *kv_array;

    if ((dentry->db_args->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_XATTR)) {
        return 0;
    }

    if ((result=STORAGE_ENGINE_FETCH_API(dentry->inode,
                    FDIR_PIECE_FIELD_INDEX_XATTR, &thread_ctx->
                    db_fetch_ctx.read_ctx)) != 0)
    {
        if (result == ENODATA) {
            result = 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", load xattr fail, result: %d",
                    __LINE__, dentry->inode, result);
            return result;
        }
    } else {
        FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(thread_ctx->
                    db_fetch_ctx.read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                        thread_ctx->db_fetch_ctx.read_ctx.op_ctx));
        if ((result=dentry_serializer_unpack_xattr(thread_ctx,
                        &content, dentry->inode, &kv_array)) != 0)
        {
            logCrit("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", unpack xattr fail, "
                    "program exit!", __LINE__, dentry->inode);
            sf_terminate_myself();
            return result;
        }

        if ((result=inode_index_xattrs_copy(kv_array, dentry)) != 0) {
            return result;
        }
    }
    dentry->db_args->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_XATTR;

    return result;
}

int dentry_check_load(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *dentry)
{
    int result;

    if ((dentry->db_args->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_BASIC) == 0) {
        if ((result=dentry_load_basic(thread_ctx, dentry)) != 0) {
            return result;
        }
    } else {
        dentry_lru_move_tail(dentry);
    }

    if (S_ISDIR(dentry->stat.mode) && (dentry->db_args->loaded_flags &
                FDIR_DENTRY_LOADED_FLAGS_CHILDREN) == 0)
    {
        if ((result=dentry_load_children(dentry)) != 0) {
            return result;
        }
    }

    return 0;
}

int dentry_load_root(FDIRNamespaceEntry *ns_entry,
        const int64_t inode, FDIRServerDentry **dentry)
{
    int result;
    FDIRServerDentry *parent = NULL;
    string_t name;

    FC_SET_STRING_EX(name, "", 0);
    if ((result=alloc_init_dentry(ns_entry, parent,
                    inode, &name, dentry)) != 0)
    {
        return result;
    }

    return dentry_load_basic(ns_entry->thread_ctx, *dentry);
}

static inline int dentry_load_child(FDIRServerDentry *parent,
        DentryPair *child_pair)
{
    int result;

    if ((result=dentry_load_children_ex(parent, child_pair)) != 0) {
        return result;
    }
    return dentry_load_basic(parent->ns_entry->
            thread_ctx, child_pair->dentry);
}

static inline int dentry_load_one(FDIRNamespaceEntry *ns_entry,
        FDIRServerDentry *parent, DentryPair *child_pair)
{
    int result;

    if (parent == NULL) {
        if ((result=alloc_init_dentry(ns_entry, parent, child_pair->inode,
                        NULL, &child_pair->dentry)) != 0)
        {
            return result;
        }
        return dentry_load_basic(ns_entry->thread_ctx, child_pair->dentry);
    } else {
        return dentry_load_child(parent, child_pair);
    }
}

static int dentry_load_all(DentryParentChildArray *parray)
{
    int result;
    DentryParentChildPair *pair;
    DentryParentChildPair *last;

    last = parray->pairs + parray->count - 1;
    for (pair = last; pair >= parray->pairs; pair--) {
        if ((result=dentry_load_child(pair->parent.dentry,
                        &pair->current)) != 0)
        {
            return result;
        }

        if (pair > parray->pairs) {
            if ((pair - 1)->parent.inode != pair->current.inode) {
                logError("file: "__FILE__", line: %d, "
                        "shit! inode: %"PRId64" != %"PRId64,
                        __LINE__, (pair - 1)->parent.inode,
                        pair->current.inode);
            }
            (pair - 1)->parent.dentry = pair->current.dentry;
        }
    }

    return 0;
}

int dentry_load_inode(FDIRDataThreadContext *thread_ctx,
        FDIRNamespaceEntry *ns_entry, const int64_t inode,
        FDIRServerDentry **dentry)
{
    int result;
    int64_t parent_inode;
    string_t content;
    FDIRDBFetchContext *db_fetch_ctx;
    DentryParentChildArray *parray;
    DentryParentChildPair *pair;

    parray = (DentryParentChildArray *)fast_mblock_alloc_object(
            &PAIR_ARRAY_ALLOCATOR);
    if (parray == NULL) {
        return ENOMEM;
    }

    db_fetch_ctx = &thread_ctx->db_fetch_ctx;
    pair = parray->pairs;
    parray->count = 1;
    pair->current.inode = inode;
    pair->current.dentry = NULL;

    do {
        if ((result=STORAGE_ENGINE_FETCH_API(pair->current.inode,
                        FDIR_PIECE_FIELD_INDEX_BASIC,
                        &db_fetch_ctx->read_ctx)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", load basic fail, result: %d",
                    __LINE__, pair->current.inode, result);
            break;
        }

        FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(db_fetch_ctx->
                    read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                        db_fetch_ctx->read_ctx.op_ctx));
        if ((result=dentry_serializer_extract_parent(db_fetch_ctx, &content,
                        pair->current.inode, &parent_inode)) != 0)
        {
            break;
        }

        pair->parent.inode = parent_inode;
        if (parent_inode == 0) {  //orphan inode
            if (parray->count > 1) {
                logError("file: "__FILE__", line: %d, "
                        "inode: %"PRId64", invalid parent inode: 0",
                        __LINE__, pair->current.inode);
                result = EINVAL;
            } else if (ns_entry == NULL) {
                result = dentry_serializer_extract_namespace(
                        db_fetch_ctx, &content, inode, &ns_entry);
            }

            pair->parent.dentry = NULL;
            break;
        }

        pair->parent.dentry = inode_index_find_dentry(parent_inode);
        if (pair->parent.dentry != NULL) {
            if (ns_entry == NULL) {
                ns_entry = pair->parent.dentry->ns_entry;
            }
            break;
        }

        pair = parray->pairs + parray->count++;
        if (parray->count > parray->alloc) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", path's level is too large, "
                    "exceeds %d", __LINE__, inode, parray->alloc);
            result = EOVERFLOW;
            break;
        }

        pair->current.inode = parent_inode;
        pair->current.dentry = NULL;
    } while (1);

    if (result == 0) {
        if (parray->count == 1) {
            result = dentry_load_one(ns_entry, pair->parent.dentry,
                    &parray->pairs->current);
        } else {
            result = dentry_load_all(parray);
        }
    }
    *dentry = parray->pairs->current.dentry;

    fast_mblock_free_object(&PAIR_ARRAY_ALLOCATOR, parray);
    return result;
}

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
#include "../inode_index.h"
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

    if ((parent->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_CHILDREN) != 0) {
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

        return ENOENT;
    }

    thread_ctx = parent->ns_entry->thread_ctx;
    if ((result=STORAGE_ENGINE_FETCH_API(parent->inode,
                    FDIR_PIECE_FIELD_INDEX_CHILDREN,
                    &thread_ctx->db_fetch_ctx.read_ctx)) != 0)
    {
        if (result != ENODATA) {
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
        child = (FDIRServerDentry *)fast_mblock_alloc_object(
                &thread_ctx->dentry_context.dentry_allocator);
        if (child == NULL) {
            return ENOMEM;
        }

        memset(child, 0, sizeof(FDIRServerDentry));
        child->inode = pair->id;
        child->parent = parent;
        if ((result=dentry_strdup(&thread_ctx->dentry_context,
                        &child->name, &pair->name)) != 0)
        {
            return result;
        }

        if ((result=uniq_skiplist_insert(parent->children, child)) != 0) {
            return result;
        }
        parent->stat.nlink++;
        child->ns_entry = parent->ns_entry;
        __sync_add_and_fetch(&child->reffer_count, 1);

        if (current_pair->inode == child->inode) {
            current_pair->dentry = child;
        }
    }

    parent->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_CHILDREN;
    if (current_pair->inode == 0) {
        return 0;
    } else {
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
    dentry->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_BASIC;

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        result = dentry_load_inode(thread_ctx, dentry->
                ns_entry, src_inode, &dentry->src_dentry);
    } else {
        result = inode_index_add_dentry(dentry);
    }

    return result;
}

static int dentry_load_one(FDIRNamespaceEntry *ns_entry,
        FDIRServerDentry *parent, const int64_t inode,
        const string_t *name, FDIRServerDentry **dentry)
{
    int result;
    DentryPair current_pair;

    if (parent != NULL) {
        current_pair.inode = inode;
        current_pair.dentry = NULL;
        if ((result=dentry_load_children_ex(parent, &current_pair)) != 0) {
            return result;
        }
        *dentry = current_pair.dentry;
    } else {
        *dentry = (FDIRServerDentry *)fast_mblock_alloc_object(
                &ns_entry->thread_ctx->dentry_context.dentry_allocator);
        if (*dentry == NULL) {
            return ENOMEM;
        }

        memset(*dentry, 0, sizeof(FDIRServerDentry) +
                sizeof(FDIRServerDentryDBArgs));
        (*dentry)->inode = inode;
        (*dentry)->parent = parent;
        if (name != NULL) {
            (*dentry)->name = *name;
        }
        (*dentry)->ns_entry = ns_entry;
        __sync_add_and_fetch(&(*dentry)->reffer_count, 1);
    }

    return dentry_load_basic(ns_entry->thread_ctx, *dentry);
}

int dentry_check_load(FDIRDataThreadContext *thread_ctx,
        FDIRServerDentry *dentry)
{
    int result;

    if ((dentry->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_BASIC) == 0) {
        if ((result=dentry_load_basic(thread_ctx, dentry)) != 0) {
            return result;
        }
    }

    if (S_ISDIR(dentry->stat.mode) && (dentry->loaded_flags &
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
    string_t empty;
    string_t name;

    FC_SET_STRING_EX(empty, "", 0);
    if ((result=dentry_strdup(&ns_entry->thread_ctx->
                    dentry_context, &name, &empty)) != 0)
    {
        return result;
    }
    return dentry_load_one(ns_entry, parent, inode, &name, dentry);
}

static int dentry_load_all(DentryParentChildArray *parray)
{
    int result;
    DentryParentChildPair *pair;
    DentryParentChildPair *last;

    last = parray->pairs + parray->count - 1;
    for (pair=last; pair>=parray->pairs; pair--) {
        if ((result=dentry_load_one(pair->parent.dentry->ns_entry,
                        pair->parent.dentry, pair->current.inode,
                        NULL, &pair->current.dentry)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int dentry_load_inode(FDIRDataThreadContext *thread_ctx,
        FDIRNamespaceEntry *ns_entry, const int64_t inode,
        FDIRServerDentry **dentry)
{
    int result;
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
            break;
        }

        FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(db_fetch_ctx->
                    read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                        db_fetch_ctx->read_ctx.op_ctx));
        if ((result=dentry_serializer_extract_parent(db_fetch_ctx, &content,
                        pair->current.inode, &pair->parent.inode)) != 0)
        {
            break;
        }

        if (pair->parent.inode == 0) {  //orphan inode
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

        pair->parent.dentry = inode_index_find_dentry(pair->parent.inode);
        if (pair->parent.dentry != NULL) {
            break;
        }

        pair = parray->pairs + parray->count++;
        if (parray->count > parray->alloc) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", path's level is too large, "
                    "exceeds %d", __LINE__, parray->pairs->current.
                    inode, parray->alloc);
            result = EOVERFLOW;
            break;
        }

        pair->current.inode = pair->parent.inode;
        pair->current.dentry = NULL;
    } while (1);

    if (result == 0) {
        if (parray->count == 1) {
            result = dentry_load_one(ns_entry, pair->parent.dentry,
                    inode, NULL, dentry);
        } else if ((result=dentry_load_all(parray)) == 0) {
            *dentry = parray->pairs->current.dentry;
        }
    }

    fast_mblock_free_object(&PAIR_ARRAY_ALLOCATOR, parray);
    return result;
}

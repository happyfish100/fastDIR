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

static int dentry_load_children(FDIRServerDentry *dentry)
{
    int result;
    string_t content;
    FDIRDataThreadContext *thread_ctx;
    const id_name_array_t *id_name_array;
    const id_name_pair_t *pair;
    const id_name_pair_t *end;
    FDIRServerDentry *child;

    thread_ctx = dentry->ns_entry->thread_ctx;
    if ((result=STORAGE_ENGINE_FETCH_API(dentry->inode,
                    FDIR_PIECE_FIELD_INDEX_CHILDREN,
                    &thread_ctx->db_fetch_ctx.read_ctx)) != 0)
    {
        return result;
    }

    FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(thread_ctx->db_fetch_ctx.
                read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(thread_ctx->
                    db_fetch_ctx.read_ctx.op_ctx));
    if ((result=dentry_serializer_unpack_children(thread_ctx, &content,
                    dentry->inode, &id_name_array)) != 0)
    {
        return result;
    }

    dentry->children = uniq_skiplist_new(&thread_ctx->dentry_context.
            factory, DENTRY_SKIPLIST_INIT_LEVEL_COUNT);
    if (dentry->children == NULL) {
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
        child->parent = dentry;
        child->inode = pair->id;
        if ((result=dentry_strdup(&thread_ctx->dentry_context,
                        &child->name, &pair->name)) != 0)
        {
            return result;
        }

        if ((result=uniq_skiplist_insert(dentry->children, child)) == 0) {
            dentry->stat.nlink++;
        } else {
            return result;
        }
        __sync_add_and_fetch(&child->db_args->reffer_count, 1);
        child->ns_entry = child->ns_entry;
    }

    dentry->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_CHILDREN;
    return 0;
}

int dentry_load_one(FDIRDataThreadContext *thread_ctx,
        FDIRNamespaceEntry *ns_entry, FDIRServerDentry *parent,
        const int64_t inode, const string_t *name, FDIRServerDentry
        **dentry, DentrySerializerExtraFields *extra_fields)
{
    int result;
    string_t content;

    if ((result=STORAGE_ENGINE_FETCH_API(inode, FDIR_PIECE_FIELD_INDEX_BASIC,
                    &thread_ctx->db_fetch_ctx.read_ctx)) != 0)
    {
        return result;
    }

    *dentry = (FDIRServerDentry *)fast_mblock_alloc_object(
            &thread_ctx->dentry_context.dentry_allocator);
    if (*dentry == NULL) {
        return ENOMEM;
    }

    memset(*dentry, 0, sizeof(FDIRServerDentry));
    (*dentry)->inode = inode;
    (*dentry)->ns_entry = ns_entry;
    (*dentry)->parent = parent;
    if (name != NULL) {
        (*dentry)->name = *name;
    }

    FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(thread_ctx->
                db_fetch_ctx.read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                    thread_ctx->db_fetch_ctx.read_ctx.op_ctx));
    if ((result=dentry_serializer_unpack_basic(thread_ctx,
                    &content, *dentry, extra_fields)) != 0)
    {
        logCrit("file: "__FILE__", line: %d, "
                "dentry unpack basic info fail, "
                "program exit!", __LINE__);
        sf_terminate_myself();
        return result;
    }

    (*dentry)->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_BASIC;

    if (S_ISDIR((*dentry)->stat.mode)) {
        if ((result=dentry_load_children(*dentry)) != 0) {
            return result;
        }
    }

    return result;
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
            return result;
        }

        FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(db_fetch_ctx->
                    read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                        db_fetch_ctx->read_ctx.op_ctx));
        if ((result=dentry_serializer_extract_parent(db_fetch_ctx, &content,
                        pair->current.inode, &pair->parent.inode)) != 0)
        {
            return result;
        }

        if (pair->parent.inode == 0) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", invalid parent inode: 0",
                    __LINE__, pair->current.inode);
            return EINVAL;
        }

        pair->parent.dentry = inode_index_get_dentry(pair->parent.inode);
        if (pair->parent.dentry != NULL) {
            break;
        }

        pair = parray->pairs + parray->count++;
        if (parray->count > parray->alloc) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", path's level is too large, "
                    "exceeds %d", __LINE__, parray->pairs->current.
                    inode, parray->alloc);
            return EOVERFLOW;
        }

        pair->current.inode = pair->parent.inode;
        pair->current.dentry = NULL;
    } while (1);

    return 0;
}

int dentry_load(FDIRDataThreadContext *thread_ctx,
        FDIRNamespaceEntry *ns_entry, FDIRServerDentry *parent,
        const int64_t inode, const string_t *name, FDIRServerDentry **dentry)
{
    int result;
    string_t content;
    DentrySerializerExtraFields extra_fields;
    FDIRDBFetchContext *db_fetch_ctx;

    if (thread_ctx == NULL) {
        //TODO
        db_fetch_ctx = NULL;
        if ((result=STORAGE_ENGINE_FETCH_API(inode, FDIR_PIECE_FIELD_INDEX_BASIC,
                        &db_fetch_ctx->read_ctx)) != 0)
        {
            return result;
        }

        FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(db_fetch_ctx->
                    read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                        db_fetch_ctx->read_ctx.op_ctx));
        if ((result=dentry_serializer_extract_namespace(db_fetch_ctx,
                        &content, inode, &ns_entry)) != 0)
        {
            return result;
        }
        thread_ctx = ns_entry->thread_ctx;
    }

    if ((result=dentry_load_one(thread_ctx, ns_entry, parent, inode,
                    name, dentry, &extra_fields)) != 0)
    {
        return result;
    }

    if (FDIR_IS_DENTRY_HARD_LINK((*dentry)->stat.mode)) {
        if ((result=dentry_load_inode(thread_ctx, (*dentry)->ns_entry,
                        extra_fields.src_inode, &(*dentry)->src_dentry)) != 0)
        {
            return result;
        }
    }

    if (parent == NULL && extra_fields.parent_inode != 0) {
        if ((result=dentry_load_inode(thread_ctx, (*dentry)->ns_entry,
                        extra_fields.parent_inode, &(*dentry)->parent)) != 0)
        {
            return result;
        }
    }

    if ((*dentry)->parent != NULL) {
        //add to parent's children
    }

    return 0;
}

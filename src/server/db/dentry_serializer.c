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
#include "../server_global.h"
#include "../dentry.h"
#include "dentry_serializer.h"

#define DENTRY_FIELD_ID_INODE         1
#define DENTRY_FIELD_ID_PARENT        2  //parent inode
#define DENTRY_FIELD_ID_SUBNAME       3
#define DENTRY_FIELD_ID_SRC_INODE     5  //src inode for hard link
#define DENTRY_FIELD_ID_LINK          6
#define DENTRY_FIELD_ID_MODE         10
#define DENTRY_FIELD_ID_ATIME        11
#define DENTRY_FIELD_ID_BTIME        12
#define DENTRY_FIELD_ID_CTIME        13
#define DENTRY_FIELD_ID_MTIME        14
#define DENTRY_FIELD_ID_UID          15
#define DENTRY_FIELD_ID_GID          16
#define DENTRY_FIELD_ID_FILE_SIZE    17
#define DENTRY_FIELD_ID_ALLOC_SIZE   18
#define DENTRY_FIELD_ID_SPACE_END    19
#define DENTRY_FIELD_ID_NLINK        20
#define DENTRY_FIELD_ID_NAMESPACE_ID 30
#define DENTRY_FIELD_ID_XATTR       100
#define DENTRY_FIELD_ID_CHILDREN    101

#define FIXED_INODES_ARRAY_SIZE  1024

#define DEFAULT_PACKED_BUFFER_SIZE  1024

static const char *piece_field_names[] = {
    "basic", "children", "xattr"
};

DentrySerializerContext g_serializer_ctx;

static int buffer_init_func(void *element, void *init_args)
{
    FastBuffer *buffer;
    buffer = (FastBuffer *)element;
    return fast_buffer_init_ex(buffer, DEFAULT_PACKED_BUFFER_SIZE);
}

int dentry_serializer_init()
{
    const int min_bits = 2;
    const int max_bits = 16;
    const bool allow_duplication = false;
    int result;

    if ((result=fast_mblock_init_ex1(&g_serializer_ctx.buffer_allocator,
                    "packed-buffer", sizeof(FastBuffer), 1024, 0,
                    buffer_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=id_name_array_allocator_init(&ID_NAME_ARRAY_ALLOCATOR_CTX,
                    min_bits, max_bits)) != 0)
    {
        return result;
    }

    sorted_id_name_array_init(&ID_NAME_SORTED_ARRAY_CTX, allow_duplication);
    return 0;
}

void dentry_serializer_batch_free_buffer(FastBuffer **buffers,
            const int count)
{
    FastBuffer **buf;
    FastBuffer **end;

    end = buffers + count;
    for (buf=buffers; buf<end; buf++) {
        if ((*buf)->alloc_size > DEFAULT_PACKED_BUFFER_SIZE) {
            (*buf)->length = 0;  //reset data length
            fast_buffer_set_capacity(*buf, DEFAULT_PACKED_BUFFER_SIZE);
        }
    }

    fast_mblock_free_objects(&g_serializer_ctx.buffer_allocator,
            (void **)buffers, count);
}

static int pack_basic(const FDIRServerDentry *dentry, FastBuffer *buffer)
{
    int result;
    int64_t parent_inode;

    if ((result=sf_serializer_pack_int32(buffer,
                    DENTRY_FIELD_ID_NAMESPACE_ID,
                    dentry->ns_entry->id)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_int64(buffer,
                    DENTRY_FIELD_ID_INODE,
                    dentry->inode)) != 0)
    {
        return result;
    }

    parent_inode = (dentry->parent != NULL ? dentry->parent->inode : 0);
    if ((result=sf_serializer_pack_int64(buffer,
                    DENTRY_FIELD_ID_PARENT,
                    parent_inode)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_string(buffer,
                    DENTRY_FIELD_ID_SUBNAME,
                    &dentry->name)) != 0)
    {
        return result;
    }


    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        if ((result=sf_serializer_pack_int64(buffer,
                        DENTRY_FIELD_ID_SRC_INODE,
                        dentry->src_dentry->inode)) != 0)
        {
            return result;
        }
    } else if (S_ISLNK(dentry->stat.mode)) {
        if ((result=sf_serializer_pack_string(buffer,
                        DENTRY_FIELD_ID_LINK,
                        &dentry->link)) != 0)
        {
            return result;
        }
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_MODE,
                    dentry->stat.mode)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_ATIME,
                    dentry->stat.atime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_BTIME,
                    dentry->stat.btime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_CTIME,
                    dentry->stat.ctime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_MTIME,
                    dentry->stat.mtime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_UID,
                    dentry->stat.uid)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_GID,
                    dentry->stat.gid)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_FILE_SIZE,
                    dentry->stat.size)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_ALLOC_SIZE,
                    dentry->stat.alloc)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_SPACE_END,
                    dentry->stat.space_end)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_NLINK,
                    dentry->stat.nlink)) != 0)
    {
        return result;
    }

    return 0;
}

int dentry_serializer_pack(const FDIRServerDentry *dentry,
        const int field_index, FastBuffer **buffer)
{
    int result;

    if (field_index == FDIR_PIECE_FIELD_INDEX_CHILDREN) {
        if (dentry->db_args->children == NULL ||
                dentry->db_args->children->count == 0)
        {
            *buffer = NULL;
            return 0;
        }
    } else if (field_index == FDIR_PIECE_FIELD_INDEX_XATTR) {
        if (dentry->kv_array == NULL || dentry->kv_array->count == 0) {
            *buffer = NULL;
            return 0;
        }
    }

    *buffer = (FastBuffer *)fast_mblock_alloc_object(
            &g_serializer_ctx.buffer_allocator);
    if (*buffer == NULL) {
        return ENOMEM;
    }

    sf_serializer_pack_begin(*buffer);

    switch (field_index) {
        case FDIR_PIECE_FIELD_INDEX_BASIC:
            result = pack_basic(dentry, *buffer);
            break;
        case FDIR_PIECE_FIELD_INDEX_CHILDREN:
            if (S_ISDIR(dentry->stat.mode)) {
                result = sf_serializer_pack_id_name_array(*buffer,
                        DENTRY_FIELD_ID_CHILDREN,
                        dentry->db_args->children->elts,
                        dentry->db_args->children->count);
            } else {
                result = EINVAL;
            }
            break;
        case FDIR_PIECE_FIELD_INDEX_XATTR:
            result = sf_serializer_pack_map(*buffer,
                    DENTRY_FIELD_ID_XATTR,
                    dentry->kv_array->elts,
                    dentry->kv_array->count);
            break;
        default:
            result = EINVAL;
            break;
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "pack dentry %s fail, inode: %"PRId64", "
                "errno: %d, error info: %s", __LINE__,
                piece_field_names[field_index], dentry->inode,
                result, STRERROR(result));
        fast_mblock_free_object(&g_serializer_ctx.buffer_allocator, *buffer);
        *buffer = NULL;
        return result;
    }

    sf_serializer_pack_end(*buffer);
    return 0;
}

int dentry_serializer_unpack_basic(FDIRDataThreadContext *thread_ctx,
        const string_t *content, FDIRServerDentry *dentry, int64_t *src_inode)
{
    int result;
    int namespace_id;
    bool found_inode;
    bool found_mode;
    bool found_size;
    bool found_nlink;
    bool found_src_inode;
    bool found_lnk;
    const SFSerializerFieldValue *fv;

    if ((result=sf_serializer_unpack(&thread_ctx->
                    db_fetch_ctx.it, content)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "unpack inode %"PRId64" fail, error info: %s",
                __LINE__, dentry->inode, thread_ctx->db_fetch_ctx.
                it.error_info);
        return result;
    }

    found_inode = false;
    found_mode = false;
    found_size= false;
    found_nlink = false;
    found_src_inode = false;
    found_lnk = false;
    *src_inode = 0;
    namespace_id = -1;
    while ((fv=sf_serializer_next(&thread_ctx->db_fetch_ctx.it)) != NULL) {
        switch (fv->fid) {
            case DENTRY_FIELD_ID_INODE:
                if (fv->value.n != dentry->inode) {
                    logError("file: "__FILE__", line: %d, "
                            "unpacked inode: %"PRId64" != "
                            "input inode: %"PRId64, __LINE__,
                            fv->value.n, dentry->inode);
                    return EINVAL;
                }
                found_inode = true;
                break;
            case DENTRY_FIELD_ID_PARENT:
                if (dentry->parent != NULL) {
                    if (fv->value.n != dentry->parent->inode) {
                        logError("file: "__FILE__", line: %d, "
                                "inode: %"PRId64", unpacked parent: %"PRId64
                                " != input parent: %"PRId64, __LINE__,
                                dentry->inode, fv->value.n,
                                dentry->parent->inode);
                        return EINVAL;
                    }
                }
                break;
            case DENTRY_FIELD_ID_SUBNAME:
                if (dentry->name.str == NULL) {
                    if ((result=dentry_strdup(&thread_ctx->dentry_context,
                                    &dentry->name, &fv->value.s)) != 0)
                    {
                        return result;
                    }
                } else if (!fc_string_equals(&fv->value.s, &dentry->name)) {
                    logError("file: "__FILE__", line: %d, "
                            "inode: %"PRId64", unpacked name: %.*s"
                            " != input name: %.*s", __LINE__,
                            dentry->inode, fv->value.s.len,
                            fv->value.s.str, dentry->name.len,
                            dentry->name.str);
                    return EINVAL;
                }
                break;
            case DENTRY_FIELD_ID_SRC_INODE:
                *src_inode = fv->value.n;
                if (*src_inode == dentry->inode) {
                    logError("file: "__FILE__", line: %d, "
                            "inode: %"PRId64", the src inode equals "
                            "to me!", __LINE__, dentry->inode);
                    return EFAULT;
                }
                found_src_inode = true;
                break;
            case DENTRY_FIELD_ID_LINK:
                if ((result=dentry_strdup(&thread_ctx->dentry_context,
                                &dentry->link, &fv->value.s)) != 0)
                {
                    return result;
                }
                found_lnk = true;
                break;
            case DENTRY_FIELD_ID_NAMESPACE_ID:
                namespace_id = fv->value.n;
                if (dentry->ns_entry == NULL) {
                    if ((dentry->ns_entry=fdir_namespace_get_by_id(
                                    namespace_id)) == NULL)
                    {
                        logError("file: "__FILE__", line: %d, "
                                "inode: %"PRId64", unpacked namespace "
                                "id: %d not exist", __LINE__, dentry->inode,
                                (int)namespace_id);
                        return ENOENT;
                    }
                } else if (namespace_id != dentry->ns_entry->id) {
                    logError("file: "__FILE__", line: %d, "
                            "inode: %"PRId64", unpacked namespace id: %d "
                            "!= input id: %d", __LINE__, dentry->inode,
                            namespace_id, dentry->ns_entry->id);
                    return EINVAL;
                }
                break;
            case DENTRY_FIELD_ID_MODE:
                dentry->stat.mode = fv->value.n;
                found_mode = true;
                break;
            case DENTRY_FIELD_ID_ATIME:
                dentry->stat.atime = fv->value.n;
                break;
            case DENTRY_FIELD_ID_BTIME:
                dentry->stat.btime = fv->value.n;
                break;
            case DENTRY_FIELD_ID_CTIME:
                dentry->stat.ctime = fv->value.n;
                break;
            case DENTRY_FIELD_ID_MTIME:
                dentry->stat.mtime = fv->value.n;
                break;
            case DENTRY_FIELD_ID_UID:
                dentry->stat.uid = fv->value.n;
                break;
            case DENTRY_FIELD_ID_GID:
                dentry->stat.gid = fv->value.n;
                break;
            case DENTRY_FIELD_ID_FILE_SIZE:
                dentry->stat.size = fv->value.n;
                found_size = true;
                break;
            case DENTRY_FIELD_ID_ALLOC_SIZE:
                dentry->stat.alloc = fv->value.n;
                break;
            case DENTRY_FIELD_ID_SPACE_END:
                dentry->stat.space_end = fv->value.n;
                break;
            case DENTRY_FIELD_ID_NLINK:
                dentry->stat.nlink = fv->value.n;
                found_nlink = true;
                break;
            default:
                logError("file: "__FILE__", line: %d, "
                            "inode: %"PRId64" unkonw field index: %d",
                            __LINE__, dentry->inode, fv->fid);
                return EINVAL;
        }
    }

    if (!found_inode) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", field inode not exist",
                __LINE__, dentry->inode);
        return ENOENT;
    }

    if (!found_mode) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", field mode not exist",
                __LINE__, dentry->inode);
        return ENOENT;
    }

    if (!found_size) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", field size not exist",
                __LINE__, dentry->inode);
        return ENOENT;
    }

    if (!found_nlink) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", field nlink not exist",
                __LINE__, dentry->inode);
        return ENOENT;
    }

    if (namespace_id == -1) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", field namespace_id not exist",
                __LINE__, dentry->inode);
        return ENOENT;
    }

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        if (!(found_src_inode && *src_inode != 0)) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", field src_inode not exist",
                    __LINE__, dentry->inode);
            return ENOENT;
        }
    } else if (S_ISLNK(dentry->stat.mode)) {
        if (!(found_lnk && dentry->link.len > 0)) {
            logError("file: "__FILE__", line: %d, "
                    "inode: %"PRId64", field link not exist",
                    __LINE__, dentry->inode);
            return ENOENT;
        }
    }

    return 0;
}

int dentry_serializer_extract_namespace(FDIRDBFetchContext *db_fetch_ctx,
        const string_t *content, const int64_t inode,
        FDIRNamespaceEntry **ns_entry)
{
    int result;
    int namespace_id;
    const SFSerializerFieldValue *fv;

    if ((result=sf_serializer_unpack(&db_fetch_ctx->it, content)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "unpack inode %"PRId64" fail, error info: %s",
                __LINE__, inode, db_fetch_ctx->it.error_info);
        return result;
    }

    while ((fv=sf_serializer_next(&db_fetch_ctx->it)) != NULL) {
        if (fv->fid == DENTRY_FIELD_ID_NAMESPACE_ID) {
            namespace_id = fv->value.n;
            if ((*ns_entry=fdir_namespace_get_by_id(namespace_id)) != NULL) {
                return 0;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "inode: %"PRId64", unpacked namespace id: %d "
                        "not exist", __LINE__, inode, namespace_id);
                return ENOENT;
            }
        }
    }

    logError("file: "__FILE__", line: %d, "
            "inode: %"PRId64", field: namespace id "
            "not exist", __LINE__, inode);
    return ENOENT;
}

int dentry_serializer_extract_parent(FDIRDBFetchContext *db_fetch_ctx,
        const string_t *content, const int64_t inode, int64_t *parent_inode)
{
    int result;
    const SFSerializerFieldValue *fv;

    if ((result=sf_serializer_unpack(&db_fetch_ctx->it, content)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "unpack inode %"PRId64" fail, error info: %s",
                __LINE__, inode, db_fetch_ctx->it.error_info);
        return result;
    }

    while ((fv=sf_serializer_next(&db_fetch_ctx->it)) != NULL) {
        if (fv->fid == DENTRY_FIELD_ID_PARENT) {
            *parent_inode = fv->value.n;
            return 0;
        }
    }

    logError("file: "__FILE__", line: %d, "
            "inode: %"PRId64", field: parent not exist",
            __LINE__, inode);
    return ENOENT;
}

int dentry_serializer_unpack_children(FDIRDataThreadContext *thread_ctx,
        const string_t *content, const int64_t inode,
        const id_name_array_t **array)
{
    int result;
    const SFSerializerFieldValue *fv;

    if ((result=sf_serializer_unpack(&thread_ctx->
                    db_fetch_ctx.it, content)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", unpack children fail, error info: %s",
                __LINE__, inode, thread_ctx->db_fetch_ctx.it.error_info);
        return result;
    }

    if ((fv=sf_serializer_next(&thread_ctx->db_fetch_ctx.it)) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", children field not exist",
                __LINE__, inode);
        return ENOENT;
    }

    if (fv->fid != DENTRY_FIELD_ID_CHILDREN) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", the first fid: %d is NOT children, "
                "expected fid: %d", __LINE__, inode, fv->fid,
                DENTRY_FIELD_ID_CHILDREN);
        return EINVAL;
    }
    if (fv->type != sf_serializer_value_type_id_name_array) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", the field type: %d is invalid, "
                "expected type: %d", __LINE__, inode, fv->type,
                sf_serializer_value_type_id_name_array);
        return EINVAL;
    }

    *array = &fv->value.id_name_array;
    return 0;
}

int dentry_serializer_unpack_xattr(FDIRDataThreadContext *thread_ctx,
        const string_t *content, const int64_t inode,
        const key_value_array_t **array)
{
    int result;
    const SFSerializerFieldValue *fv;

    if ((result=sf_serializer_unpack(&thread_ctx->
                    db_fetch_ctx.it, content)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", unpack xattr fail, error info: %s",
                __LINE__, inode, thread_ctx->db_fetch_ctx.it.error_info);
        return result;
    }

    if ((fv=sf_serializer_next(&thread_ctx->db_fetch_ctx.it)) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", xattr field not exist",
                __LINE__, inode);
        return ENOENT;
    }

    if (fv->fid != DENTRY_FIELD_ID_XATTR) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", the first fid: %d is NOT xattr, "
                "expected fid: %d", __LINE__, inode, fv->fid,
                DENTRY_FIELD_ID_XATTR);
        return EINVAL;
    }
    if (fv->type != sf_serializer_value_type_map) {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", the field type: %d is invalid, "
                "expected type: %d", __LINE__, inode, fv->type,
                sf_serializer_value_type_map);
        return EINVAL;
    }

    *array = &fv->value.kv_array;
    return 0;
}

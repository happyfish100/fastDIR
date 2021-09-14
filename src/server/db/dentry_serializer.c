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
#define DENTRY_FIELD_ID_XATTR        30
#define DENTRY_FIELD_ID_HASH_CODE    40
#define DENTRY_FIELD_ID_CHILDREN     50

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
    const bool allow_duplicate = false;
    int result;

    if ((result=fast_mblock_init_ex1(&g_serializer_ctx.buffer_allocator,
                    "packed-buffer", sizeof(FastBuffer), 1024, 0,
                    buffer_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=i64_array_allocator_init(&I64_ARRAY_ALLOCATOR_CTX,
                    min_bits, max_bits)) != 0)
    {
        return result;
    }

    sorted_i64_array_init(&I64_SORTED_ARRAY_CTX, allow_duplicate);
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

    if ((result=sf_serializer_pack_int32(buffer,
                    DENTRY_FIELD_ID_HASH_CODE,
                    dentry->hash_code)) != 0)
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
        if (dentry->db_args->children == NULL) {
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
                result = sf_serializer_pack_int64_array(*buffer,
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
        return result;
    }

    sf_serializer_pack_end(*buffer);
    return 0;
}

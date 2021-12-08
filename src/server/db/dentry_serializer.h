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


#ifndef _FDIR_DENTRY_SERIALIZER_H
#define _FDIR_DENTRY_SERIALIZER_H

#include "sf/sf_serializer.h"
#include "../server_types.h"
#include "../ns_manager.h"
#include "../data_thread.h"
#include "change_notify.h"

typedef struct {
    struct fast_mblock_man buffer_allocator;
    struct array_allocator_context id_name_array_allocator_ctx;
    struct sorted_array_context id_name_sorted_array_ctx;
} DentrySerializerContext;

#define ID_NAME_ARRAY_ALLOCATOR_CTX g_serializer_ctx.id_name_array_allocator_ctx
#define ID_NAME_SORTED_ARRAY_CTX    g_serializer_ctx.id_name_sorted_array_ctx

#ifdef __cplusplus
extern "C" {
#endif

    extern DentrySerializerContext g_serializer_ctx;

    int dentry_serializer_init();
    void dentry_serializer_destroy();

    int dentry_serializer_pack(const FDIRServerDentry *dentry,
            const int field_index, FastBuffer **buffer);


    static inline FastBuffer *dentry_serializer_alloc_buffer(
            const int capacity)
    {
        FastBuffer *buffer;
        if ((buffer=(FastBuffer *)fast_mblock_alloc_object(
                        &g_serializer_ctx.buffer_allocator)) == NULL)
        {
            return NULL;
        }

        buffer->length = 0;
        if (fast_buffer_check_capacity(buffer, capacity) != 0) {
            fast_mblock_free_object(&g_serializer_ctx.
                    buffer_allocator, buffer);
            return NULL;
        }
        return buffer;
    }

    static inline FastBuffer *dentry_serializer_to_buffer(const string_t *s)
    {
        FastBuffer *buffer;

        if ((buffer=dentry_serializer_alloc_buffer(s->len)) == NULL) {
            return NULL;
        }

        memcpy(buffer->data, s->str, s->len);
        buffer->length = s->len;
        return buffer;
    }

    void dentry_serializer_batch_free_buffer(FastBuffer **buffers,
            const int count);

    int dentry_serializer_unpack_basic(FDIRDataThreadContext *thread_ctx,
            const string_t *content, FDIRServerDentry *dentry,
            int64_t *src_inode);

    int dentry_serializer_extract_namespace(FDIRDBFetchContext *db_fetch_ctx,
            const string_t *content, const int64_t inode,
            FDIRNamespaceEntry **ns_entry);

    int dentry_serializer_extract_parent(FDIRDBFetchContext *db_fetch_ctx,
            const string_t *content, const int64_t inode,
            int64_t *parent_inode);

    int dentry_serializer_unpack_children(FDIRDataThreadContext *thread_ctx,
            const string_t *content, const int64_t inode,
            const id_name_array_t **array);

    int dentry_serializer_unpack_xattr(FDIRDataThreadContext *thread_ctx,
            const string_t *content, const int64_t inode,
            const key_value_array_t **array);

#ifdef __cplusplus
}
#endif

#endif

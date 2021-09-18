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
#include "change_notify.h"

typedef struct
{
    struct fast_mblock_man buffer_allocator;
    struct array_allocator_context i64_array_allocator_ctx;
    struct sorted_array_context i64_sorted_array_ctx;
} DentrySerializerContext;

#define I64_ARRAY_ALLOCATOR_CTX g_serializer_ctx.i64_array_allocator_ctx
#define I64_SORTED_ARRAY_CTX    g_serializer_ctx.i64_sorted_array_ctx

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

    void dentry_serializer_batch_free_buffer(FastBuffer **buffers,
            const int count);

#ifdef __cplusplus
}
#endif

#endif

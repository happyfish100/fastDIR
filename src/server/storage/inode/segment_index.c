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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/fc_atomic.h"
#include "binlog_index.h"
#include "binlog_reader.h"
#include "inode_index_array.h"
#include "segment_index.h"

typedef struct inode_segment_index_info {
    int64_t binlog_id;
    struct {
        volatile uint64_t first;
        volatile uint64_t last;
        FDIRStorageInodeIndexArray array;
        union {
            int value;
            struct {
                bool in_memory : 1;
                bool dirty : 1;
            };
        } flags;
    } inodes;
    time_t last_access_time;
    pthread_mutex_t lock;
} InodeSegmentIndexInfo;

typedef struct inode_segment_index_array {
    InodeSegmentIndexInfo **segments;
    volatile int count;
    int alloc;
} InodeSegmentIndexArray;

typedef struct inode_segment_index_context {
    volatile InodeSegmentIndexArray *si_array;
    struct fast_mblock_man array_allocator;
    struct fast_mblock_man segment_allocator;
} InodeSegmentIndexContext;

static InodeSegmentIndexContext segment_index_ctx;

static int segment_alloc_init_func(InodeSegmentIndexInfo *element, void *args)
{
    return init_pthread_lock(&element->lock);
}

static int alloc_segments(InodeSegmentIndexArray *array, const int size)
{
    array->alloc = 128;
    while (array->alloc < size) {
        array->alloc *= 2;
    }

    array->segments = (InodeSegmentIndexInfo **)fc_malloc(
            sizeof(InodeSegmentIndexInfo *) * array->alloc);
    if (array->segments == NULL) {
        return ENOMEM;
    }

    return 0;
}

static int dump(FDIRInodeBinlogIndexContext *bctx)
{
    int result;
    FDIRInodeBinlogIndexInfo *binlog;
    FDIRInodeBinlogIndexInfo *end;
    InodeSegmentIndexArray *si_array;
    InodeSegmentIndexInfo **segment;

    si_array = (InodeSegmentIndexArray *)fast_mblock_alloc_object(
            &segment_index_ctx.array_allocator);
    if (si_array == NULL) {
        return ENOMEM;
    }

    if ((result=alloc_segments(si_array, bctx->index_array.count)) != 0) {
        return result;
    }

    end = bctx->index_array.indexes + bctx->index_array.count;
    for (binlog=bctx->index_array.indexes, segment=si_array->segments;
            binlog<end; binlog++, segment++)
    {
        *segment = (InodeSegmentIndexInfo *)fast_mblock_alloc_object(
                &segment_index_ctx.segment_allocator);
        if (*segment == NULL) {
            return ENOMEM;
        }

        (*segment)->binlog_id = binlog->binlog_id;
        (*segment)->inodes.first = binlog->inodes.first;
        (*segment)->inodes.last = binlog->inodes.last;
    }

    si_array->count = segment - si_array->segments;
    __sync_bool_compare_and_swap(&segment_index_ctx.si_array, NULL, si_array);
    return 0;
}

int inode_segment_index_init()
{
    int result;
    FDIRInodeBinlogIndexContext binlog_index_ctx;

    if ((result=fast_mblock_init_ex1(&segment_index_ctx.array_allocator,
                    "segment-index-array", sizeof(InodeSegmentIndexArray),
                    1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }
    if ((result=fast_mblock_init_ex1(&segment_index_ctx.segment_allocator,
                    "segment-index-info", sizeof(InodeSegmentIndexInfo),
                    8 * 1024, 0, (fast_mblock_alloc_init_func)
                    segment_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=binlog_index_load(&binlog_index_ctx)) != 0) {
        return result;
    }

    result = dump(&binlog_index_ctx);
    binlog_index_free(&binlog_index_ctx);
    return result;
}

static int segment_index_compare(const InodeSegmentIndexInfo **segment1,
        const InodeSegmentIndexInfo **segment2)
{
    int64_t sub;
    sub = (*segment1)->inodes.first - (*segment2)->inodes.first;
    if (sub < 0) {
        if ((*segment2)->inodes.first <= (*segment1)->inodes.last) {
            return 0;
        }
        return -1;
    } else if (sub > 0) {
        if ((*segment1)->inodes.first <= (*segment2)->inodes.last) {
            return 0;
        }
        return 1;
    } else {
        return 0;
    }
}

static InodeSegmentIndexInfo *find(const uint64_t inode)
{
    volatile InodeSegmentIndexArray *si_array;
    struct {
        InodeSegmentIndexInfo holder;
        InodeSegmentIndexInfo *ptr;
    } target;
    InodeSegmentIndexInfo **found;

    target.holder.inodes.first = target.holder.inodes.last = inode;
    target.ptr = &target.holder;
    si_array = FC_ATOMIC_GET(segment_index_ctx.si_array);

    found = (InodeSegmentIndexInfo **)bsearch(&target.ptr,
            si_array->segments, si_array->count,
            sizeof(InodeSegmentIndexInfo *),
            (int (*)(const void *, const void *))segment_index_compare);
    return (found != NULL ? *found : NULL);
}

int inode_segment_index_add(const FDIRStorageInodeIndexInfo *inode)
{
    InodeSegmentIndexInfo *segment;
    int result;

    //TODO
    if ((segment=find(inode->inode)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&segment->lock);
    do {
        if (!segment->inodes.flags.in_memory) {
            if ((result=binlog_reader_load(segment->binlog_id,
                            &segment->inodes.array)) != 0)
            {
                break;
            }
            segment->inodes.flags.in_memory = true;
        }
        result = inode_index_array_add(&segment->inodes.array, inode);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&segment->lock);

    return result;
}

int inode_segment_index_delete(const uint64_t inode)
{
    InodeSegmentIndexInfo *segment;
    int result;

    if ((segment=find(inode)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&segment->lock);
    if (segment->inodes.flags.in_memory) {
        result = inode_index_array_delete_ex(
                &segment->inodes.array, inode, true);
    } else {
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lock);

    return result;
}

int inode_segment_index_find(FDIRStorageInodeIndexInfo *inode)
{
    InodeSegmentIndexInfo *segment;
    int result;

    if ((segment=find(inode->inode)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&segment->lock);
    do {
        if (!segment->inodes.flags.in_memory) {
            if ((result=binlog_reader_load(segment->binlog_id,
                            &segment->inodes.array)) != 0)
            {
                break;
            }
            segment->inodes.flags.in_memory = true;
        }
        result = inode_index_array_find(&segment->inodes.array, inode);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&segment->lock);

    return result;
}

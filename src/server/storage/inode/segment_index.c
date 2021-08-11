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
#include "../../server_global.h"
#include "binlog_index.h"
#include "binlog_reader.h"
#include "binlog_writer.h"
#include "inode_index_array.h"
#include "segment_index.h"

#define BINLOG_INDEX_FILENAME "binlog_index.dat"

#define BINLOG_INDEX_ITEM_CURRENT_WRITE     "current_write"

typedef struct inode_segment_index_array {
    FDIRInodeSegmentIndexInfo **segments;
    volatile int count;
    int alloc;
} InodeSegmentIndexArray;

typedef struct inode_segment_index_context {
    struct {
        int write_index;
        int inode_count;
        FDIRInodeSegmentIndexInfo *segment;
    } current_binlog;

    volatile InodeSegmentIndexArray *si_array;
    struct fast_mblock_man array_allocator;
    struct fast_mblock_man segment_allocator;
    volatile int64_t version;
} InodeSegmentIndexContext;

static InodeSegmentIndexContext segment_index_ctx;

static int write_to_binlog_index(const int current_write_index)
{
    char full_filename[PATH_MAX];
    char buff[256];
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    len = sprintf(buff, "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE, current_write_index);
    return safeWriteToFile(full_filename, buff, len);
}

static int get_binlog_index_from_file()
{
    char full_filename[PATH_MAX];
    IniContext iniContext;
    int result;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return write_to_binlog_index(segment_index_ctx.
                    current_binlog.write_index);
        } else {
            return errno != 0 ? errno : EPERM;
        }
    }

    memset(&iniContext, 0, sizeof(IniContext));
    if ((result=iniLoadFromFile(full_filename, &iniContext)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, "
                "error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    segment_index_ctx.current_binlog.write_index = iniGetIntValue(
            NULL, BINLOG_INDEX_ITEM_CURRENT_WRITE, &iniContext, 0);

    iniFreeContext(&iniContext);
    return 0;
}

static int segment_alloc_init_func(FDIRInodeSegmentIndexInfo *element, void *args)
{
    return init_pthread_lock(&element->lock);
}

static int alloc_segments(InodeSegmentIndexArray *array, const int size)
{
    array->alloc = 128;
    while (array->alloc < size) {
        array->alloc *= 2;
    }

    array->segments = (FDIRInodeSegmentIndexInfo **)fc_malloc(
            sizeof(FDIRInodeSegmentIndexInfo *) * array->alloc);
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
    FDIRInodeSegmentIndexInfo **segment;

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
        *segment = (FDIRInodeSegmentIndexInfo *)fast_mblock_alloc_object(
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
                    "segment-index-info", sizeof(FDIRInodeSegmentIndexInfo),
                    8 * 1024, 0, (fast_mblock_alloc_init_func)
                    segment_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    if ((result=binlog_index_load(&binlog_index_ctx)) != 0) {
        return result;
    }

    result = dump(&binlog_index_ctx);
    binlog_index_free(&binlog_index_ctx);
    return result;
}

static int segment_index_compare(const FDIRInodeSegmentIndexInfo **segment1,
        const FDIRInodeSegmentIndexInfo **segment2)
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

static FDIRInodeSegmentIndexInfo *find(const uint64_t inode)
{
    volatile InodeSegmentIndexArray *si_array;
    struct {
        FDIRInodeSegmentIndexInfo holder;
        FDIRInodeSegmentIndexInfo *ptr;
    } target;
    FDIRInodeSegmentIndexInfo **found;

    target.holder.inodes.first = target.holder.inodes.last = inode;
    target.ptr = &target.holder;
    si_array = FC_ATOMIC_GET(segment_index_ctx.si_array);

    found = (FDIRInodeSegmentIndexInfo **)bsearch(&target.ptr,
            si_array->segments, si_array->count,
            sizeof(FDIRInodeSegmentIndexInfo *),
            (int (*)(const void *, const void *))segment_index_compare);
    return (found != NULL ? *found : NULL);
}

int inode_segment_index_add(const FDIRStorageInodeIndexInfo *inode)
{
    FDIRInodeSegmentIndexInfo *segment;

    //TODO
    if ((segment=find(inode->inode)) == NULL) {
        return ENOENT;
    }

    //segment_index_ctx.current_binlog.segment
    //segment_index_ctx.current_binlog.write_index
    //segment_index_ctx.current_binlog.inode_count

    return inode_binlog_writer_log(segment,
            inode_index_op_type_create, inode); 
}

int inode_segment_index_delete(const uint64_t inode)
{
    FDIRInodeSegmentIndexInfo *segment;
    FDIRStorageInodeIndexInfo inode_index;

    if ((segment=find(inode)) == NULL) {
        return ENOENT;
    }

    inode_index.inode = inode;
    return inode_binlog_writer_log(segment,
            inode_index_op_type_remove, &inode_index);
}

int inode_segment_index_find(FDIRStorageInodeIndexInfo *inode)
{
    FDIRInodeSegmentIndexInfo *segment;
    int result;

    if ((segment=find(inode->inode)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&segment->lock);
    do {
        if (!segment->inodes.flags.in_memory) {
            if ((result=inode_binlog_writer_load(segment)) != 0) {
                break;
            }
            segment->inodes.flags.in_memory = true;
        }
        result = inode_index_array_find(&segment->inodes.array, inode);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&segment->lock);

    return result;
}

int inode_segment_index_update(FDIRInodeSegmentIndexInfo *segment,
        FDIRInodeBinlogRecord **records, const int count)
{
    int result;
    FDIRInodeBinlogRecord **record;
    FDIRInodeBinlogRecord **end;

    PTHREAD_MUTEX_LOCK(&segment->lock);
    if (segment->inodes.flags.in_memory) {
        end = records + count;
        for (record=records; record<end; record++) {
            if ((*record)->op_type == inode_index_op_type_create) {
                result = inode_index_array_add(&segment->inodes.array,
                        &(*record)->inode_index);
            } else {
                if ((result=inode_index_array_delete(&segment->inodes.array,
                                (*record)->inode_index.inode)) == 0)
                {
                    if (2 * segment->inodes.array.counts.deleted >=
                            segment->inodes.array.counts.total)
                    {
                        result = inode_binlog_writer_shrink(segment);
                    }
                }
            }
            if (result != 0) {
                if (result == ENOENT) {
                    result = 0;
                } else {
                    break;
                }
            }
        }
    } else {
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lock);

    return result;
}

int inode_segment_index_shrink(FDIRInodeSegmentIndexInfo *segment)
{
    int result;

    PTHREAD_MUTEX_LOCK(&segment->lock);
    do {
        if (!segment->inodes.flags.in_memory) {
            if ((result=binlog_reader_load(segment)) != 0) {
                break;
            }
            segment->inodes.flags.in_memory = true;
        }

        result = inode_index_array_check_shrink(&segment->inodes.array);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&segment->lock);

    return result;
}

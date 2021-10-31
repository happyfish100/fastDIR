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
#include "fastcommon/locked_list.h"
#include "../storage_global.h"
#include "binlog_index.h"
#include "binlog_reader.h"
#include "binlog_writer.h"
#include "inode_index_array.h"
#include "bid_journal.h"
#include "segment_index.h"

#define BINLOG_INDEX_FILENAME      "binlog_index.dat"
#define BINLOG_INDEX_ITEM_CURRENT_WRITE     "current_write"

typedef struct inode_segment_index_array {
    FDIRInodeSegmentIndexInfo **segments;
    int count;
    int alloc;
} InodeSegmentIndexArray;

typedef struct inode_segment_index_context {
    struct {
        int64_t binlog_id;
        FDIRInodeSegmentIndexInfo *segment;
    } current_binlog;

    volatile int64_t current_version;

    InodeSegmentIndexArray si_array;
    struct fast_mblock_man segment_allocator;
    pthread_rwlock_t rwlock;
    FCLockedList fifo; //element: FDIRInodeSegmentIndexInfo
} InodeSegmentIndexContext;

static InodeSegmentIndexContext segment_index_ctx = {
    {1, NULL}, 0, {NULL, 0, 0}
};

static int write_to_binlog_index()
{
    char full_filename[PATH_MAX];
    char buff[256];
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    len = sprintf(buff, "%s=%"PRId64"\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE,
            segment_index_ctx.current_binlog.binlog_id);
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
            return write_to_binlog_index();
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

    segment_index_ctx.current_binlog.binlog_id = iniGetInt64Value(
            NULL, BINLOG_INDEX_ITEM_CURRENT_WRITE, &iniContext, 0);

    iniFreeContext(&iniContext);
    return 0;
}

static int segment_alloc_init_func(FDIRInodeSegmentIndexInfo
        *element, void *args)
{
    return init_pthread_lock_cond_pair(&element->lcp);
}

static int check_alloc_segments(InodeSegmentIndexArray *array, const int size)
{
    FDIRInodeSegmentIndexInfo **segments;
    int alloc;

    if (!(size > 0 && array->alloc >= size)) {
        alloc = 128;
        while (alloc < size) {
            alloc *= 2;
        }

        segments = (FDIRInodeSegmentIndexInfo **)fc_malloc(
                sizeof(FDIRInodeSegmentIndexInfo *) * alloc);
        if (segments == NULL) {
            return ENOMEM;
        }

        if (array->count > 0) {
            memcpy(segments, array->segments, sizeof(
                        FDIRInodeSegmentIndexInfo *) * array->count);
            free(array->segments);
        }

        array->segments = segments;
        array->alloc = alloc;
    }

    return 0;
}

static int convert_to_segement_array(SFBinlogIndexContext *bctx)
{
    int result;
    FDIRInodeBinlogIndexInfo *binlog;
    FDIRInodeBinlogIndexInfo *end;
    InodeSegmentIndexArray *si_array;
    FDIRInodeSegmentIndexInfo **segment;

    si_array = &segment_index_ctx.si_array;
    if ((result=check_alloc_segments(si_array, bctx->
                    index_array.count)) != 0)
    {
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
        (*segment)->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_CLEAN;
    }

    si_array->count = segment - si_array->segments;
    return 0;
}

static int segment_index_compare_bid(const FDIRInodeSegmentIndexInfo
        **segment1, const FDIRInodeSegmentIndexInfo **segment2)
{
    return fc_compare_int64((*segment1)->binlog_id, (*segment2)->binlog_id);
}

static FDIRInodeSegmentIndexInfo *find_segment_by_bid(const uint64_t binlog_id)
{
    struct {
        FDIRInodeSegmentIndexInfo holder;
        FDIRInodeSegmentIndexInfo *ptr;
    } target;
    FDIRInodeSegmentIndexInfo **found;

    target.holder.binlog_id = binlog_id;
    target.ptr = &target.holder;
    found = (FDIRInodeSegmentIndexInfo **)bsearch(&target.ptr,
            segment_index_ctx.si_array.segments,
            segment_index_ctx.si_array.count,
            sizeof(FDIRInodeSegmentIndexInfo *),
            (int (*)(const void *, const void *))
            segment_index_compare_bid);
    return (found != NULL ? *found : NULL);
}

static int segment_array_add(const uint64_t binlog_id,
        const int64_t first_inode, const int64_t last_inode)
{
    int result;
    InodeSegmentIndexArray *si_array;
    FDIRInodeSegmentIndexInfo *segment;

    segment = (FDIRInodeSegmentIndexInfo *)fast_mblock_alloc_object(
            &segment_index_ctx.segment_allocator);
    if (segment == NULL) {
        return ENOMEM;
    }

    si_array = &segment_index_ctx.si_array;
    if ((result=check_alloc_segments(si_array, si_array->count + 1)) != 0) {
        return result;
    }

    segment->binlog_id = binlog_id;
    segment->inodes.first = first_inode;
    segment->inodes.last = last_inode;
    segment->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_CLEAN;
    si_array->segments[si_array->count++] = segment;

    return 0;
}

static int set_last_segment_inode(uint64_t *last_bid)
{
    int result;
    int64_t last_inode;
    FDIRInodeSegmentIndexInfo *segment;

    if (segment_index_ctx.si_array.count == 0) {
        return 0;
    }

    segment = segment_index_ctx.si_array.segments[
        segment_index_ctx.si_array.count - 1];
    if (segment->binlog_id == *last_bid) {
        return 0;
    }

    *last_bid = segment->binlog_id;
    if ((result=inode_binlog_reader_get_last_inode(segment->
                    binlog_id, &last_inode)) != 0)
    {
        return result == ENOENT ? 0 : result;
    }

    segment->inodes.last = last_inode;
    return 0;
}

static int replay_with_bid_journal()
{
    int result;
    FDIRInodeBidJournalArray jarray;
    FDIRInodeBinlogIdJournal *journal;
    FDIRInodeBinlogIdJournal *end;
    FDIRInodeSegmentIndexInfo *segment;
    int64_t first_inode;
    int64_t last_inode;

    result = bid_journal_fetch(&jarray, g_binlog_index_ctx.last_version + 1);
    if (result != 0) {
        return (result == ENOENT ? 0 : result);
    }

    end = jarray.records + jarray.count;
    for (journal=jarray.records; journal<end; journal++) {
        segment = find_segment_by_bid(journal->binlog_id);
        if (journal->op_type == inode_binlog_id_op_type_create) {
            if (segment == NULL) {
                result = inode_binlog_reader_get_first_inode(
                        journal->binlog_id, &first_inode);
                if (result != 0) {
                    if (result == ENOENT) {
                        result = 0;
                        continue;
                    }
                    break;
                }
                if ((result=inode_binlog_reader_get_last_inode(journal->
                                binlog_id, &last_inode)) != 0)
                {
                    break;
                }
                if ((result=segment_array_add(journal->binlog_id,
                                first_inode, last_inode)) != 0)
                {
                    break;
                }
            }
        } else {
            if (segment != NULL) {
                segment->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_READY;
            }
        }
    }

    bid_journal_free(&jarray);
    return result;
}

static int segment_array_inc(const uint64_t first_inode)
{
    int result;
    InodeSegmentIndexArray *si_array;
    FDIRInodeSegmentIndexInfo *segment;

    segment = (FDIRInodeSegmentIndexInfo *)fast_mblock_alloc_object(
            &segment_index_ctx.segment_allocator);
    if (segment == NULL) {
        return ENOMEM;
    }

    if ((result=inode_index_array_alloc(&segment->inodes.array,
                FDIR_STORAGE_BATCH_INODE_COUNT)) != 0)
    {
        return result;
    }

    si_array = &segment_index_ctx.si_array;
    if ((result=check_alloc_segments(si_array, si_array->count + 1)) != 0) {
        return result;
    }

    if (si_array->count > 0) {
        segment_index_ctx.current_binlog.binlog_id++;
        if ((result=write_to_binlog_index()) != 0) {
            return result;
        }
    }

    segment->binlog_id = segment_index_ctx.current_binlog.binlog_id;
    segment->inodes.first = first_inode;
    segment->inodes.last = first_inode;
    segment->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_READY;
    si_array->segments[si_array->count++] = segment;
    segment_index_ctx.current_binlog.segment = segment;

    locked_list_add_tail(&segment->dlink, &segment_index_ctx.fifo);
    return bid_journal_log(segment->binlog_id,
            inode_binlog_id_op_type_create);
}

static int convert_to_index_array(SFBinlogIndexContext *bctx)
{
    int result;
    InodeSegmentIndexArray *si_array;
    FDIRInodeSegmentIndexInfo **segment;
    FDIRInodeSegmentIndexInfo **end;
    FDIRInodeBinlogIndexInfo *binlog;

    result = 0;
    si_array = &segment_index_ctx.si_array;

    PTHREAD_RWLOCK_RDLOCK(&segment_index_ctx.rwlock);
    end = si_array->segments + si_array->count;
    for (segment=si_array->segments,
            binlog=bctx->index_array.indexes;
            segment<end; segment++)
    {
        if ((*segment)->inodes.status == FDIR_STORAGE_SEGMENT_STATUS_READY
                && (*segment)->inodes.array.counts.total == 0)
        {
            continue;
        }

        if (bctx->index_array.count >= bctx->index_array.alloc) {
            if ((result=binlog_index_expand()) != 0) {
                break;
            }
            binlog = bctx->index_array.indexes + bctx->index_array.count;
        }

        binlog->binlog_id = (*segment)->binlog_id;
        binlog->inodes.first = (*segment)->inodes.first;
        binlog->inodes.last = (*segment)->inodes.last;
        binlog++;
        bctx->index_array.count++;
    }
    PTHREAD_RWLOCK_RDLOCK(&segment_index_ctx.rwlock);

    return result;
}

static int set_current_binlog()
{
    FDIRInodeSegmentIndexInfo *segment;

    if (segment_index_ctx.si_array.count > 0) {
        segment = segment_index_ctx.si_array.segments[
            segment_index_ctx.si_array.count - 1];
        if (segment->binlog_id == segment_index_ctx.
                current_binlog.binlog_id)
        {
            segment_index_ctx.current_binlog.segment = segment;
            return 0;
        }
    }

    return 0;
}

static int binlog_index_dump(void *args)
{
    int64_t current_version;
    int result;

    current_version = bid_journal_current_version();
    if (g_binlog_index_ctx.last_version == current_version) {
        return 0;
    }

    if ((result=convert_to_index_array(&g_binlog_index_ctx)) != 0) {
        return result;
    }

    g_binlog_index_ctx.last_version = current_version;
    return binlog_index_save();
}

int inode_segment_index_init()
{
    int result;
    uint64_t last_bid;
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntries[1];

    if ((result=init_pthread_rwlock(&segment_index_ctx.rwlock)) != 0) {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&segment_index_ctx.segment_allocator,
                    "segment-index-info", sizeof(FDIRInodeSegmentIndexInfo),
                    8 * 1024, 0, (fast_mblock_alloc_init_func)
                    segment_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=locked_list_init(&segment_index_ctx.fifo)) != 0) {
        return result;
    }

    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    binlog_index_init();
    if ((result=binlog_index_load()) != 0) {
        return result;
    }

    last_bid = 0;
    set_last_segment_inode(&last_bid);
    if ((result=convert_to_segement_array(&g_binlog_index_ctx)) != 0) {
        return result;
    }

    if ((result=replay_with_bid_journal()) != 0) {
        return result;
    }
    set_last_segment_inode(&last_bid);

    if ((result=set_current_binlog()) != 0) {
        return result;
    }

    INIT_SCHEDULE_ENTRY_EX(scheduleEntries[0], sched_generate_next_id(),
            INDEX_DUMP_BASE_TIME, INDEX_DUMP_INTERVAL,
            binlog_index_dump, NULL);
    scheduleArray.entries = scheduleEntries;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}

static int segment_index_compare_inode(const FDIRInodeSegmentIndexInfo
        **segment1, const FDIRInodeSegmentIndexInfo **segment2)
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

static FDIRInodeSegmentIndexInfo *find_segment_by_inode(const uint64_t inode)
{
    struct {
        FDIRInodeSegmentIndexInfo holder;
        FDIRInodeSegmentIndexInfo *ptr;
    } target;
    FDIRInodeSegmentIndexInfo **found;

    target.holder.inodes.first = target.holder.inodes.last = inode;
    target.ptr = &target.holder;

    PTHREAD_RWLOCK_RDLOCK(&segment_index_ctx.rwlock);
    found = (FDIRInodeSegmentIndexInfo **)bsearch(&target.ptr,
            segment_index_ctx.si_array.segments,
            segment_index_ctx.si_array.count,
            sizeof(FDIRInodeSegmentIndexInfo *),
            (int (*)(const void *, const void *))
            segment_index_compare_inode);
    PTHREAD_RWLOCK_UNLOCK(&segment_index_ctx.rwlock);
    return (found != NULL ? *found : NULL);
}

static int check_load(FDIRInodeSegmentIndexInfo *segment)
{
    int result;

    result = 0;
    while (result == 0 && segment->inodes.status !=
            FDIR_STORAGE_SEGMENT_STATUS_READY)
    {
        switch (segment->inodes.status) {
            case FDIR_STORAGE_SEGMENT_STATUS_CLEAN:
                segment->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_LOADING;
                if ((result=inode_binlog_reader_load(segment)) != 0) {
                    break;
                }
                segment->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_READY;
                locked_list_add_tail(&segment->dlink, &segment_index_ctx.fifo);
                pthread_cond_broadcast(&segment->lcp.cond);
                break;
            case FDIR_STORAGE_SEGMENT_STATUS_LOADING:
                while (segment->inodes.status ==
                        FDIR_STORAGE_SEGMENT_STATUS_LOADING)
                {
                    pthread_cond_wait(&segment->lcp.cond, &segment->lcp.lock);
                }
                break;
            default:
                break;
        }
    }

    return result;
}

int inode_segment_index_pre_add(const int64_t inode)
{
    int result;
    FDIRInodeSegmentIndexInfo *segment;
    pthread_mutex_t *lock;

    PTHREAD_RWLOCK_WRLOCK(&segment_index_ctx.rwlock);
    segment = segment_index_ctx.current_binlog.segment;
    if (segment == NULL) {
        result = segment_array_inc(inode);
        segment = segment_index_ctx.current_binlog.segment;
    } else {
        result = 0;
    }

    if (result == 0) {
        lock = &segment->lcp.lock;
        PTHREAD_MUTEX_LOCK(lock);
        do {
            if ((result=check_load(segment)) != 0) {
                break;
            }

            if (segment->inodes.array.counts.total >=
                    FDIR_STORAGE_BATCH_INODE_COUNT)
            {
                PTHREAD_MUTEX_UNLOCK(lock);
                result = segment_array_inc(inode);
                segment = segment_index_ctx.current_binlog.segment;
                lock = &segment->lcp.lock;
                PTHREAD_MUTEX_LOCK(lock);
            } else {
                segment->inodes.last = inode;
            }

            if (result != 0) {
                break;
            }

            if ((result=inode_index_array_pre_add(&segment->
                            inodes.array, inode)) != 0)
            {
                break;
            }
        } while (0);
        PTHREAD_MUTEX_UNLOCK(lock);
    }
    PTHREAD_RWLOCK_UNLOCK(&segment_index_ctx.rwlock);

    return result;
}

int inode_segment_index_real_add(const DAPieceFieldInfo *field,
        FDIRInodeUpdateResult *r)
{
    int result;

    if ((r->segment=find_segment_by_inode(field->oid)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&r->segment->lcp.lock);
    do {
        if ((result=check_load(r->segment)) != 0) {
            break;
        }

        if ((result=inode_index_array_real_add(&r->segment->
                        inodes.array, field)) != 0)
        {
            break;
        }

        r->version = __sync_add_and_fetch(&segment_index_ctx.
                current_version, 1);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&r->segment->lcp.lock);
    return result;
}

int inode_segment_index_delete(FDIRStorageInodeIndexInfo *inode,
        FDIRInodeUpdateResult *r)
{
    int result;

    if ((r->segment=find_segment_by_inode(inode->inode)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&r->segment->lcp.lock);
    do {
        if ((result=check_load(r->segment)) != 0) {
            break;
        }

        if ((result=inode_index_array_delete(&r->segment->
                        inodes.array, inode)) != 0)
        {
            break;
        }

        if (2 * r->segment->inodes.array.counts.deleted >=
                r->segment->inodes.array.counts.total)
        {
            if ((result=inode_binlog_writer_shrink(r->segment)) != 0) {
                break;
            }
        }

        r->version = __sync_add_and_fetch(&segment_index_ctx.
                current_version, 1);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&r->segment->lcp.lock);

    return result;
}

int inode_segment_index_update(const DAPieceFieldInfo *field,
        const bool normal_update, FDIRInodeUpdateResult *r)
{
    int result;
    bool modified;

    if ((r->segment=find_segment_by_inode(field->oid)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&r->segment->lcp.lock);
    do {
        if ((result=check_load(r->segment)) != 0) {
            break;
        }

        if ((result=inode_index_array_update(&r->segment->inodes.array,
                        field, normal_update, &r->old, &modified)) != 0)
        {
            break;
        }
        if (modified) {
            r->version = (normal_update ? __sync_add_and_fetch(
                        &segment_index_ctx.current_version, 1) :
                    field->storage.version);
        } else {
            r->version = 0;
        }
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&r->segment->lcp.lock);

    return result;
}

int inode_segment_index_get(FDIRStorageInodeIndexInfo *inode)
{
    FDIRInodeSegmentIndexInfo *segment;
    int result;

    if ((segment=find_segment_by_inode(inode->inode)) == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    if ((result=check_load(segment)) == 0) {
        result = inode_index_array_find(&segment->inodes.array, inode);
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int inode_segment_index_shrink(FDIRInodeSegmentIndexInfo *segment)
{
    int result;

    if ((result=check_load(segment)) != 0) {
        return result;
    }

    if ((result=inode_index_array_check_shrink(&segment->inodes.array)) != 0) {
        return result;
    }

    if (segment->inodes.array.inodes == NULL) {
        locked_list_del(&segment->dlink, &segment_index_ctx.fifo);
    }
    return 0;
}

int inode_segment_index_eliminate(const int min_elements)
{
    int total;
    FDIRInodeSegmentIndexInfo *segment;

    total = 0;
    do {
        locked_list_pop(&segment_index_ctx.fifo,
                FDIRInodeSegmentIndexInfo,
                dlink, segment);
        if (segment == NULL) {
            return ENOENT;
        }

        PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
        total += segment->inodes.array.alloc;
        inode_index_array_free(&segment->inodes.array);
        segment->inodes.status = FDIR_STORAGE_SEGMENT_STATUS_CLEAN;
        PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
    } while(total < min_elements);

    return 0;
}

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
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../../server_global.h"
#include "write_fd_cache.h"
#include "inode_index_array.h"
#include "binlog_reader.h"
#include "bid_journal.h"
#include "segment_index.h"
#include "binlog_writer.h"

#define BINLOG_RECORD_BATCH_SIZE  1024

typedef struct binlog_writer_synchronize_args {
    struct {
        bool done;
        pthread_lock_cond_pair_t lcp;
    } notify;
    FDIRInodeSegmentIndexInfo *segment;
} BWriterSynchronizeArgs;

typedef struct binlog_writer_shrink_task {
    FDIRInodeSegmentIndexInfo *segment;
    struct binlog_writer_shrink_task *next;
} BinlogWriterShrinkTask;

typedef struct {
    struct {
        struct fast_mblock_man sargs;  //synchronize args
        struct fast_mblock_man record;
        struct fast_mblock_man stask;  //shrink task
    } allocators;
    struct {
        struct fc_queue normal; //update and load
        struct fc_queue shrink; //array shrink
    } queues;
    time_t last_shrink_time;
    volatile int64_t current_version;
    volatile bool running;
} BinlogWriterContext;

typedef struct {
    uint64_t binlog_id;
    char buff[8 * 1024];
    char *current;
    char *buff_end;
    int fd;
} BinlogWriterCache;

static BinlogWriterContext binlog_writer_ctx;

#define WRITER_NORMAL_QUEUE   binlog_writer_ctx.queues.normal
#define WRITER_SHRINK_QUEUE   binlog_writer_ctx.queues.shrink

static inline void notify(BWriterSynchronizeArgs *sync_args)
{
    PTHREAD_MUTEX_LOCK(&sync_args->notify.lcp.lock);
    sync_args->notify.done = true;
    pthread_cond_signal(&sync_args->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&sync_args->notify.lcp.lock);
}

static inline void cache_init(BinlogWriterCache *cache)
{
    cache->binlog_id = 0;
    cache->fd = -1;
    cache->current = cache->buff;
    cache->buff_end = cache->buff + sizeof(cache->buff);
}

static inline int cache_write(BinlogWriterCache *cache)
{
    int len;

    if ((len=cache->current - cache->buff) == 0) {
        return 0;
    }

    cache->current = cache->buff;
    return fc_safe_write(cache->fd, cache->buff, len);
}

static inline int log4create(const FDIRStorageInodeIndexInfo *index,
        BinlogWriterCache *cache)
{
    int result;

    if (cache->buff_end - cache->current <
            FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
    {
        if ((result=cache_write(cache)) != 0) {
            return result;
        }
    }

    cache->current += sprintf(cache->current,
            "%"PRId64" %"PRId64" %c %"PRId64" %d\n",
            index->version, index->inode,
            inode_index_op_type_create,
            index->file_id, index->offset);
    return 0;
}

static inline int log4remove(const FDIRStorageInodeIndexInfo *index,
        BinlogWriterCache *cache)
{
    int result;

    if (cache->buff_end - cache->current <
            FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
    {
        if ((result=cache_write(cache)) != 0) {
            return result;
        }
    }

    cache->current += sprintf(cache->current,
            "%"PRId64" %"PRId64" %c\n",
            index->version, index->inode,
            inode_index_op_type_remove);
    return 0;
}

static int log(FDIRInodeBinlogRecord *record, BinlogWriterCache *cache)
{
    int result;

    if (record->binlog_id != cache->binlog_id || cache->fd < 0) {
        if ((result=cache_write(cache)) != 0) {
            return result;
        }
        cache->binlog_id = record->binlog_id;
        if ((cache->fd=write_fd_cache_get(record->binlog_id)) < 0) {
            return -1 * cache->fd;
        }
    }

    if (record->op_type == inode_index_op_type_create) {
        return log4create(&record->inode_index, cache);
    } else {
        return log4remove(&record->inode_index, cache);
    }
}

#define update_segment_index(start, end)  \
    inode_segment_index_update((FDIRInodeSegmentIndexInfo *) \
            (*start)->args, start, end - start)

#define dec_segment_updating_count(start, end)  \
    FC_ATOMIC_DEC_EX(((FDIRInodeSegmentIndexInfo *)(*start)-> \
                args)->inodes.updating_count, end - start)

static int deal_sorted_record(FDIRInodeBinlogRecord **records,
        const int count)
{
    FDIRInodeBinlogRecord **record;
    FDIRInodeBinlogRecord **end;
    FDIRInodeBinlogRecord **start;
    BinlogWriterCache cache;
    int r;
    int result;

    cache_init(&cache);
    start = NULL;
    result = 0;
    end = records + count;
    for (record=records; record<end; record++) {
        if ((*record)->op_type == inode_index_op_type_synchronize) {
            if (start != NULL) {
                if ((result=update_segment_index(start, record)) != 0) {
                    break;
                }
                start = NULL;
            }
            notify((BWriterSynchronizeArgs *)(*record)->args);
        } else {
            if (start == NULL) {
                start = record;
            } else if ((*record)->binlog_id != (*start)->binlog_id) {
                if ((result=update_segment_index(start, record)) != 0) {
                    break;
                }
                start = record;
            }
            if ((result=log(*record, &cache)) != 0) {
                break;
            }
        }
    }

    r = cache_write(&cache);
    if (record == end) {
        if (start != NULL) {
            if ((result=update_segment_index(start, end)) != 0) {
                return result;
            }
        }
        return r;
    }

    for (; record<end; record++) {
        if ((*record)->op_type == inode_index_op_type_synchronize) {
            notify((BWriterSynchronizeArgs *)(*record)->args);
        }
    }

    return result;
}

static int record_compare(const FDIRInodeBinlogRecord **record1,
        const FDIRInodeBinlogRecord **record2)
{
    int sub;
    sub = fc_compare_int64((*record1)->binlog_id, (*record2)->binlog_id);
    if (sub == 0) {
        return fc_compare_int64((*record1)->version, (*record2)->version);
    } else {
        return sub;
    }
}

static void dec_updating_count(FDIRInodeBinlogRecord **records,
        const int count)
{
    FDIRInodeBinlogRecord **record;
    FDIRInodeBinlogRecord **end;
    FDIRInodeBinlogRecord **start;

    start = NULL;
    end = records + count;
    for (record=records; record<end; record++) {
        if ((*record)->op_type == inode_index_op_type_synchronize) {
            if (start != NULL) {
                dec_segment_updating_count(start, record);
                start = NULL;
            }
        } else {
            if (start == NULL) {
                start = record;
            } else if ((*record)->binlog_id != (*start)->binlog_id) {
                dec_segment_updating_count(start, record);
                start = record;
            }
        }
    }

    if (start != NULL) {
        dec_segment_updating_count(start, end);
    }
}

static int deal_binlog_records(FDIRInodeBinlogRecord *head)
{
    int result;
    int count;
    FDIRInodeBinlogRecord *records[BINLOG_RECORD_BATCH_SIZE];
    FDIRInodeBinlogRecord **pp;
    FDIRInodeBinlogRecord *record;
    struct fast_mblock_node *node;
    struct fast_mblock_chain chain;

    chain.head = chain.tail = NULL;
    pp = records;
    record = head;
    do {
        *pp++ = record;

        node = fast_mblock_to_node_ptr(record);
        if (chain.head == NULL) {
            chain.head = node;
        } else {
            chain.tail->next = node;
        }
        chain.tail = node;

        record = record->next;
    } while (record != NULL);
    chain.tail->next = NULL;

    count = pp - records;
    if (count > 1) {
        qsort(records, count, sizeof(FDIRInodeBinlogRecord *),
                (int (*)(const void *, const void *))record_compare);
    }

    result = deal_sorted_record(records, count);
    dec_updating_count(records, count);
    fast_mblock_batch_free(&binlog_writer_ctx.allocators.record, &chain);
    return result;
}

static int shrink(FDIRInodeSegmentIndexInfo *segment)
{
    int result;
    BinlogWriterCache cache;
    FDIRStorageInodeIndexInfo *inode;
    FDIRStorageInodeIndexInfo *end;
    char full_filename[PATH_MAX];
    char tmp_filename[PATH_MAX];

    if ((result=inode_segment_index_shrink(segment)) != 0) {
        return result;
    }

    binlog_fd_cache_filename(segment->binlog_id,
            full_filename, sizeof(full_filename));
    if (segment->inodes.array.counts.total == 0) {
        if ((result=fc_delete_file_ex(full_filename, "inode binlog")) != 0) {
            return result;
        } else {
            return bid_journal_log(segment->binlog_id,
                    inode_binlog_id_op_type_remove);
        }
    }

    snprintf(tmp_filename, sizeof(tmp_filename),
            "%s.tmp", full_filename);
    cache_init(&cache);
    if ((cache.fd=open(tmp_filename, O_WRONLY |
                    O_CREAT | O_TRUNC, 0755)) < 0)
    {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, tmp_filename, result, strerror(result));
        return result;
    }

    end = segment->inodes.array.inodes + segment->inodes.array.counts.total;
    for (inode=segment->inodes.array.inodes; inode<end; inode++) {
        if (inode->status == FDIR_STORAGE_INODE_STATUS_NORMAL) {
            if ((result=log4create(inode, &cache)) != 0) {
                close(cache.fd);
                return result;
            }
        }
    }

    result = cache_write(&cache);
    close(cache.fd);
    if (result != 0) {
        return result;
    }

    if (rename(tmp_filename, full_filename) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "rename file \"%s\" to \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, tmp_filename, full_filename,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

static void deal_shrink_queue()
{
    BinlogWriterShrinkTask *stask;
    int result;

    while (g_current_time - binlog_writer_ctx.last_shrink_time == 0) {
        if (fc_queue_timedpeek_ms(&WRITER_NORMAL_QUEUE, 100) != NULL) {
            break;
        }
    }

    if (g_current_time - binlog_writer_ctx.last_shrink_time == 0) {
        return;
    }

    if ((stask=(BinlogWriterShrinkTask *)fc_queue_try_pop(
                    &WRITER_SHRINK_QUEUE)) == NULL)
    {
        return;
    }
    if (!SF_G_CONTINUE_FLAG) {
        return;
    }

    binlog_writer_ctx.last_shrink_time = g_current_time;

    write_fd_cache_remove(stask->segment->binlog_id);
    PTHREAD_MUTEX_LOCK(&stask->segment->lcp.lock);
    result = shrink(stask->segment);
    PTHREAD_MUTEX_UNLOCK(&stask->segment->lcp.lock);
    if (result != 0) {
        logCrit("file: "__FILE__", line: %d, "
                "deal_shrink_queue fail, "
                "program exit!", __LINE__);
        sf_terminate_myself();
    }
}

static void *binlog_writer_func(void *arg)
{
    FDIRInodeBinlogRecord *head;
    bool blocked;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "inode-binlog-writer");
#endif

    binlog_writer_ctx.running = true;
    while (SF_G_CONTINUE_FLAG) {
        blocked = fc_queue_empty(&WRITER_SHRINK_QUEUE);
        if ((head=(FDIRInodeBinlogRecord *)fc_queue_pop_all_ex(
                        &WRITER_NORMAL_QUEUE, blocked)) == NULL)
        {
            deal_shrink_queue();
            continue;
        }

        if (deal_binlog_records(head) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "deal_binlog_records fail, "
                    "program exit!", __LINE__);
            sf_terminate_myself();
        }

        if (!blocked || !fc_queue_empty(&WRITER_SHRINK_QUEUE)) {
            deal_shrink_queue();
        }
    }
    binlog_writer_ctx.running = false;
    return NULL;
}

static int sargs_alloc_init_func(BWriterSynchronizeArgs *element, void *args)
{
    return init_pthread_lock_cond_pair(&element->notify.lcp);
}

int inode_binlog_writer_init()
{
    int result;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&binlog_writer_ctx.allocators.sargs,
                    "inode-sync-args", sizeof(BWriterSynchronizeArgs),
                    1024, 0, (fast_mblock_alloc_init_func)
                    sargs_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&binlog_writer_ctx.allocators.record,
                    "inode-binlog-record", sizeof(FDIRInodeBinlogRecord),
                    BINLOG_RECORD_BATCH_SIZE, BINLOG_RECORD_BATCH_SIZE,
                    NULL, NULL, true)) != 0)
    {
        return result;
    }
    fast_mblock_set_need_wait(&binlog_writer_ctx.allocators.record,
            true, (bool *)&SF_G_CONTINUE_FLAG);

    if ((result=fast_mblock_init_ex1(&binlog_writer_ctx.allocators.stask,
                    "shrink-task", sizeof(BinlogWriterShrinkTask),
                    1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&WRITER_NORMAL_QUEUE, (unsigned long)
                    (&((FDIRInodeBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&WRITER_SHRINK_QUEUE, (unsigned long)
                    (&((BinlogWriterShrinkTask *)NULL)->next))) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, binlog_writer_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

static inline int push_to_normal_queue(const uint64_t binlog_id,
        const FDIRStorageInodeIndexOpType op_type,
        const FDIRStorageInodeIndexInfo *inode_index,
        void *args)
{
    FDIRInodeBinlogRecord *record;

    if ((record=(FDIRInodeBinlogRecord *)fast_mblock_alloc_object(
                    &binlog_writer_ctx.allocators.record)) == NULL)
    {
        return ENOMEM;
    }

    record->binlog_id = binlog_id;
    record->version = __sync_add_and_fetch(&binlog_writer_ctx.
            current_version, 1);
    record->op_type = op_type;
    if (inode_index != NULL) {
        record->inode_index = *inode_index;
    }
    record->args = args;
    fc_queue_push(&WRITER_NORMAL_QUEUE, record);
    return 0;
}

int inode_binlog_writer_log(FDIRInodeSegmentIndexInfo *segment,
        const FDIRStorageInodeIndexOpType op_type,
        const FDIRStorageInodeIndexInfo *inode_index)
{
    FC_ATOMIC_INC(segment->inodes.updating_count);
    return push_to_normal_queue(segment->binlog_id,
            op_type, inode_index, segment);
}

int inode_binlog_writer_shrink(FDIRInodeSegmentIndexInfo *segment)
{
    BinlogWriterShrinkTask *stask;

    if ((stask=(BinlogWriterShrinkTask *)fast_mblock_alloc_object(
                    &binlog_writer_ctx.allocators.stask)) == NULL)
    {
        return ENOMEM;
    }

    stask->segment = segment;
    fc_queue_push(&WRITER_SHRINK_QUEUE, stask);
    return 0;
}

int inode_binlog_writer_synchronize(FDIRInodeSegmentIndexInfo *segment)
{
    BWriterSynchronizeArgs *sync_args;
    int result;

    if ((sync_args=(BWriterSynchronizeArgs *)fast_mblock_alloc_object(
                    &binlog_writer_ctx.allocators.sargs)) == NULL)
    {
        return ENOMEM;
    }
    sync_args->segment = segment;

    do {
        result = push_to_normal_queue(segment->binlog_id,
                inode_index_op_type_synchronize, NULL, sync_args);
        if (result != 0) {
            break;
        }

        PTHREAD_MUTEX_LOCK(&sync_args->notify.lcp.lock);
        while (!sync_args->notify.done) {
            pthread_cond_wait(&sync_args->notify.lcp.cond,
                    &sync_args->notify.lcp.lock);
        }
        sync_args->notify.done = false;  /* reset for next */
        PTHREAD_MUTEX_UNLOCK(&sync_args->notify.lcp.lock);
    } while (0);

    fast_mblock_free_object(&binlog_writer_ctx.
            allocators.sargs, sync_args);
    return result;
}

void inode_binlog_writer_finish()
{
}

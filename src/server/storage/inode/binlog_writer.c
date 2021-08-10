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
#include "binlog_reader.h"
#include "segment_index.h"
#include "binlog_writer.h"

#define BINLOG_RECORD_BATCH_SIZE  1024

#define BINLOG_INDEX_FILENAME "binlog_index.dat"

#define BINLOG_INDEX_ITEM_CURRENT_WRITE     "current_write"
#define BINLOG_INDEX_ITEM_CURRENT_COMPRESS  "current_compress"

typedef struct binlog_writer_load_args {
    struct {
        bool done;
        int32_t result;
        pthread_lock_cond_pair_t lcp;
    } notify;
    FDIRStorageInodeIndexArray *index_array;
} BinlogWriterLoadArgs;

typedef struct {
    struct {
        int write_index;
        int inode_count;
    } current_binlog;
    int current_compress;

    struct fast_mblock_man largs_allocator;
    struct fast_mblock_man record_allocator;
    struct fc_queue queue;
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

static int write_to_binlog_index(const int current_write_index)
{
    char full_filename[PATH_MAX];
    char buff[256];
    int fd;
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", STORAGE_PATH_STR, BINLOG_INDEX_FILENAME);
    if ((fd=open(full_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, full_filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : ENOENT;
    }

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE, current_write_index,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS,
            binlog_writer_ctx.current_compress);
    if (fc_safe_write(fd, buff, len) != len) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, full_filename,
                errno, STRERROR(errno));
        close(fd);
        return errno != 0 ? errno : EIO;
    }

    close(fd);
    return 0;
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
            return write_to_binlog_index(binlog_writer_ctx.
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

    binlog_writer_ctx.current_binlog.write_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_WRITE, &iniContext, 0);
    binlog_writer_ctx.current_compress = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS, &iniContext, 0);

    iniFreeContext(&iniContext);
    return 0;
}

static inline void notify(BinlogWriterLoadArgs *load_args, const int result)
{
    PTHREAD_MUTEX_LOCK(&load_args->notify.lcp.lock);
    load_args->notify.done = true;
    load_args->notify.result = result;
    pthread_cond_signal(&load_args->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&load_args->notify.lcp.lock);
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

    if (cache->buff_end - cache->current <
            FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
    {
        if ((result=cache_write(cache)) != 0) {
            return result;
        }
    }

    if (record->op_type == inode_index_op_type_create) {
        cache->current += sprintf(cache->current,
                "%"PRId64" %c %"PRId64" %d\n",
                record->inode_index.inode,
                record->op_type,
                record->inode_index.file_id,
                record->inode_index.offset);
    } else {
        cache->current += sprintf(cache->current,
                "%"PRId64" %c\n",
                record->inode_index.inode,
                record->op_type);
    }

    return 0;
}

#define update_segment_index(start, end)  \
    inode_segment_index_update((FDIRInodeSegmentIndexInfo *) \
            (*start)->args, start, end - start)

static int deal_sorted_record(FDIRInodeBinlogRecord **records,
        const int count)
{
    FDIRInodeBinlogRecord **record;
    FDIRInodeBinlogRecord **end;
    FDIRInodeBinlogRecord **start;
    BinlogWriterLoadArgs *load_args;
    BinlogWriterCache cache;
    int r;
    int result;

    cache_init(&cache);
    start = NULL;
    result = 0;
    end = records + count;
    for (record=records; record<end; record++) {
        if ((*record)->op_type == inode_index_op_type_load) {
            if (start != NULL) {
                if ((result=update_segment_index(start, record)) != 0) {
                    break;
                }
                start = NULL;
            }
            load_args = (BinlogWriterLoadArgs *)(*record)->args;
            result = binlog_reader_load((*record)->binlog_id,
                    load_args->index_array);
            notify(load_args, result);
            if (result != 0) {
                break;
            }
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
        if ((*record)->op_type == inode_index_op_type_load) {
            load_args = (BinlogWriterLoadArgs *)(*record)->args;
            notify(load_args, ECANCELED);
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
    fast_mblock_batch_free(&binlog_writer_ctx.record_allocator, &chain);
    return result;
}

static void *binlog_writer_func(void *arg)
{
    FDIRInodeBinlogRecord *head;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "inode-binlog-writer");
#endif

    binlog_writer_ctx.running = true;

    while (SF_G_CONTINUE_FLAG) {
        if ((head=(FDIRInodeBinlogRecord *)fc_queue_pop_all(
                        &binlog_writer_ctx.queue)) == NULL)
        {
            continue;
        }

        if (deal_binlog_records(head) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "deal_binlog_records fail, "
                    "program exit!", __LINE__);
            sf_terminate_myself();
        }
    }
    binlog_writer_ctx.running = false;
    return NULL;
}

static int largs_alloc_init_func(BinlogWriterLoadArgs *element, void *args)
{
    return init_pthread_lock_cond_pair(&element->notify.lcp);
}

int inode_binlog_writer_init()
{
    int result;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&binlog_writer_ctx.largs_allocator,
                    "inode-load-args", sizeof(BinlogWriterLoadArgs),
                    1024, 0, (fast_mblock_alloc_init_func)
                    largs_alloc_init_func, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&binlog_writer_ctx.record_allocator,
                    "inode-binlog", sizeof(FDIRInodeBinlogRecord),
                    BINLOG_RECORD_BATCH_SIZE, BINLOG_RECORD_BATCH_SIZE,
                    NULL, NULL, true)) != 0)
    {
        return result;
    }
    fast_mblock_set_need_wait(&binlog_writer_ctx.record_allocator,
            true, (bool *)&SF_G_CONTINUE_FLAG);

    if ((result=fc_queue_init(&binlog_writer_ctx.queue, (unsigned long)
                    (&((FDIRInodeBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    return fc_create_thread(&tid, binlog_writer_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

static inline int push_to_queue(const uint64_t binlog_id,
        const FDIRStorageInodeIndexOpType op_type,
        const FDIRStorageInodeIndexInfo *inode_index,
        void *args)
{
    FDIRInodeBinlogRecord *record;

    if ((record=(FDIRInodeBinlogRecord *)fast_mblock_alloc_object(
                    &binlog_writer_ctx.record_allocator)) == NULL)
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
    fc_queue_push(&binlog_writer_ctx.queue, record);
    return 0;
}

int inode_binlog_writer_log(FDIRInodeSegmentIndexInfo *segment,
        const FDIRStorageInodeIndexOpType op_type,
        const FDIRStorageInodeIndexInfo *inode_index)
{
    return push_to_queue(segment->binlog_id, op_type, inode_index, segment);
}

int inode_binlog_writer_load(const uint64_t binlog_id,
        FDIRStorageInodeIndexArray *index_array)
{
    BinlogWriterLoadArgs *load_args;
    int result;

    if ((load_args=(BinlogWriterLoadArgs *)fast_mblock_alloc_object(
                    &binlog_writer_ctx.largs_allocator)) == NULL)
    {
        return ENOMEM;
    }
    load_args->index_array = index_array;

    do {
        if ((result=push_to_queue(binlog_id, inode_index_op_type_load,
                        NULL, load_args)) != 0)
        {
            break;
        }

        PTHREAD_MUTEX_LOCK(&load_args->notify.lcp.lock);
        while (!load_args->notify.done) {
            pthread_cond_wait(&load_args->notify.lcp.cond,
                    &load_args->notify.lcp.lock);
        }
        load_args->notify.done = false;  /* reset for next */
        result = load_args->notify.result;
        PTHREAD_MUTEX_UNLOCK(&load_args->notify.lcp.lock);
    } while (0);

    fast_mblock_free_object(&binlog_writer_ctx.largs_allocator, load_args);
    return result;
}

void inode_binlog_writer_finish()
{
}

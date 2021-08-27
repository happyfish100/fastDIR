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
#include "diskallocator/binlog/space/binlog_writer.h"
#include "write_fd_cache.h"
#include "inode_index_array.h"
#include "binlog_reader.h"
#include "bid_journal.h"
#include "segment_index.h"
#include "binlog_writer.h"

static inline int log4create(const FDIRStorageInodeIndexInfo *index,
        DABinlogWriterCache *cache)
{
    int result;

    if (cache->buff_end - cache->current <
            FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
    {
        if ((result=da_binlog_writer_cache_write(cache)) != 0) {
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
        DABinlogWriterCache *cache)
{
    int result;

    if (cache->buff_end - cache->current <
            FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
    {
        if ((result=da_binlog_writer_cache_write(cache)) != 0) {
            return result;
        }
    }

    cache->current += sprintf(cache->current,
            "%"PRId64" %"PRId64" %c\n",
            index->version, index->inode,
            inode_index_op_type_remove);
    return 0;
}

static int log(FDIRInodeBinlogRecord *record, DABinlogWriterCache *cache)
{
    if (record->op_type == inode_index_op_type_create) {
        return log4create(&record->inode_index, cache);
    } else {
        return log4remove(&record->inode_index, cache);
    }
}

#define update_segment_index(start, end)  \
    inode_segment_index_update((FDIRInodeSegmentIndexInfo *) \
            (*start)->args, start, end - start)

static int shrink(FDIRInodeSegmentIndexInfo *segment)
{
    int result;
    DABinlogWriterCache cache;
    FDIRStorageInodeIndexInfo *inode;
    FDIRStorageInodeIndexInfo *end;
    char full_filename[PATH_MAX];
    char tmp_filename[PATH_MAX];

    if ((result=inode_segment_index_shrink(segment)) != 0) {
        return result;
    }

    write_fd_cache_filename(segment->writer.key.id,
            full_filename, sizeof(full_filename));
    if (segment->inodes.array.counts.total == 0) {
        if ((result=fc_delete_file_ex(full_filename, "inode binlog")) != 0) {
            return result;
        } else {
            return bid_journal_log(segment->writer.key.id,
                    inode_binlog_id_op_type_remove);
        }
    }

    snprintf(tmp_filename, sizeof(tmp_filename),
            "%s.tmp", full_filename);
    da_binlog_writer_cache_init(&cache);
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

    result = da_binlog_writer_cache_write(&cache);
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

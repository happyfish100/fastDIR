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

#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "write_fd_cache.h"
#include "inode_index_array.h"
#include "binlog_reader.h"
#include "bid_journal.h"
#include "segment_index.h"
#include "binlog_writer.h"

int inode_binlog_pack_record(const FDIRStorageInodeFieldInfo *field,
        char *buff, const int size)
{
    if (field->op_type == da_binlog_op_type_create ||
            field->op_type == da_binlog_op_type_update)
    {
        return snprintf(buff, size, "%u %"PRId64" %"PRId64" %c %d "
                "%u %u %u\n", (uint32_t)g_current_time,
                field->storage.version, field->inode, field->op_type,
                field->index, field->storage.trunk_id,
                field->storage.offset, field->storage.size);
    } else {
        return snprintf(buff, size, "%u %"PRId64" %"PRId64" %c\n",
                (uint32_t)g_current_time, field->storage.version,
                field->inode, field->op_type);
    }
}

static inline int log4create(const FDIRStorageInodeIndexInfo *index,
        DABinlogWriterCache *cache)
{
    int result;
    int i;
    int count;
    FDIRStorageInodeFieldInfo field;

    if (cache->buff_end - cache->current < FDIR_PIECE_FIELD_COUNT *
            FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
    {
        if ((result=da_binlog_writer_cache_write(cache)) != 0) {
            return result;
        }
    }

    count = 0;
    field.inode = index->inode;
    for (i=0; i<FDIR_PIECE_FIELD_COUNT; i++) {
        if (!DA_PIECE_FIELD_IS_EMPTY(index->fields + i)) {
            field.op_type = (++count == 1) ?
                da_binlog_op_type_create :
                da_binlog_op_type_update;
            field.index = i;
            field.storage = index->fields[i];
            cache->current += inode_binlog_pack_record(&field,
                    cache->current, cache->buff_end - cache->current);
        }
    }

    return 0;
}

int inode_binlog_shrink_callback(DABinlogWriter *writer, void *args)
{
    int result;
    FDIRInodeSegmentIndexInfo *segment;
    DABinlogIdTypePair id_type_pair;
    DABinlogWriterCache cache;
    FDIRStorageInodeIndexInfo *inode;
    FDIRStorageInodeIndexInfo *end;
    char full_filename[PATH_MAX];
    char tmp_filename[PATH_MAX];

    segment = (FDIRInodeSegmentIndexInfo *)args;
    if ((result=inode_segment_index_shrink(segment)) != 0) {
        return result;
    }

    DA_SET_BINLOG_ID_TYPE(id_type_pair, segment->binlog_id,
            FDIR_STORAGE_BINLOG_TYPE_INODE);
    da_write_fd_cache_filename(&id_type_pair,
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

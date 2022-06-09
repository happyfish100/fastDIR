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
#include "sf/sf_buffered_writer.h"
#include "binlog_pack.h"
#include "binlog_reader.h"
#include "binlog_read_thread.h"
#include "binlog_dump.h"
#include "binlog_sort.h"

typedef struct {
    char inodes_filename[PATH_MAX];
} BinlogSortContext;

static int deal_data_buffer(BinlogReadThreadContext *reader_ctx,
        BinlogReadThreadResult *r, SFBufferedWriter *writer)
{
    int result;
    int64_t inode;
    char error_info[256];
    const char *p;
    const char *rec_end;
    const char *end;

    p = r->buffer.buff;
    end = r->buffer.buff + r->buffer.length;
    while (p < end) {
        if ((result=binlog_unpack_inode(p, end - p, &inode, &rec_end,
                        error_info, sizeof(error_info))) != 0)
        {
            ServerBinlogReader *reader;
            int64_t offset;
            int64_t line_count;

            reader = &reader_ctx->reader;
            offset = r->binlog_position.offset + (p - r->buffer.buff);
            if (fc_get_file_line_count_ex(reader->filename,
                        offset, &line_count) == 0)
            {
                ++line_count;
            }

            logError("file: "__FILE__", line: %d, "
                    "binlog file: %s, offset: %"PRId64", line no: "
                    "%"PRId64", %s", __LINE__, reader->filename,
                    offset, line_count, error_info);
            return result;
        }

        if (inode != FDIR_DATA_DUMP_DUMMY_INODE) {
            if (SF_BUFFERED_WRITER_REMAIN(*writer) < 64) {
                if ((result=sf_buffered_writer_save(writer)) != 0) {
                    return result;
                }
            }
            writer->buffer.current += sprintf(writer->buffer.current,
                    "%"PRId64" %"PRId64" %d\n", inode,
                    r->binlog_position.offset + (p - r->buffer.buff),
                    (int)(rec_end - p));
        }

        p = rec_end;
    }

    return 0;
}

static int sort_inodes(BinlogSortContext *context)
{
    const int64_t last_data_version = 0;
    const int buffer_size = 4 * 1024 * 1024;
    const int buffer_count = BINLOG_READ_THREAD_BUFFER_COUNT;
    int result;
    char tmp_filename[PATH_MAX];
    SFBinlogFilePosition hint_pos;
    BinlogReadThreadContext reader_ctx;
    SFBufferedWriter inode_writer;
    BinlogReadThreadResult *r;

    hint_pos.index = 0;
    hint_pos.offset = 0;
    if ((result=binlog_read_thread_init_ex(&reader_ctx,
                    FDIR_DATA_DUMP_SUBDIR_NAME, &hint_pos,
                    last_data_version, buffer_size,
                    buffer_count)) != 0)
    {
        return result;
    }

    snprintf(tmp_filename, sizeof(tmp_filename),
            "%s.tmp", context->inodes_filename);
    if ((result=sf_buffered_writer_init_ex(&inode_writer,
                    tmp_filename, buffer_size)) != 0)
    {
        return result;
    }

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((r=binlog_read_thread_fetch_result(&reader_ctx)) == NULL) {
            result = EINTR;
            break;
        }

        if (r->err_no == ENOENT) {
            break;
        } else if (r->err_no != 0) {
            result = r->err_no;
            break;
        }

        if ((result=deal_data_buffer(&reader_ctx, r, &inode_writer)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&reader_ctx, r);
    }

    if (result == 0) {
        if (!SF_G_CONTINUE_FLAG) {
            result = EINTR;
        } else if ((result=sf_buffered_writer_save(&inode_writer)) != 0) {
            return result;
        }
    }

    binlog_read_thread_terminate(&reader_ctx);
    sf_buffered_writer_destroy(&inode_writer);
    return result;
}

int binlog_sort_by_inode(const bool check_exist)
{
    int result;
    BinlogSortContext context;

    /*
    if (DUMP_ORDER_BY == FDIR_DUMP_ORDER_BY_INODE) {
        return 0;
    }
    */

    snprintf(context.inodes_filename, sizeof(context.inodes_filename),
            "%s/%s/inodes.dat", DATA_PATH_STR, FDIR_DATA_DUMP_SUBDIR_NAME);
    if ((result=sort_inodes(&context)) != 0) {
        return result;
    }

    return result;
    /*
    DUMP_ORDER_BY = FDIR_DUMP_ORDER_BY_INODE;
    return binlog_dump_write_to_mark_file();
    */
}

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
#include "fastcommon/system_info.h"
#include "sf/sf_buffered_writer.h"
#include "binlog_pack.h"
#include "binlog_reader.h"
#include "binlog_read_thread.h"
#include "binlog_dump.h"
#include "binlog_sort.h"

typedef struct {
    FilenameFDPair inode_fp; //sorted inodes
    string_t inode_content;
    struct {
        BinlogReadThreadContext *reader_ctx;
        BinlogReadThreadResult *r;
        SFBufferedWriter *writer;
    } extract_inode;
} DumpDataSortContext;

static int deal_data_buffer(DumpDataSortContext *ctx)
{
    int result;
    int64_t inode;
    char error_info[256];
    const char *p;
    const char *rec_end;
    const char *end;

    p = ctx->extract_inode.r->buffer.buff;
    end = ctx->extract_inode.r->buffer.buff +
        ctx->extract_inode.r->buffer.length;
    while (p < end) {
        if ((result=binlog_unpack_inode(p, end - p, &inode, &rec_end,
                        error_info, sizeof(error_info))) != 0)
        {
            ServerBinlogReader *reader;
            int64_t offset;
            int64_t line_count;

            reader = &ctx->extract_inode.reader_ctx->reader;
            offset = ctx->extract_inode.r->binlog_position.offset +
                (p - ctx->extract_inode.r->buffer.buff);
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
            if (SF_BUFFERED_WRITER_REMAIN(*ctx->
                        extract_inode.writer) < 64)
            {
                if ((result=sf_buffered_writer_save(ctx->
                                extract_inode.writer)) != 0)
                {
                    return result;
                }
            }

            ctx->extract_inode.writer->buffer.current += sprintf(
                    ctx->extract_inode.writer->buffer.current,
                    "%"PRId64" %"PRId64" %d\n", inode,
                    ctx->extract_inode.r->binlog_position.offset +
                    (p - ctx->extract_inode.r->buffer.buff),
                    (int)(rec_end - p));
        }

        p = rec_end;
    }

    return 0;
}

static int sort_inode_file(DumpDataSortContext *context,
        const char *tmp_filename)
{
    int result;
    int64_t mem_size;
    int64_t buffer_size_mb;
    char buffer_size_str[32];
    char tmp_path[PATH_MAX];
    char cmd_line[2 * PATH_MAX];

    snprintf(tmp_path, sizeof(tmp_path), "%s/tmp", DATA_PATH_STR);
    if ((result=fc_check_mkdir(tmp_path, 0755)) != 0) {
        return result;
    }

    if ((result=get_sys_total_mem_size(&mem_size)) != 0) {
        return result;
    }
    buffer_size_mb = (mem_size * 0.25) / (1024 * 1024);
    if (buffer_size_mb < 1024) {
        sprintf(buffer_size_str, "%"PRId64"M", buffer_size_mb);
    } else {
        sprintf(buffer_size_str, "%"PRId64"G", buffer_size_mb / 1024);
    }

    snprintf(cmd_line, sizeof(cmd_line), "/usr/bin/sort -n -k1,1 -S %s "
            "-T %s -o %s %s", buffer_size_str, tmp_path, context->
            inode_fp.filename, tmp_filename);
    if ((result=system(cmd_line)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "execute command %s fail, return status: %d",
                __LINE__, cmd_line, result);
        return result;
    }

    return 0;
}

static int generate_sorted_inode_file()
{
    const int64_t last_data_version = 0;
    const int buffer_size = 4 * 1024 * 1024;
    const int buffer_count = BINLOG_READ_THREAD_BUFFER_COUNT;
    int result;
    DumpDataSortContext context;
    char tmp_filename[PATH_MAX];
    SFBinlogFilePosition hint_pos;
    BinlogReadThreadContext reader_ctx;
    SFBufferedWriter inode_writer;

    binlog_sort_get_inodes_filename(context.inode_fp.filename,
            sizeof(context.inode_fp.filename));
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
            "%s.tmp", context.inode_fp.filename);
    if ((result=sf_buffered_writer_init_ex(&inode_writer,
                    tmp_filename, buffer_size)) != 0)
    {
        return result;
    }

    context.extract_inode.reader_ctx = &reader_ctx;
    context.extract_inode.writer = &inode_writer;
    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((context.extract_inode.r=binlog_read_thread_fetch_result(
                        &reader_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        if (context.extract_inode.r->err_no == ENOENT) {
            break;
        } else if (context.extract_inode.r->err_no != 0) {
            result = context.extract_inode.r->err_no;
            break;
        }

        if ((result=deal_data_buffer(&context)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&reader_ctx,
                context.extract_inode.r);
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

    if (result != 0) {
        return result;
    }

    if ((result=sort_inode_file(&context, tmp_filename)) != 0) {
        return result;
    }

    if ((result=fc_delete_file_ex(tmp_filename, "tmp")) != 0) {
        return result;
    }

    return result;
}

int binlog_sort_generate_inodes_file()
{
    int result;
    int64_t start_time_ms;
    int64_t time_used_ms;
    char buff[16];

    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "begin extract and sort inodes ...", __LINE__);
    if ((result=generate_sorted_inode_file()) != 0) {
        return result;
    }

    time_used_ms = get_current_time_ms() - start_time_ms;
    long_to_comma_str(time_used_ms, buff);
    logInfo("file: "__FILE__", line: %d, "
            "extract and sort inodes done, "
            "time used: %s ms.", __LINE__, buff);

    return 0;
}

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
    int64_t inode;
    int64_t offset;
    int length;
    char *data;
} DumpDataInodeInfo;

typedef struct {
    DumpDataInodeInfo *inodes;
    int count;
    int alloc;
} DumpDataInodeArray;

typedef struct {
    FilenameFDPair inode_fp; //sorted inodes
    FilenameFDPair data_fp;  //full dump data
    string_t inode_content;
    struct fast_mpool_man mpool; //for inode data/binlog
    DumpDataInodeArray inode_array;    //order by inode
    DumpDataInodeInfo **origin_inodes; //for memcpy
    DumpDataInodeInfo **sorted_inodes; //order by offset
    SFBufferedWriter data_writer;
    int64_t current_version;
    struct {
        BinlogReadThreadContext *reader_ctx;
        BinlogReadThreadResult *r;
        SFBufferedWriter *writer;
    } extract_inode;
    char last_padding_buff[256];
    int last_padding_len;
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
        } else {
            ctx->last_padding_len = snprintf(ctx->last_padding_buff,
                    sizeof(ctx->last_padding_buff), "%.*s",
                    (int)(rec_end - p), p);
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

static int generate_sorted_inode_file(DumpDataSortContext *context)
{
    const int64_t last_data_version = 0;
    const int buffer_size = 4 * 1024 * 1024;
    const int buffer_count = BINLOG_READ_THREAD_BUFFER_COUNT;
    int result;
    char tmp_filename[PATH_MAX];
    SFBinlogFilePosition hint_pos;
    BinlogReadThreadContext reader_ctx;
    SFBufferedWriter inode_writer;

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
            "%s.tmp", context->inode_fp.filename);
    if ((result=sf_buffered_writer_init_ex(&inode_writer,
                    tmp_filename, buffer_size)) != 0)
    {
        return result;
    }

    context->extract_inode.reader_ctx = &reader_ctx;
    context->extract_inode.writer = &inode_writer;
    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((context->extract_inode.r=binlog_read_thread_fetch_result(
                        &reader_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        if (context->extract_inode.r->err_no == ENOENT) {
            break;
        } else if (context->extract_inode.r->err_no != 0) {
            result = context->extract_inode.r->err_no;
            break;
        }

        if ((result=deal_data_buffer(context)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&reader_ctx,
                context->extract_inode.r);
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

    if ((result=sort_inode_file(context, tmp_filename)) != 0) {
        return result;
    }

    if ((result=fc_delete_file_ex(tmp_filename, "tmp")) != 0) {
        return result;
    }

    return result;
}

static int cmp_inode_by_offset(const DumpDataInodeInfo **ino1,
        const DumpDataInodeInfo **ino2)
{
    return fc_compare_int64((*ino1)->offset, (*ino2)->offset);
}

static int parse_inodes(DumpDataSortContext *ctx)
{
    DumpDataInodeInfo *ino;
    char *p;
    char *bend;
    char *endptr;

    ino = ctx->inode_array.inodes;
    bend = ctx->inode_content.str + ctx->inode_content.len;
    p = ctx->inode_content.str;
    while (p < bend) {
        ino->inode = strtoll(p, &endptr, 10);
        if (*endptr != ' ') {
            logError("file: "__FILE__", line: %d, "
                    "unexpect end char: 0x%02x",
                    __LINE__, *endptr);
            return EINVAL;
        }

        ino->offset = strtoll(endptr + 1, &endptr, 10);
        if (*endptr != ' ') {
            logError("file: "__FILE__", line: %d, "
                    "unexpect end char: 0x%02x",
                    __LINE__, *endptr);
            return EINVAL;
        }

        ino->length = strtol(endptr + 1, &endptr, 10);
        if (*endptr != '\n') {
            logError("file: "__FILE__", line: %d, "
                    "unexpect end char: 0x%02x",
                    __LINE__, *endptr);
            return EINVAL;
        }

        p = endptr + 1;
        ++ino;
    }

    ctx->inode_array.count = ino - ctx->inode_array.inodes;
    return 0;
}

static int fetch_inodes_data(DumpDataSortContext *ctx)
{
    int result;
    int bytes;
    DumpDataInodeInfo **pp;
    DumpDataInodeInfo **end;

    memcpy(ctx->sorted_inodes, ctx->origin_inodes,
            sizeof(DumpDataInodeInfo *) *
            ctx->inode_array.count);
    qsort(ctx->sorted_inodes, ctx->inode_array.count,
            sizeof(DumpDataInodeInfo *), (int (*)(const void *,
                    const void *))cmp_inode_by_offset);

    fast_mpool_reset(&ctx->mpool);
    end = ctx->sorted_inodes + ctx->inode_array.count;
    for (pp=ctx->sorted_inodes; pp<end; pp++) {
        (*pp)->data = fast_mpool_alloc(&ctx->mpool, (*pp)->length);
        if ((*pp)->data == NULL) {
            return ENOMEM;
        }

        if ((bytes=pread(ctx->data_fp.fd, (*pp)->data, (*pp)->length,
                        (*pp)->offset)) != (*pp)->length)
        {
            result = (errno != 0 ? errno : EIO);
            logError("file: "__FILE__", line: %d, "
                    "read from file %s fail, offset: %"PRId64", "
                    "length: %d, read bytes: %d, errno: %d, "
                    "error info: %s", __LINE__, ctx->data_fp.
                    filename, (*pp)->offset, (*pp)->length,
                    bytes, result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

static int do_sort_data(DumpDataSortContext *ctx)
{
    int result;
    int out_len;
    DumpDataInodeInfo *ino;
    DumpDataInodeInfo *end;

    if ((result=parse_inodes(ctx)) != 0) {
        return result;
    }

    if ((result=fetch_inodes_data(ctx)) != 0) {
        return result;
    }

    end = ctx->inode_array.inodes + ctx->inode_array.count;
    for (ino=ctx->inode_array.inodes; ino<end; ino++) {
        if (SF_BUFFERED_WRITER_REMAIN(ctx->data_writer) < ino->length + 64) {
            if ((result=sf_buffered_writer_save(&ctx->data_writer)) != 0) {
                return result;
            }
        }

        ++(ctx->current_version);
        if ((result=binlog_repack_buffer(ino->data, ino->length, ctx->
                        current_version, ctx->data_writer.buffer.current,
                        &out_len)) != 0)
        {
            return result;
        }
        ctx->data_writer.buffer.current += out_len;
    }

    return 0;
}

static inline int open_file_for_read(FilenameFDPair *pair)
{
    int result;

    if ((pair->fd=open(pair->filename, O_RDONLY | O_CLOEXEC)) < 0) {
        result = (errno != 0 ? errno : ENOENT);
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, pair->filename, result, STRERROR(result));
        return result;
    } else {
        return 0;
    }
}

static int init_inodes_arrays(DumpDataSortContext *ctx,
        const int buffer_size)
{
    DumpDataInodeInfo *src;
    DumpDataInodeInfo *end;
    DumpDataInodeInfo **dest;

    ctx->inode_array.alloc = buffer_size / 16;
    if ((ctx->inode_array.inodes=fc_malloc(sizeof(DumpDataInodeInfo) *
                    ctx->inode_array.alloc)) == NULL)
    {
        return ENOMEM;
    }

    if ((ctx->origin_inodes=fc_malloc(sizeof(DumpDataInodeInfo *) *
                    ctx->inode_array.alloc)) == NULL)
    {
        return ENOMEM;
    }
    if ((ctx->sorted_inodes=fc_malloc(sizeof(DumpDataInodeInfo *) *
                    ctx->inode_array.alloc)) == NULL)
    {
        return ENOMEM;
    }

    end = ctx->inode_array.inodes + ctx->inode_array.alloc;
    for (src=ctx->inode_array.inodes, dest=ctx->origin_inodes;
            src<end; src++, dest++)
    {
        *dest = src;
    }

    return 0;
}

static int sort_dump_data(DumpDataSortContext *ctx)
{
    const int buffer_size = 512 * 1024;
    const int alloc_size_once = 16 * 1024 * 1024;
    const int discard_size = 1024;
    int result;
    int bytes;
    string_t remain;
    char *buff;
    char *last_newline;
    char tmp_filename[PATH_MAX];

    fdir_get_dump_data_filename_ex(FDIR_DATA_DUMP_SUBDIR_NAME,
            ctx->data_fp.filename, sizeof(ctx->data_fp.filename));
    snprintf(tmp_filename, sizeof(tmp_filename),
            "%s.tmp", ctx->data_fp.filename);
    if ((result=sf_buffered_writer_init_ex(&ctx->data_writer,
                    tmp_filename, 4 * 1024 * 1024)) != 0)
    {
        return result;
    }

    if ((buff=fc_malloc(buffer_size)) == NULL) {
        return ENOMEM;
    }

    if ((result=init_inodes_arrays(ctx, buffer_size)) != 0) {
        return result;
    }

    if ((result=fast_mpool_init(&ctx->mpool, alloc_size_once,
                    discard_size)) != 0)
    {
        return result;
    }

    if ((result=open_file_for_read(&ctx->inode_fp)) != 0) {
        return result;
    }

    if ((result=open_file_for_read(&ctx->data_fp)) != 0) {
        return result;
    }

    ctx->current_version = 0;
    ctx->inode_content.str = buff;
    remain.len = 0;
    while ((bytes=read(ctx->inode_fp.fd, buff + remain.len,
                    buffer_size - remain.len)) > 0)
    {
        ctx->inode_content.len = remain.len + bytes;
        if ((last_newline=(char *)fc_memrchr(buff, '\n',
                        ctx->inode_content.len)) == NULL)
        {
            logError("file: "__FILE__", line: %d, "
                    "sorted inode file: %s, expect new line!",
                    __LINE__, ctx->inode_fp.filename);
            return EINVAL;
        }

        remain.str =  last_newline + 1;
        remain.len = (buff + ctx->inode_content.len) - remain.str;
        ctx->inode_content.len -= remain.len;
        if ((result=do_sort_data(ctx)) != 0) {
            return result;
        }

        if (remain.len > 0) {
            memcpy(buff, remain.str, remain.len);
        }
    }

    close(ctx->inode_fp.fd);
    close(ctx->data_fp.fd);
    free(buff);
    free(ctx->inode_array.inodes);
    fast_mpool_destroy(&ctx->mpool);
    fc_delete_file_ex(ctx->inode_fp.filename, "inode index");

    if (SF_BUFFERED_WRITER_REMAIN(ctx->data_writer) <
            ctx->last_padding_len)
    {
        if ((result=sf_buffered_writer_save(&ctx->data_writer)) != 0) {
            return result;
        }
    }

    memcpy(ctx->data_writer.buffer.current,
            ctx->last_padding_buff, ctx->last_padding_len);
    ctx->data_writer.buffer.current += ctx->last_padding_len;
    if ((result=sf_buffered_writer_save(&ctx->data_writer)) != 0) {
        return result;
    }
    sf_buffered_writer_destroy(&ctx->data_writer);

    if (rename(tmp_filename, ctx->data_fp.filename) != 0) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "rename file %s to %s fail, errno: %d, error info: %s",
                __LINE__, tmp_filename, ctx->data_fp.filename, result,
                STRERROR(result));
        return result;
    } else {
        return 0;
    }
}

int binlog_sort_by_inode()
{
    int result;
    DumpDataSortContext context;
    int64_t start_time_ms;
    int64_t time_used_ms;
    char buff[16];

    if (DUMP_ORDER_BY == FDIR_DUMP_ORDER_BY_INODE) {
        return 0;
    }

    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "begin extract and sort inodes ...", __LINE__);

    context.last_padding_len = 0;
    snprintf(context.inode_fp.filename, sizeof(context.inode_fp.filename),
            "%s/%s/inodes.dat", DATA_PATH_STR, FDIR_DATA_DUMP_SUBDIR_NAME);
    if ((result=generate_sorted_inode_file(&context)) != 0) {
        return result;
    }

    time_used_ms = get_current_time_ms() - start_time_ms;
    long_to_comma_str(time_used_ms, buff);
    logInfo("file: "__FILE__", line: %d, "
            "extract and sort inodes done, "
            "time used: %s ms.", __LINE__, buff);

    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "begin sort dump data ...", __LINE__);
    if ((result=sort_dump_data(&context)) != 0) {
        return result;
    }

    time_used_ms = get_current_time_ms() - start_time_ms;
    long_to_comma_str(time_used_ms, buff);
    logInfo("file: "__FILE__", line: %d, "
            "sort dump data done, "
            "time used: %s ms.", __LINE__, buff);
    DUMP_ORDER_BY = FDIR_DUMP_ORDER_BY_INODE;
    return binlog_dump_write_to_mark_file();
}

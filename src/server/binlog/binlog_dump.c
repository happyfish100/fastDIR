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
#include "binlog_pack.h"
#include "../db/dentry_loader.h"
#include "../dentry.h"
#include "../data_thread.h"
#include "binlog_dump.h"

typedef struct {
    int64_t dentry_count;
    FDIRBinlogRecord record;
    FastBuffer buffer;
    BinlogPackContext pack_ctx;
    FDIRBinlogDumpContext *dump_ctx;
} DataDumperContext;

static int init_dump_ctx(FDIRBinlogDumpContext *dump_ctx)
{
    const uint64_t next_version = 1;
    const int buffer_size = 4 * 1024 * 1024;
    const int ring_size = 1024;
    const short order_mode = SF_BINLOG_THREAD_ORDER_MODE_VARY;
    const int max_record_size = 0;  //use the binlog buffer of the caller
    const int writer_count = 1;
    const bool use_fixed_buffer_size = true;
    const char *subdir_name = "binlog/dump";
    char filepath[PATH_MAX];
    int result;

    sf_binlog_writer_get_filepath(DATA_PATH_STR, subdir_name,
            filepath, sizeof(filepath));
    if ((result=fc_check_mkdir(filepath, 0755)) != 0) {
        return result;
    }

    if ((result=sf_synchronize_ctx_init(&dump_ctx->sctx)) != 0) {
        return result;
    }

    dump_ctx->current_version = 0;
    if ((result=sf_binlog_writer_init_by_version_ex(&dump_ctx->bwctx.writer,
                    DATA_PATH_STR, subdir_name, next_version, buffer_size,
                    ring_size, SF_BINLOG_NEVER_ROTATE_FILE)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_init_thread_ex(&dump_ctx->bwctx.thread,
            subdir_name, &dump_ctx->bwctx.writer, order_mode,
            max_record_size, writer_count, use_fixed_buffer_size);
}

static void destroy_dump_ctx(FDIRBinlogDumpContext *dump_ctx)
{
    sf_synchronize_ctx_destroy(&dump_ctx->sctx);
    sf_binlog_writer_destroy(&dump_ctx->bwctx);
}

static void data_dump_finish_notify(FDIRBinlogRecord *record,
        const int result, const bool is_error)
{
    FDIRBinlogDumpContext *dump_ctx;

    dump_ctx = record->notify.args;
    sf_synchronize_counter_notify(&dump_ctx->sctx, 1);
}

int binlog_dump_all(const char *filename)
{
#define RECORD_FIXED_COUNT  64
    int result;
    FDIRBinlogDumpContext dump_ctx;
    FDIRDataThreadContext *thread;
    FDIRDataThreadContext *end;
    struct {
        FDIRBinlogRecord holder[RECORD_FIXED_COUNT];
        FDIRBinlogRecord *elts;
    } records;
    FDIRBinlogRecord *record;
    int64_t start_time_ms;
    int64_t time_used_ms;
    char buff[16];

    if ((result=init_dump_ctx(&dump_ctx)) != 0) {
        return result;
    }

    if (g_data_thread_vars.thread_array.count <= RECORD_FIXED_COUNT) {
        records.elts = records.holder;
    } else {
        records.elts = fc_malloc(sizeof(FDIRBinlogRecord) *
                g_data_thread_vars.thread_array.count);
        if (records.elts == NULL) {
            return ENOMEM;
        }
    }

    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "begin dump data ...", __LINE__);

    memset(records.elts, 0, sizeof(FDIRBinlogRecord) *
            g_data_thread_vars.thread_array.count);

    sf_synchronize_counter_add(&dump_ctx.sctx,
            g_data_thread_vars.thread_array.count);
    end = g_data_thread_vars.thread_array.contexts +
        g_data_thread_vars.thread_array.count;
    for (thread=g_data_thread_vars.thread_array.contexts,
            record=records.elts; thread<DATA_THREAD_END;
            thread++, record++)
    {
        record->record_type = fdir_record_type_query;
        record->operation = SERVER_OP_DUMP_DATA_INT;
        record->notify.func = data_dump_finish_notify;
        record->notify.args = &dump_ctx;
        record->hash_code = (record - records.elts); //for data thread dispatch
        push_to_data_thread_queue(record);
    }

    sf_synchronize_counter_wait(&dump_ctx.sctx);

    time_used_ms = get_current_time_ms() - start_time_ms;
    logInfo("file: "__FILE__", line: %d, "
            "dump data done, time used: %s ms", __LINE__,
            long_to_comma_str(time_used_ms, buff));

    if (records.elts != records.holder) {
        free(records.elts);
    }

    destroy_dump_ctx(&dump_ctx);
    return result;
}

static int output_dentry(DataDumperContext *dd_ctx,
        FDIRServerDentry *dentry)
{
    SFBinlogWriterBuffer *wbuffer;
    int result;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&dd_ctx->
                    dump_ctx->bwctx.thread)) == NULL)
    {
        return ENOMEM;
    }

    dd_ctx->record.timestamp = g_current_time;
    dd_ctx->record.data_version = FC_ATOMIC_INC(
            dd_ctx->dump_ctx->current_version);
    dd_ctx->record.inode = dentry->inode;
    dd_ctx->record.me.pname.parent_inode = (dentry->parent != NULL ?
            dentry->parent->inode : 0);
    dd_ctx->record.me.pname.name = dentry->name;

    dd_ctx->record.stat = dentry->stat;
    dd_ctx->record.options.rdev = (dentry->stat.rdev != 0 ? 1 : 0);
    dd_ctx->record.options.gid = (dentry->stat.gid != 0 ? 1 : 0);
    dd_ctx->record.options.uid = (dentry->stat.uid != 0 ? 1 : 0);
    dd_ctx->record.options.size = (dentry->stat.size != 0 ? 1 : 0);
    dd_ctx->record.options.inc_alloc = (dentry->stat.alloc != 0 ? 1 : 0);
    dd_ctx->record.options.space_end = (dentry->stat.space_end != 0 ? 1 : 0);

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        dd_ctx->record.options.src_inode = 1;
        dd_ctx->record.hdlink.src.inode = dentry->src_dentry->inode;
        dd_ctx->record.options.link = 0;
    } else if (S_ISLNK(dentry->stat.mode)) {
        dd_ctx->record.options.link = 1;
        dd_ctx->record.link = dentry->link;
        dd_ctx->record.options.src_inode = 0;
    } else {
        dd_ctx->record.options.src_inode = 0;
        dd_ctx->record.options.link = 0;
    }

    if (dentry->kv_array != NULL) {
        dd_ctx->record.xattr_kvarray = *dentry->kv_array;
    } else {
        dd_ctx->record.xattr_kvarray.elts = NULL;
        dd_ctx->record.xattr_kvarray.count = 0;
    }

    dd_ctx->buffer.length = 0;
    if ((result=binlog_pack_record_ex(&dd_ctx->pack_ctx,
                    &dd_ctx->record, &dd_ctx->buffer)) != 0)
    {
        return result;
    }

    wbuffer->bf.buff = dd_ctx->buffer.data;
    wbuffer->bf.length = dd_ctx->buffer.length;
    wbuffer->version.first = wbuffer->version.last =
        dd_ctx->record.data_version;
    sf_push_to_binlog_write_queue(&dd_ctx->dump_ctx->bwctx.writer, wbuffer);
    return 0;
}

static int dentry_dump(DataDumperContext *dd_ctx, FDIRServerDentry *dentry)
{
    int result;
    FDIRServerDentry *current;
    UniqSkiplistIterator iterator;

    if (STORAGE_ENABLED) {
        if ((result=dentry_check_load(dentry->context->
                        thread_ctx, dentry)) != 0)
        {
            return result;
        }

        if ((result=dentry_load_xattr(dentry->context->
                        thread_ctx, dentry)) != 0)
        {
            return result;
        }
    }

    if ((result=output_dentry(dd_ctx, dentry)) != 0) {
        return result;
    }

    if (!S_ISDIR(dentry->stat.mode)) {
        return 0;
    }

    uniq_skiplist_iterator(dentry->children, &iterator);
    while ((current=uniq_skiplist_next(&iterator)) != NULL) {
        if ((result=dentry_dump(dd_ctx, current)) != 0) {
            return result;
        }
    }

    if (STORAGE_ENABLED && dentry->context->thread_ctx->
            lru_ctx.target_reclaims > 0)
    {
        FDIRDataThreadContext *thread_ctx;
        int64_t target_reclaims;

        thread_ctx = dentry->context->thread_ctx;
        if ((target_reclaims=thread_ctx->lru_ctx.target_reclaims) > 0) {
            thread_ctx->lru_ctx.target_reclaims = 0;
            dentry_lru_eliminate(thread_ctx, target_reclaims);
        }
    }

    return 0;
}

static void init_record_common_fields(FDIRBinlogRecord *record)
{
    record->options.flags = 0;
    record->options.path_info.ns = 1;
    record->options.path_info.subname = 1;
    record->options.hash_code = 1;
    record->options.mode = 1;
    record->options.atime = 1;
    record->options.btime = 1;
    record->options.ctime = 1;
    record->options.mtime = 1;
    record->operation = BINLOG_OP_DUMP_DENTRY_INT;
}

int binlog_dump_data(struct fdir_data_thread_context *thread_ctx,
        FDIRBinlogDumpContext *dump_ctx)
{
    DataDumperContext dd_ctx;
    const FDIRNamespacePtrArray *ns_parray;
    FDIRNamespaceEntry **ns_entry;
    FDIRNamespaceEntry **ns_end;
    int result;

    memset(&dd_ctx, 0, sizeof(dd_ctx));
    dd_ctx.dump_ctx = dump_ctx;
    if ((result=binlog_pack_context_init(&dd_ctx.pack_ctx)) != 0) {
        return result;
    }

    if ((result=fast_buffer_init_ex(&dd_ctx.buffer, 8 * 1024)) != 0) {
        return result;
    }
    init_record_common_fields(&dd_ctx.record);

    result = 0;
    ns_parray = fdir_namespace_get_all();
    ns_end = ns_parray->namespaces + ns_parray->count;
    for (ns_entry=ns_parray->namespaces; ns_entry<ns_end; ns_entry++) {
        if ((*ns_entry)->thread_ctx == thread_ctx &&
                (*ns_entry)->current.root.ptr != NULL)
        {
            dd_ctx.record.ns = (*ns_entry)->name;
            dd_ctx.record.hash_code = (*ns_entry)->hash_code;
            if ((result=dentry_dump(&dd_ctx, (*ns_entry)->
                            current.root.ptr)) != 0)
            {
                break;
            }
        }
    }

    binlog_pack_context_destroy(&dd_ctx.pack_ctx);
    fast_buffer_destroy(&dd_ctx.buffer);
    return result;
}

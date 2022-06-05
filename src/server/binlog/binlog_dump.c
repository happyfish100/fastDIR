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
#include "binlog_reader.h"
#include "binlog_write.h"
#include "binlog_dump.h"

#define DUMP_FILE_PREFIX_NAME  "dump"

#define DUMP_MARK_ITEM_LAST_DATA_VERSION   "last_data_version"
#define DUMP_MARK_ITEM_NEXT_BINLOG_INDEX   "next_binlog_index"
#define DUMP_MARK_ITEM_NEXT_BINLOG_OFFSET  "next_binlog_offset"


typedef struct dentry_chain_node {
    FDIRServerDentry *dentry;
    struct {
        struct dentry_chain_node *chain;
        struct dentry_chain_node *htable;
    } nexts;
} DEntryChainNode;

typedef struct {
    DEntryChainNode **buckets;
    int capacity;
} DEntryNodeHashtable;

typedef struct {
    DEntryChainNode *head;
    DEntryChainNode *tail;
    int64_t count;
} DEntryNodeList;

typedef struct {
    FastBuffer buffer;
    int64_t version;  //for waiting binlog write done
} VersionedFastBuffer;

typedef struct {
    VersionedFastBuffer *buffers;
    int count;
    uint64_t index;
} VersionedBufferArray;

typedef struct {
    int64_t last_data_version;   //for waiting binlog write done
    int64_t write_done_version;  //the data version of binlog write done
    uint32_t hardlink_count;
    FDIRBinlogRecord record;
    VersionedBufferArray buffer_array;
    BinlogPackContext pack_ctx;
    struct {
        struct fast_mblock_man allocator;  //element: DEntryChainNode
        struct {
            DEntryNodeHashtable htable;
            DEntryNodeList list;
        } orphan;  //hardlink source
        DEntryNodeList list;
    } hardlink;
    FDIRBinlogDumpContext *dump_ctx;
} DataDumperContext;  //for data thread


static int binlog_dump_write_to_mark_file()
{
    char filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    fdir_get_dump_mark_filename(filename, sizeof(filename));
    len = sprintf(buff, "%s=%"PRId64"\n"
            "%s=%d\n"
            "%s=%"PRId64"\n",
            DUMP_MARK_ITEM_LAST_DATA_VERSION, DUMP_LAST_DATA_VERSION,
            DUMP_MARK_ITEM_NEXT_BINLOG_INDEX, DUMP_NEXT_POSITION.index,
            DUMP_MARK_ITEM_NEXT_BINLOG_OFFSET, DUMP_NEXT_POSITION.offset);
    if ((result=safeWriteToFile(filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
    }

    return result;
}

int binlog_dump_load_from_mark_file()
{
    char filename[PATH_MAX];
    IniContext ini_context;
    int result;

    fdir_get_dump_mark_filename(filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        return result == ENOENT ? 0 : result;
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, filename, result);
        return result;
    }

    DUMP_LAST_DATA_VERSION = iniGetInt64Value(NULL,
            DUMP_MARK_ITEM_LAST_DATA_VERSION,
            &ini_context, 0);
    DUMP_NEXT_POSITION.index = iniGetIntValue(NULL,
            DUMP_MARK_ITEM_NEXT_BINLOG_INDEX,
            &ini_context, 0);
    DUMP_NEXT_POSITION.offset = iniGetInt64Value(NULL,
            DUMP_MARK_ITEM_NEXT_BINLOG_OFFSET,
            &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int init_dump_ctx(FDIRBinlogDumpContext *dump_ctx,
        const char *subdir_name)
{
    const uint64_t next_version = 1;
    const int buffer_size = 4 * 1024 * 1024;
    const int ring_size = 1024;
    const short order_mode = SF_BINLOG_THREAD_ORDER_MODE_VARY;
    const int max_record_size = 0;  //use the binlog buffer of the caller
    const int writer_count = 1;
    const bool use_fixed_buffer_size = true;
    char filepath[PATH_MAX];
    int result;

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            subdir_name, filepath, sizeof(filepath));
    if ((result=fc_check_mkdir(filepath, 0755)) != 0) {
        return result;
    }

    if ((result=sf_synchronize_ctx_init(&dump_ctx->sctx)) != 0) {
        return result;
    }

    dump_ctx->result = 0;
    dump_ctx->current_version = 0;
    dump_ctx->orphan_count = 0;
    dump_ctx->hardlink_count = 0;
    if ((result=sf_binlog_writer_init_by_version_ex(&dump_ctx->
                    bwctx.writer, DATA_PATH_STR, subdir_name,
                    DUMP_FILE_PREFIX_NAME, next_version, buffer_size,
                    ring_size, SF_BINLOG_NEVER_ROTATE_FILE)) != 0)
    {
        return result;
    }
    sf_binlog_writer_set_flags(&dump_ctx->bwctx.writer,
            SF_FILE_WRITER_FLAGS_WANT_DONE_VERSION);

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

    if (result != 0) {
        dump_ctx->result = result;
    }
    sf_synchronize_counter_notify(&dump_ctx->sctx, 1);
}

static int binlog_padding(FDIRBinlogDumpContext *dump_ctx)
{
    SFBinlogWriterBuffer *wbuffer;
    FastBuffer buffer;
    FDIRBinlogRecord record;
    int result;

    if ((result=sf_binlog_writer_change_order_by(&dump_ctx->bwctx.writer,
                    SF_BINLOG_WRITER_TYPE_ORDER_BY_NONE)) != 0)
    {
        return result;
    }

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&dump_ctx->
                    bwctx.thread)) == NULL)
    {
        return ENOMEM;
    }

    if ((result=fast_buffer_init_ex(&buffer, 256)) != 0) {
        return result;
    }

    memset(&record, 0, sizeof(record));
    record.operation = BINLOG_OP_NO_OP_INT;
    record.timestamp = g_current_time;
    record.data_version = dump_ctx->last_data_version;
    record.inode = 9007199936325732LL;  //dummy
    record.options.hash_code = 1;

    buffer.length = 0;
    if ((result=binlog_pack_record(&record, &buffer)) != 0) {
        return result;
    }

    wbuffer->bf.buff = buffer.data;
    wbuffer->bf.length = buffer.length;
    wbuffer->version.first = wbuffer->version.last = record.data_version;
    sf_push_to_binlog_write_queue(&dump_ctx->bwctx.writer, wbuffer);

    while (sf_binlog_writer_get_last_version(&dump_ctx->
                bwctx.writer) < record.data_version)
    {
        fc_sleep_ms(10);
    }

    fast_buffer_destroy(&buffer);
    return 0;
}

static int dump_finish(FDIRBinlogDumpContext *dump_ctx,
        const char *subdir_name, char *out_filename)
{
    int result;
    char tmp_filename[PATH_MAX];
    char mark_filename[PATH_MAX];
    SFBinlogFilePosition position;

    if ((result=binlog_find_position(&dump_ctx->hint_pos, dump_ctx->
                    last_data_version, &position)) != 0)
    {
        return result;
    }

    fdir_get_dump_mark_filename(mark_filename, sizeof(mark_filename));
    if ((result=fc_delete_file_ex(mark_filename, "mark")) != 0) {
        return result;
    }

    sf_binlog_writer_get_filename_ex(DATA_PATH_STR,
            subdir_name, DUMP_FILE_PREFIX_NAME, 0,
            tmp_filename, sizeof(tmp_filename));
    if (rename(tmp_filename, out_filename) != 0) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "rename file %s to %s fail, "
                "errno: %d, error info: %s",
                __LINE__, tmp_filename, out_filename,
                result, STRERROR(result));
        return result;
    }

    DUMP_LAST_DATA_VERSION = dump_ctx->last_data_version;
    DUMP_NEXT_POSITION = position;
    return binlog_dump_write_to_mark_file();
}

static int dump_all()
{
#define RECORD_FIXED_COUNT  64
    int result;
    FDIRBinlogDumpContext dump_ctx;
    char out_filename[PATH_MAX];
    struct {
        FDIRBinlogRecord holder[RECORD_FIXED_COUNT];
        FDIRBinlogRecord *elts;
    } records;
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *end;
    int64_t start_time_ms;
    int64_t time_used_ms;
    char buff[16];

    if (FC_ATOMIC_GET(DATA_CURRENT_VERSION) == 0) {
        return ENOENT;
    }

    if ((result=init_dump_ctx(&dump_ctx, FDIR_DATA_DUMP_SUBDIR_NAME)) != 0) {
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
    dump_ctx.last_data_version = FC_ATOMIC_GET(DATA_CURRENT_VERSION);
    binlog_get_current_write_position(&dump_ctx.hint_pos);

    end = records.elts + g_data_thread_vars.thread_array.count;
    for (record=records.elts; record<end; record++) {
        record->record_type = fdir_record_type_query;
        record->operation = SERVER_OP_DUMP_DATA_INT;
        record->notify.func = data_dump_finish_notify;
        record->notify.args = &dump_ctx;
        record->hash_code = (record - records.elts); //for data thread dispatch
        push_to_data_thread_queue(record);
    }

    sf_synchronize_counter_wait(&dump_ctx.sctx);
    result = dump_ctx.result;
    if (result == 0) {
        result = binlog_padding(&dump_ctx);
    }

    if (records.elts != records.holder) {
        free(records.elts);
    }
    destroy_dump_ctx(&dump_ctx);

    if (result == 0) {
        fdir_get_dump_data_filename(out_filename, sizeof(out_filename));
        if ((result=dump_finish(&dump_ctx, FDIR_DATA_DUMP_SUBDIR_NAME,
                        out_filename)) == 0)
        {
            time_used_ms = get_current_time_ms() - start_time_ms;
            long_to_comma_str(time_used_ms, buff);
            logInfo("file: "__FILE__", line: %d, "
                    "dump data to %s done, dentry count: %"PRId64", "
                    "hardlink count: %"PRId64", orphan inode count: "
                    "%"PRId64", time used: %s ms", __LINE__, out_filename,
                    dump_ctx.current_version, dump_ctx.hardlink_count,
                    dump_ctx.orphan_count, buff);
        }
    }

    return result;
}

int binlog_dump_all()
{
    int result;

    if (__sync_bool_compare_and_swap(&FULL_DUMPING, 0, 1)) {
        result = dump_all();
        __sync_bool_compare_and_swap(&FULL_DUMPING, 1, 0);
        return result;
    } else {
        return EINPROGRESS;
    }
}

static int output_dentry(DataDumperContext *dd_ctx,
        FDIRServerDentry *dentry, const bool orphan_inode)
{
    SFBinlogWriterBuffer *wbuffer;
    VersionedFastBuffer *vb;
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
    if (orphan_inode) {
        dd_ctx->record.stat.mode = FDIR_SET_DENTRY_ORPHAN_INODE(
                dd_ctx->record.stat.mode);
    }
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

    vb = dd_ctx->buffer_array.buffers + dd_ctx->buffer_array.
        index++ % dd_ctx->buffer_array.count;
    while (vb->version > dd_ctx->write_done_version) {
        /* wait write done to reuse buffer */
        fc_sleep_ms(1);
        dd_ctx->write_done_version = sf_binlog_writer_get_last_version(
                &dd_ctx->dump_ctx->bwctx.writer);
    }

    vb->version = dd_ctx->record.data_version;
    vb->buffer.length = 0;
    if ((result=binlog_pack_record_ex(&dd_ctx->pack_ctx,
                    &dd_ctx->record, &vb->buffer)) != 0)
    {
        return result;
    }

    wbuffer->bf.buff = vb->buffer.data;
    wbuffer->bf.length = vb->buffer.length;
    wbuffer->version.first = wbuffer->version.last =
        dd_ctx->record.data_version;
    sf_push_to_binlog_write_queue(&dd_ctx->dump_ctx->bwctx.writer, wbuffer);
    dd_ctx->last_data_version = dd_ctx->record.data_version;
    return 0;
}

static inline DEntryChainNode *add_to_list(DataDumperContext *dd_ctx,
        DEntryNodeList *list, FDIRServerDentry *dentry)
{
    DEntryChainNode *node;

    node = fast_mblock_alloc_object(&dd_ctx->hardlink.allocator);
    if (node == NULL) {
        return NULL;
    }

    node->dentry = dentry;
    node->nexts.chain = NULL;
    if (list->head == NULL) {
        list->head = node;
    } else {
        list->tail->nexts.chain = node;
    }
    list->tail = node;
    list->count++;

    return node;
}

static int deal_orphan_dentry(DataDumperContext *dd_ctx,
        FDIRServerDentry *dentry)
{
    DEntryChainNode **bucket;
    DEntryChainNode *previous;
    DEntryChainNode *node;

    bucket = dd_ctx->hardlink.orphan.htable.buckets + dentry->
        inode % dd_ctx->hardlink.orphan.htable.capacity;
    previous = NULL;
    node = *bucket;
    while (node != NULL) {
        if (node->dentry->inode == dentry->inode) {
            return 0;  //already exist
        } else if (node->dentry->inode > dentry->inode) {
            break;
        }

        previous = node;
        node = node->nexts.htable;
    }

    if ((node=add_to_list(dd_ctx, &dd_ctx->hardlink.
                    orphan.list, dentry)) == NULL)
    {
        return ENOMEM;
    }

    if (previous == NULL) {
        node->nexts.htable = *bucket;
        *bucket = node;
    } else {
        node->nexts.htable = previous->nexts.htable;
        previous->nexts.htable = node;
    }

    return 0;
}

static inline int deal_hardlink_dentry(DataDumperContext *dd_ctx,
        FDIRServerDentry *dentry)
{
    if (add_to_list(dd_ctx, &dd_ctx->hardlink.list, dentry) == NULL) {
        return ENOMEM;
    }

    if (dentry->src_dentry->parent == NULL) { //orphan inode
        return deal_orphan_dentry(dd_ctx, dentry->src_dentry);
    } else {
        return 0;
    }
}

static inline int output_dentry_list(DataDumperContext *dd_ctx,
        DEntryNodeList *list, const bool orphan_inode)
{
    int result;
    DEntryChainNode *node;

    node = list->head;
    while (node != NULL) {
        dd_ctx->record.ns = node->dentry->ns_entry->name;
        dd_ctx->record.hash_code = node->dentry->ns_entry->hash_code;
        if ((result=output_dentry(dd_ctx, node->dentry, orphan_inode)) != 0) {
            return result;
        }
        node = node->nexts.chain;
    }

    return 0;
}

static int dentry_dump(DataDumperContext *dd_ctx, FDIRServerDentry *dentry)
{
    const bool orphan_inode = false;
    int result;
    FDIRServerDentry *current;
    UniqSkiplistIterator iterator;

    if (STORAGE_ENABLED) {
        if ((result=dentry_check_load_basic_children(dentry->
                        context->thread_ctx, dentry)) != 0)
        {
            return result;
        }

        if ((result=dentry_check_load_xattr(dentry->context->
                        thread_ctx, dentry)) != 0)
        {
            return result;
        }
    }

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        return deal_hardlink_dentry(dd_ctx, dentry);
    }

    if ((result=output_dentry(dd_ctx, dentry, orphan_inode)) != 0) {
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

static int init_versioned_buffer_array(VersionedBufferArray *array)
{
    VersionedFastBuffer *vb;
    VersionedFastBuffer *end;
    int result;

    array->index = 0;
    array->count = 256;
    array->buffers = fc_malloc(sizeof(VersionedFastBuffer) * array->count);
    if (array->buffers == NULL) {
        return ENOMEM;
    }

    end = array->buffers + array->count;
    for (vb=array->buffers; vb<end; vb++) {
        vb->version = 0;
        if ((result=fast_buffer_init_ex(&vb->buffer, 4 * 1024)) != 0) {
            return result;
        }
    }

    return 0;
}

static void destroy_versioned_buffer_array(VersionedBufferArray *array)
{
    VersionedFastBuffer *vb;
    VersionedFastBuffer *end;

    end = array->buffers + array->count;
    for (vb=array->buffers; vb<end; vb++) {
        fast_buffer_destroy(&vb->buffer);
    }
    free(array->buffers);
}


static int init_dd_ctx(DataDumperContext *dd_ctx,
        FDIRBinlogDumpContext *dump_ctx)
{
    int result;
    int bytes;

    memset(dd_ctx, 0, sizeof(*dd_ctx));
    dd_ctx->dump_ctx = dump_ctx;
    if ((result=binlog_pack_context_init(&dd_ctx->pack_ctx)) != 0) {
        return result;
    }

    if ((result=init_versioned_buffer_array(&dd_ctx->buffer_array)) != 0) {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&dd_ctx->hardlink.allocator,
                    "dentry_chain_node", sizeof(DEntryChainNode),
                    8 * 1024, 0, NULL, NULL, false)) != 0)
    {
        return result;
    }

    dd_ctx->hardlink.orphan.htable.capacity = 175447;
    bytes = sizeof(DEntryChainNode *) * dd_ctx->
        hardlink.orphan.htable.capacity;
    dd_ctx->hardlink.orphan.htable.buckets = fc_malloc(bytes);
    if (dd_ctx->hardlink.orphan.htable.buckets == NULL) {
        return ENOMEM;
    }
    memset(dd_ctx->hardlink.orphan.htable.buckets, 0, bytes);

    init_record_common_fields(&dd_ctx->record);
    return 0;
}

static void destroy_dd_ctx(DataDumperContext *dd_ctx)
{
    binlog_pack_context_destroy(&dd_ctx->pack_ctx);
    destroy_versioned_buffer_array(&dd_ctx->buffer_array);
    fast_mblock_destroy(&dd_ctx->hardlink.allocator);
    free(dd_ctx->hardlink.orphan.htable.buckets);
}

int binlog_dump_data(struct fdir_data_thread_context *thread_ctx,
        FDIRBinlogDumpContext *dump_ctx)
{
    DataDumperContext dd_ctx;
    const FDIRNamespacePtrArray *ns_parray;
    FDIRNamespaceEntry **ns_entry;
    FDIRNamespaceEntry **ns_end;
    int result;

    if ((result=init_dd_ctx(&dd_ctx, dump_ctx)) != 0) {
        return result;
    }

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

    if ((result=output_dentry_list(&dd_ctx, &dd_ctx.
                    hardlink.orphan.list, true)) == 0)
    {
        result = output_dentry_list(&dd_ctx, &dd_ctx.hardlink.list, false);
    }

    FC_ATOMIC_INC_EX(dump_ctx->orphan_count, dd_ctx.
                    hardlink.orphan.list.count);
    FC_ATOMIC_INC_EX(dump_ctx->hardlink_count,
            dd_ctx.hardlink.list.count);

    /*
    logInfo("data thread #%d, last_data_version: %"PRId64", "
            "current write done version: %"PRId64,
            (int)(thread_ctx - g_data_thread_vars.thread_array.contexts),
            dd_ctx.last_data_version, sf_binlog_writer_get_last_version(
                &dump_ctx->bwctx.writer));
                */

    if (result == 0 && dd_ctx.last_data_version > 0) {
        while (sf_binlog_writer_get_last_version(&dump_ctx->
                    bwctx.writer) < dd_ctx.last_data_version)
        {
            fc_sleep_ms(10);
        }
    }

    destroy_dd_ctx(&dd_ctx);
    return result;
}

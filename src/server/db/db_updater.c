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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "../server_global.h"
#include "../ns_manager.h"
#include "dentry_serializer.h"
#include "event_dealer.h"
#include "db_updater.h"

#define REDO_TMP_FILENAME  ".dbstore.tmp"
#define REDO_LOG_FILENAME  "dbstore.redo"

#define REDO_HEADER_FIELD_ID_RECORD_COUNT          1
#define REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION    2
#define REDO_HEADER_FIELD_ID_LAST_DENTRY_VERSION   3

#define REDO_ENTRY_FIELD_ID_VERSION               1
#define REDO_ENTRY_FIELD_ID_INODE                 2
#define REDO_ENTRY_FIELD_ID_INC_ALLOC             3
#define REDO_ENTRY_FIELD_ID_NAMESPACE_ID          4
#define REDO_ENTRY_FIELD_ID_MODE                  5
#define REDO_ENTRY_FIELD_ID_OP_TYPE               6
#define REDO_ENTRY_FIELD_ID_FIELD_INDEX           7
#define REDO_ENTRY_FIELD_ID_FIELD_BUFFER          8

typedef struct db_updater_ctx {
    SafeWriteFileInfo redo;
    FDIRNamespaceDumpContext ns_dump_ctx;
} DBUpdaterCtx;

static DBUpdaterCtx db_updater_ctx;

#define NS_DUMP_CTX  db_updater_ctx.ns_dump_ctx

static int compare_field_version(const FDIRDBUpdateFieldInfo *entry1,
        const FDIRDBUpdateFieldInfo *entry2)
{
    return fc_compare_int64(entry1->version, entry2->version);
}

int db_updater_realloc_dentry_array(FDIRDBUpdateFieldArray *array)
{
    FDIRDBUpdateFieldInfo *entries;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    entries = (FDIRDBUpdateFieldInfo *)fc_malloc(
            sizeof(FDIRDBUpdateFieldInfo) * array->alloc);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (array->entries != NULL) {
        memcpy(entries, array->entries, sizeof(
                    FDIRDBUpdateFieldInfo) * array->count);
        free(array->entries);
    }

    array->entries = entries;
    return 0;
}

static inline int write_buffer_to_file(const FastBuffer *buffer)
{
    int result;

    if (fc_safe_write(db_updater_ctx.redo.fd, buffer->data,
                buffer->length) != buffer->length)
    {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write file %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.tmp_filename,
                result, STRERROR(result));
        return result;
    }
    return 0;
}

static int write_header(FDIRDBUpdaterContext *ctx)
{
    int result;

    sf_serializer_pack_begin(&ctx->buffer);
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_RECORD_COUNT,
                    ctx->array.count)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION,
                    ctx->last_versions.field)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_LAST_DENTRY_VERSION,
                    ctx->last_versions.dentry)) != 0)
    {
        return result;
    }

    sf_serializer_pack_end(&ctx->buffer);

    /*
    logInfo("count: %d, last_versions {field: %"PRId64", dentry: %"PRId64"}, "
            "buffer length: %d", ctx->array.count, ctx->last_versions.field,
            ctx->last_versions.dentry, ctx->buffer.length);
            */

    return write_buffer_to_file(&ctx->buffer);
}

static int write_one_entry(FDIRDBUpdaterContext *ctx,
        const FDIRDBUpdateFieldInfo *entry)
{
    int result;

    sf_serializer_pack_begin(&ctx->buffer);

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_VERSION,
                    entry->version)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_INODE,
                    entry->inode)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_INC_ALLOC,
                    entry->inc_alloc)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_NAMESPACE_ID,
                    entry->namespace_id)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_MODE,
                    entry->mode)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_OP_TYPE,
                    entry->op_type)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_FIELD_INDEX,
                    entry->field_index)) != 0)
    {
        return result;
    }

    if (entry->buffer != NULL) {
        if ((result=sf_serializer_pack_buffer(&ctx->buffer,
                        REDO_ENTRY_FIELD_ID_FIELD_BUFFER,
                        entry->buffer)) != 0)
        {
            return result;
        }
    }

    sf_serializer_pack_end(&ctx->buffer);
    return write_buffer_to_file(&ctx->buffer);
}

static int do_write(FDIRDBUpdaterContext *ctx)
{
    int result;
    FDIRDBUpdateFieldInfo *entry;
    FDIRDBUpdateFieldInfo *end;

    if ((result=write_header(ctx)) != 0) {
        return result;
    }

    end = ctx->array.entries + ctx->array.count;
    for (entry=ctx->array.entries; entry<end; entry++) {
        if ((result=write_one_entry(ctx, entry)) != 0) {
            return result;
        }
    }

    if (fsync(db_updater_ctx.redo.fd) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "fsync file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.tmp_filename,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

static int write_redo_log(FDIRDBUpdaterContext *ctx)
{
    int result;

    if ((result=fc_safe_write_file_open(&db_updater_ctx.redo)) != 0) {
        return result;
    }

    //logInfo("write redo log count =====: %d", ctx->array.count);
    if ((result=do_write(ctx)) != 0) {
        return result;
    }

    return fc_safe_write_file_close(&db_updater_ctx.redo);
}

static int unpack_from_file(SFSerializerIterator *it,
        const char *caption, BufferInfo *buffer)
{
    const int max_size = 256 * 1024 * 1024;
    int result;
    string_t content;

    if ((result=sf_serializer_read_message(db_updater_ctx.
                    redo.fd, buffer, max_size)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "read %s message from file %s fail, "
                "errno: %d, error info: %s", __LINE__, caption,
                db_updater_ctx.redo.filename, result, STRERROR(result));
        return result;
    }

    FC_SET_STRING_EX(content, buffer->buff, buffer->length);
    if ((result=sf_serializer_unpack(it, &content)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "file: %s, unpack %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename, caption,
                it->error_no, it->error_info);
        return result;
    }

    return 0;
}

static int unpack_header(SFSerializerIterator *it,
        FDIRDBUpdaterContext *ctx, BufferInfo *buffer,
        int *record_count)
{
    int result;
    const SFSerializerFieldValue *fv;

    *record_count = 0;
    ctx->last_versions.field = ctx->last_versions.dentry = 0;
    if ((result=unpack_from_file(it, "header", buffer)) != 0) {
        return result;
    }

    while ((fv=sf_serializer_next(it)) != NULL) {
        switch (fv->fid) {
            case REDO_HEADER_FIELD_ID_RECORD_COUNT:
                *record_count = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION:
                ctx->last_versions.field = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_LAST_DENTRY_VERSION:
                ctx->last_versions.dentry = fv->value.n;
                break;
            default:
                break;
        }
    }
    if (*record_count == 0 || ctx->last_versions.field == 0 ||
            ctx->last_versions.dentry == 0)
    {
        logError("file: "__FILE__", line: %d, "
                "file: %s, invalid packed header, record_count: %d, "
                "last_versions {field: %"PRId64", dentry: %"PRId64"}",
                __LINE__, db_updater_ctx.redo.filename, *record_count,
                ctx->last_versions.field, ctx->last_versions.dentry);
        return EINVAL;
    }

    return 0;
}

static int unpack_one_dentry(SFSerializerIterator *it,
        FDIRDBUpdaterContext *ctx, BufferInfo *buffer,
        const int rowno)
{
    int result;
    char caption[32];
    FDIRDBUpdateFieldInfo *entry;
    const SFSerializerFieldValue *fv;

    sprintf(caption, "dentry #%d", rowno);
    if ((result=unpack_from_file(it, caption, buffer)) != 0) {
        return result;
    }

    if (ctx->array.count >= ctx->array.alloc) {
        if ((result=db_updater_realloc_dentry_array(&ctx->array)) != 0) {
            return result;
        }
    }

    entry = ctx->array.entries + ctx->array.count;
    entry->buffer = NULL;
    entry->args = NULL;
    while ((fv=sf_serializer_next(it)) != NULL) {
        switch (fv->fid) {
            case REDO_ENTRY_FIELD_ID_VERSION:
                entry->version = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_INODE:
                entry->inode = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_INC_ALLOC:
                entry->inc_alloc = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_NAMESPACE_ID:
                entry->namespace_id = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_MODE:
                entry->mode = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_OP_TYPE:
                entry->op_type = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_FIELD_INDEX:
                entry->field_index = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_FIELD_BUFFER:
                if ((entry->buffer=dentry_serializer_to_buffer(
                                &fv->value.s)) == NULL)
                {
                    return ENOMEM;
                }
            default:
                break;
        }
    }

    if (it->error_no != 0) {
        logError("file: "__FILE__", line: %d, "
                "file: %s, unpack entry fail, "
                "errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename,
                it->error_no, it->error_info);
        return it->error_no;
    }

    ctx->array.count++;
    return 0;
}

static int do_load(FDIRDBUpdaterContext *ctx)
{
    int result;
    int i;
    int record_count;
    BufferInfo buffer;
    SFSerializerIterator it;

    if ((result=fc_init_buffer(&buffer, 4 * 1024)) != 0) {
        return result;
    }

    sf_serializer_iterator_init(&it);

    ctx->array.count = 0;
    if ((result=unpack_header(&it, ctx, &buffer, &record_count)) != 0) {
        return result;
    }

    for (i=0; i<record_count; i++) {
        if ((result=unpack_one_dentry(&it, ctx, &buffer, i + 1)) != 0) {
            break;
        }
    }

    fc_free_buffer(&buffer);
    sf_serializer_iterator_destroy(&it);
    return result;
}

static int dump_namespaces(FDIRDBUpdaterContext *ctx)
{
    FDIRDBUpdateFieldInfo *entry;
    FDIRDBUpdateFieldInfo *end;
    FDIRNamespaceEntry *ns_entry;
    int change_count;

    change_count = 0;
    end = ctx->array.entries + ctx->array.count;
    for (entry=ctx->array.entries; entry<end; entry++) {
        if (entry->args != NULL) {
            ns_entry = ((FDIRServerDentry *)entry->args)->ns_entry;
        } else {
            ns_entry = fdir_namespace_get_by_id(entry->namespace_id);
            if (ns_entry == NULL) {
                logError("file: "__FILE__", line: %d, "
                        "namespace id: %d not exist",
                        __LINE__, entry->namespace_id);
                return ENOENT;
            }
        }

        if (entry->op_type == da_binlog_op_type_create &&
                entry->field_index == FDIR_PIECE_FIELD_INDEX_BASIC)
        {
            if (ns_entry->delay.root.inode == 0) {
                ns_entry->delay.root.inode = entry->inode;
            }

            if (S_ISDIR(entry->mode)) {
                ns_entry->delay.counts.dir += 1;
            } else {
                ns_entry->delay.counts.file += 1;
            }
            ++change_count;
        } else if (entry->op_type == da_binlog_op_type_remove) {
            if (ns_entry->delay.root.inode == entry->inode) {
                ns_entry->delay.root.inode = 0;
            }

            if (S_ISDIR(entry->mode)) {
                ns_entry->delay.counts.dir -= 1;
            } else {
                ns_entry->delay.counts.file -= 1;
            }
            ++change_count;
        }

        if (entry->inc_alloc != 0) {
            ns_entry->delay.used_bytes += entry->inc_alloc;
            ++change_count;
        }
    }

    if (change_count == 0) {
        return 0;
    }

    NS_DUMP_CTX.last_version = ctx->last_versions.field;
    return fdir_namespace_dump(&NS_DUMP_CTX);
}

static int resume_from_redo_log(FDIRDBUpdaterContext *ctx)
{
    int result;

    if ((db_updater_ctx.redo.fd=open(db_updater_ctx.
                    redo.filename, O_RDONLY)) < 0)
    {
        result = errno != 0 ? errno : EIO;
        if (result == ENOENT) {
            return 0;
        }
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename,
                result, STRERROR(result));
        return result;
    }

    result = do_load(ctx);
    close(db_updater_ctx.redo.fd);

    logInfo("last_versions {field: %"PRId64", dentry: %"PRId64"}",
            ctx->last_versions.field, ctx->last_versions.dentry);

    if (result != 0) {
        return result;
    }
    if ((result=fdir_namespace_load(&NS_DUMP_CTX.last_version)) != 0) {
        return result;
    }

    if (NS_DUMP_CTX.last_version != ctx->last_versions.field) {
        if ((result=dump_namespaces(ctx)) != 0) {
            return result;
        }
    }

    if ((result=STORAGE_ENGINE_REDO_API(&ctx->array)) != 0) {
        return result;
    }

    event_dealer_free_buffers(&ctx->array);
    return fdir_namespace_load_root();
}

int db_updater_init(FDIRDBUpdaterContext *ctx)
{
    int result;

    if ((result=fc_safe_write_file_init(&db_updater_ctx.redo,
                    STORAGE_PATH_STR, REDO_LOG_FILENAME,
                    REDO_TMP_FILENAME)) != 0)
    {
        return result;
    }

    return resume_from_redo_log(ctx);
}

void db_updater_destroy()
{
}

int db_updater_deal(FDIRDBUpdaterContext *ctx)
{
    int result;

    if (ctx->array.count > 1) {
        qsort(ctx->array.entries, ctx->array.count, sizeof(
                    FDIRDBUpdateFieldInfo), (int (*)(const void *,
                            const void *))compare_field_version);
    }

    if ((result=write_redo_log(ctx)) != 0) {
        return result;
    }

    if ((result=dump_namespaces(ctx)) != 0) {
        return result;
    }

    return STORAGE_ENGINE_STORE_API(&ctx->array);
}

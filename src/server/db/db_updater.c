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
#include "../storage/storage_engine.h"
#include "dentry_serializer.h"
#include "db_updater.h"

#define REDO_TMP_FILENAME  ".dbstore.tmp"
#define REDO_LOG_FILENAME  "dbstore.redo"

#define REDO_HEADER_FIELD_ID_RECORD_COUNT          1
#define REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION    2
#define REDO_HEADER_FIELD_ID_LAST_DENTRY_VERSION   3

#define REDO_DENTRY_FIELD_ID_VERSION               1
#define REDO_DENTRY_FIELD_ID_INODE                 2
#define REDO_DENTRY_FIELD_ID_OP_TYPE               3
#define REDO_DENTRY_FIELD_ID_FIELD_INDEX           4
#define REDO_DENTRY_FIELD_ID_FIELD_BUFFER          5

typedef struct db_updater_ctx {
    SafeWriteFileInfo redo;
} DBUpdaterCtx;

static DBUpdaterCtx db_updater_ctx;

static int resume_from_redo_log(const int64_t start_version);

int db_updater_init()
{
    int result;
    int64_t start_version;

    if ((result=fc_safe_write_file_init(&db_updater_ctx.redo,
                    DATA_PATH_STR, REDO_LOG_FILENAME,
                    REDO_TMP_FILENAME)) != 0)
    {
        return result;
    }

    //TODO
    start_version = 0;
    return resume_from_redo_log(start_version);
}

void db_updater_destroy()
{
}

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
    return write_buffer_to_file(&ctx->buffer);
}

static int write_one_entry(FDIRDBUpdaterContext *ctx,
        const FDIRDBUpdateFieldInfo *entry)
{
    int result;

    sf_serializer_pack_begin(&ctx->buffer);

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_VERSION,
                    entry->version)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_INODE,
                    entry->inode)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_OP_TYPE,
                    entry->op_type)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_FIELD_INDEX,
                    entry->field_index)) != 0)
    {
        return result;
    }

    if (entry->buffer != NULL) {
        if ((result=sf_serializer_pack_buffer(&ctx->buffer,
                        REDO_DENTRY_FIELD_ID_FIELD_BUFFER,
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
    FDIRDBUpdateFieldInfo *last;

    if ((result=write_header(ctx)) != 0) {
        return result;
    }

    last = ctx->array.entries + ctx->array.count - 1;
    for (entry = last; entry >= ctx->array.entries; entry--) {
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

    if ((result=do_write(ctx)) != 0) {
        return result;
    }

    return fc_safe_write_file_close(&db_updater_ctx.redo);
}

static int unpack_from_file(SFSerializerIterator *it)
{
    int result;
    int length;
    char buff[1024];
    string_t content;

    if ((length=sf_serializer_read_message(db_updater_ctx.
                    redo.fd, buff, sizeof(buff))) < 0)
    {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename,
                result, STRERROR(result));
        return result;
    }

    FC_SET_STRING_EX(content, buff, length);
    if ((result=sf_serializer_unpack(it, &content)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "file: %s, unpack header fail, "
                "errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename,
                it->error_no, it->error_info);
        return result;
    }

    return 0;
}

static int unpack_header(SFSerializerIterator *it,
        FDIRDBUpdaterContext *ctx, int *record_count)
{
    int result;
    const SFSerializerFieldValue *fv;

    *record_count = 0;
    ctx->last_versions.field = ctx->last_versions.dentry = 0;
    if ((result=unpack_from_file(it)) != 0) {
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

static int unpack_one_dentry(const int64_t start_version,
        SFSerializerIterator *it, FDIRDBUpdaterContext *ctx)
{
    int result;
    FDIRDBUpdateFieldInfo *entry;
    const SFSerializerFieldValue *fv;

    if ((result=unpack_from_file(it)) != 0) {
        return result;
    }

    if (ctx->array.count >= ctx->array.alloc) {
        if ((result=db_updater_realloc_dentry_array(
                        &ctx->array)) != 0)
        {
            return result;
        }
    }
    entry = ctx->array.entries + ctx->array.count;
    entry->buffer = NULL;
    entry->args = NULL;
    while ((fv=sf_serializer_next(it)) != NULL) {
        switch (fv->fid) {
            case REDO_DENTRY_FIELD_ID_VERSION:
                entry->version = fv->value.n;
                if (entry->version < start_version) {
                    return ENOENT;
                }
                break;
            case REDO_DENTRY_FIELD_ID_INODE:
                entry->inode = fv->value.n;
                break;
            case REDO_DENTRY_FIELD_ID_OP_TYPE:
                entry->op_type = fv->value.n;
                break;
            case REDO_DENTRY_FIELD_ID_FIELD_INDEX:
                entry->field_index = fv->value.n;
                break;
            case REDO_DENTRY_FIELD_ID_FIELD_BUFFER:
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

static int unpack_dentries(const int64_t start_version,
        SFSerializerIterator *it, FDIRDBUpdaterContext *ctx,
        const int record_count)
{
    int result;
    int i;

    for (i=0; i<record_count; i++) {
        if ((result=unpack_one_dentry(start_version,
                        it, ctx)) != 0)
        {
            if (result == ENOENT) {
                result = 0;
                break;
            }
            return result;
        }
    }

    if (ctx->array.count > 1) {
        qsort(ctx->array.entries, ctx->array.count, sizeof(
                    FDIRDBUpdateFieldInfo), (int (*)(const void *,
                            const void *))compare_field_version);
    }

    return 0;
}

static int do_load(const int64_t start_version,
        FDIRDBUpdaterContext *ctx)
{
    int result;
    int record_count;
    SFSerializerIterator it;

    sf_serializer_iterator_init(&it);

    if ((result=unpack_header(&it, ctx, &record_count)) != 0) {
        return result;
    }

    if (ctx->last_versions.field >= start_version) {
        result = unpack_dentries(start_version,
                &it, ctx, record_count);
    }

    sf_serializer_iterator_destroy(&it);
    return result;
}

static int resume_from_redo_log(const int64_t start_version)
{
    FDIRDBUpdaterContext ctx;
    int result;

    ctx.array.entries = NULL;
    ctx.array.count = ctx.array.alloc = 0;
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

    result = do_load(start_version, &ctx);
    close(db_updater_ctx.redo.fd);

    if (result == 0) {
    }

    free(ctx.array.entries);
    return result;
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

    return fdir_storage_engine_store(&ctx->array);
}

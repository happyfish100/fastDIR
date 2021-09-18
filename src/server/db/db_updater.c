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
#include "../dentry.h"
#include "dentry_serializer.h"
#include "db_updater.h"

#define REDO_TMP_FILENAME  ".store.tmp"
#define REDO_LOG_FILENAME  "store.redo"

#define REDO_HEADER_FIELD_ID_RECORD_COUNT   1
#define REDO_HEADER_FIELD_ID_LAST_VERSION   2

#define REDO_DENTRY_FIELD_INDEX_BASE              20
#define REDO_DENTRY_FIELD_ID_VERSION               1
#define REDO_DENTRY_FIELD_ID_INODE                 2
#define REDO_DENTRY_FIELD_ID_OP_TYPE               3
#define REDO_DENTRY_FIELD_ID_EMPTY_FIELD_INDEXES   4
#define REDO_DENTRY_FIELD_ID_PACKED_FIELD_INDEXES  5
#define REDO_DENTRY_FIELD_ID_PACKED_PIECE_SIZES    6
#define REDO_DENTRY_FIELD_ID_INDEX_BASIC      \
    (REDO_DENTRY_FIELD_INDEX_BASE + FDIR_PIECE_FIELD_INDEX_BASIC)
#define REDO_DENTRY_FIELD_ID_INDEX_CHILDREN   \
    (REDO_DENTRY_FIELD_INDEX_BASE + FDIR_PIECE_FIELD_INDEX_CHILDREN)
#define REDO_DENTRY_FIELD_ID_INDEX_XATTR      \
    (REDO_DENTRY_FIELD_INDEX_BASE + FDIR_PIECE_FIELD_INDEX_XATTR )


#define PIECE_STORAGE_FIELD_INDEX_FILE_ID  0
#define PIECE_STORAGE_FIELD_INDEX_OFFSET   1
#define PIECE_STORAGE_FIELD_INDEX_SIZE     2
#define PIECE_STORAGE_FIELD_COUNT          3

typedef struct {
    int8_t indexes[FDIR_PIECE_FIELD_COUNT];
    int count;
} FieldIndexArray;

typedef struct {
    int32_t sizes[FDIR_PIECE_FIELD_COUNT];
    int count;
} PiecesSizeArray;

typedef struct db_updater_ctx {
    struct {
        char *filename;
        char *tmp_filename;
        int fd;
    } redo;
} DBUpdaterCtx;

static DBUpdaterCtx db_updater_ctx;

int db_updater_init()
{
    char full_filename[PATH_MAX];

    snprintf(full_filename, sizeof(full_filename), "%s/%s",
            STORAGE_PATH_STR, REDO_LOG_FILENAME);
    db_updater_ctx.redo.filename = fc_strdup(full_filename);
    if (db_updater_ctx.redo.filename == NULL) {
        return ENOMEM;
    }

    snprintf(full_filename, sizeof(full_filename), "%s/%s",
            STORAGE_PATH_STR, REDO_TMP_FILENAME);
    db_updater_ctx.redo.tmp_filename = fc_strdup(full_filename);
    if (db_updater_ctx.redo.tmp_filename == NULL) {
        return ENOMEM;
    }
    db_updater_ctx.redo.fd = -1;

    return 0;
}

void db_updater_destroy()
{
}

static int compare_dentry_version(const FDIRDBUpdaterDentry *entry1,
        const FDIRDBUpdaterDentry *entry2)
{
    return fc_compare_int64(entry1->version, entry2->version);
}

int db_updater_realloc_dentry_array(FDIRDBUpdaterDentryArray *array)
{
    FDIRDBUpdaterDentry *entries;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    entries = (FDIRDBUpdaterDentry *)fc_malloc(
            sizeof(FDIRDBUpdaterDentry) * array->alloc);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (array->entries != NULL) {
        memcpy(entries, array->entries, sizeof(
                    FDIRDBUpdaterDentry) * array->count);
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
                    REDO_HEADER_FIELD_ID_LAST_VERSION,
                    ctx->last_version)) != 0)
    {
        return result;
    }

    sf_serializer_pack_end(&ctx->buffer);
    return write_buffer_to_file(&ctx->buffer);
}

static inline int pack_piece_storage(FastBuffer *buffer,
        const FDIRServerPieceStorage *store, const int fid)
{
    int32_t values[PIECE_STORAGE_FIELD_COUNT];

    values[PIECE_STORAGE_FIELD_INDEX_FILE_ID] = store->file_id;
    values[PIECE_STORAGE_FIELD_INDEX_OFFSET] = store->offset;
    values[PIECE_STORAGE_FIELD_INDEX_SIZE] = store->size;
    return sf_serializer_pack_int32_array(buffer, fid,
            values, PIECE_STORAGE_FIELD_COUNT);
}

static int write_one_entry(FDIRDBUpdaterContext *ctx,
        const FDIRDBUpdaterDentry *entry)
{
    const FDIRDBUpdaterMessage *msg;
    const FDIRDBUpdaterMessage *end;
    FieldIndexArray packed_fields;
    FieldIndexArray empty_fields;
    PiecesSizeArray size_array;
    int result;

    packed_fields.count = 0;
    empty_fields.count = 0;
    size_array.count = 0;
    end = entry->mms.messages + entry->mms.msg_count;
    for (msg=entry->mms.messages; msg<end; msg++) {
        if (msg->buffer != NULL) {
            packed_fields.indexes[packed_fields.count++] = msg->field_index;
            size_array.sizes[size_array.count++] = msg->buffer->length;
        } else {
            empty_fields.indexes[empty_fields.count++] = msg->field_index;
        }
    }

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

    if ((packed_fields.count > 0) && (result=
                sf_serializer_pack_int8_array(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_PACKED_FIELD_INDEXES,
                    packed_fields.indexes, packed_fields.count)) != 0)
    {
        return result;
    }

    if ((empty_fields.count > 0) && (result=
                sf_serializer_pack_int8_array(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_EMPTY_FIELD_INDEXES,
                    empty_fields.indexes, empty_fields.count)) != 0)
    {
        return result;
    }

    if ((size_array.count > 0) && (result=
                sf_serializer_pack_int32_array(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_PACKED_PIECE_SIZES,
                    size_array.sizes, size_array.count)) != 0)
    {
        return result;
    }

    if ((result=pack_piece_storage(&ctx->buffer, entry->
                    fields + FDIR_PIECE_FIELD_INDEX_BASIC,
                    REDO_DENTRY_FIELD_ID_INDEX_BASIC)) != 0)
    {
        return result;
    }

    if ((result=pack_piece_storage(&ctx->buffer, entry->
                    fields + FDIR_PIECE_FIELD_INDEX_CHILDREN,
                    REDO_DENTRY_FIELD_ID_INDEX_CHILDREN)) != 0)
    {
        return result;
    }

    if ((result=pack_piece_storage(&ctx->buffer, entry->
                    fields + FDIR_PIECE_FIELD_INDEX_XATTR,
                    REDO_DENTRY_FIELD_ID_INDEX_XATTR)) != 0)
    {
        return result;
    }

    sf_serializer_pack_end(&ctx->buffer);
    if ((result=write_buffer_to_file(&ctx->buffer)) != 0) {
        return result;
    }

    for (msg=entry->mms.messages; msg<end; msg++) {
        if (msg->buffer != NULL) {
            if ((result=write_buffer_to_file(msg->buffer)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

static int do_write(FDIRDBUpdaterContext *ctx)
{
    int result;
    FDIRDBUpdaterDentry *entry;
    FDIRDBUpdaterDentry *last;

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

    if ((db_updater_ctx.redo.fd=open(db_updater_ctx.redo.tmp_filename,
                    O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
    {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.tmp_filename,
                result, STRERROR(result));
        return result;
    }

    result = do_write(ctx);
    close(db_updater_ctx.redo.fd);
    if (result != 0) {
        return result;
    }

    if (rename(db_updater_ctx.redo.tmp_filename,
                db_updater_ctx.redo.filename) != 0)
    {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "rename file \"%s\" to \"%s\" fail, "
                "errno: %d, error info: %s", __LINE__,
                db_updater_ctx.redo.tmp_filename,
                db_updater_ctx.redo.filename,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

static int unpack_from_file(SFSerializerIterator *it)
{
    int result;
    int length;
    char buff[1024];
    string_t content;

    if ((length=sf_serializer_read_message(db_updater_ctx.
                    redo.fd, buff, sizeof(buff))) != 0)
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
    ctx->last_version = 0;
    if ((result=unpack_from_file(it)) != 0) {
        return result;
    }

    while ((fv=sf_serializer_next(it)) != NULL) {
        switch (fv->fid) {
            case REDO_HEADER_FIELD_ID_RECORD_COUNT:
                *record_count = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_LAST_VERSION:
                ctx->last_version = fv->value.n;
                break;
            default:
                break;
        }
    }
    if (*record_count == 0 || ctx->last_version == 0) {
        logError("file: "__FILE__", line: %d, "
                "file: %s, invalid packed header, "
                "record_count: %d, last_version: %"PRId64,
                __LINE__, db_updater_ctx.redo.filename,
                *record_count, ctx->last_version);
        return EINVAL;
    }

    return 0;
}

static inline void unpack_piece_storage(
        const SFSerializerFieldValue *fv,
        FDIRServerPieceStorage *store)
{
    store->file_id = fv->value.int_array.
        elts[PIECE_STORAGE_FIELD_INDEX_FILE_ID];
    store->offset = fv->value.int_array.
        elts[PIECE_STORAGE_FIELD_INDEX_OFFSET];
    store->size = fv->value.int_array.
        elts[PIECE_STORAGE_FIELD_INDEX_SIZE];
}

static int unpack_one_dentry(const int64_t start_version,
        SFSerializerIterator *it, FDIRDBUpdaterContext *ctx)
{
    int result;
    int i;
    int read_bytes;
    FDIRDBUpdaterDentry *entry;
    FDIRDBUpdaterMessage *msg;
    const SFSerializerFieldValue *fv;
    FieldIndexArray packed_fields;
    PiecesSizeArray size_array;

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
    entry->mms.msg_count = 0;
    packed_fields.count = 0;
    size_array.count = 0;
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
            case REDO_DENTRY_FIELD_ID_EMPTY_FIELD_INDEXES:
                for (i=0; i<fv->value.int_array.count; i++) {
                    msg = entry->mms.messages + entry->mms.msg_count++;
                    msg->field_index = fv->value.int_array.elts[i];
                    msg->buffer = NULL;
                }
                break;
            case REDO_DENTRY_FIELD_ID_PACKED_FIELD_INDEXES:
                for (i=0; i<fv->value.int_array.count; i++) {
                    packed_fields.indexes[i] = fv->value.int_array.elts[i];
                }
                packed_fields.count = fv->value.int_array.count;
                break;
            case REDO_DENTRY_FIELD_ID_PACKED_PIECE_SIZES:
                for (i=0; i<fv->value.int_array.count; i++) {
                    size_array.sizes[i] = fv->value.int_array.elts[i];
                }
                size_array.count = fv->value.int_array.count;
                break;
            case REDO_DENTRY_FIELD_ID_INDEX_BASIC:
                unpack_piece_storage(fv, entry->fields +
                        FDIR_PIECE_FIELD_INDEX_BASIC);
                break;
            case REDO_DENTRY_FIELD_ID_INDEX_CHILDREN:
                unpack_piece_storage(fv, entry->fields +
                        FDIR_PIECE_FIELD_INDEX_CHILDREN);
                break;
            case REDO_DENTRY_FIELD_ID_INDEX_XATTR:
                unpack_piece_storage(fv, entry->fields +
                        FDIR_PIECE_FIELD_INDEX_XATTR);
                break;
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

    for (i=0; i<packed_fields.count; i++) {
        msg = entry->mms.messages + entry->mms.msg_count;
        msg->field_index = packed_fields.indexes[i];
        if ((msg->buffer=dentry_serializer_alloc_buffer(
                        size_array.sizes[i])) == NULL)
        {
            return ENOMEM;
        }

        msg->buffer->length = size_array.sizes[i];
        read_bytes = fc_safe_read(db_updater_ctx.redo.fd,
                msg->buffer->data, msg->buffer->length);
        if (read_bytes != msg->buffer->length) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "try read %d bytes from file %s fail, real "
                    "read bytes: %d, errno: %d, error info: %s",
                    __LINE__, msg->buffer->length, db_updater_ctx.
                    redo.filename, read_bytes, result, STRERROR(result));
            return result;
        }
        entry->mms.msg_count++;
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
                    FDIRDBUpdaterDentry), (int (*)(const void *,
                            const void *))compare_dentry_version);
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

    if (ctx->last_version >= start_version) {
        result = unpack_dentries(start_version,
                &it, ctx, record_count);
    }

    sf_serializer_iterator_destroy(&it);
    return result;
}

static int load_from_redo_log(const int64_t start_version,
        FDIRDBUpdaterContext *ctx)
{
    int result;

    ctx->array.entries = NULL;
    ctx->array.count = ctx->array.alloc = 0;
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

    result = do_load(start_version, ctx);
    close(db_updater_ctx.redo.fd);

    if (result == 0) {
    }

    return result;
}

int db_updater_deal(FDIRDBUpdaterContext *ctx)
{
    int result;

    if (ctx->array.count > 1) {
        qsort(ctx->array.entries, ctx->array.count, sizeof(
                    FDIRDBUpdaterDentry), (int (*)(const void *,
                            const void *))compare_dentry_version);
    }

    if ((result=write_redo_log(ctx)) != 0) {
        return result;
    }

    //TODO

    return 0;
}

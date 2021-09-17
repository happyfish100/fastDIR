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
#define REDO_DENTRY_FIELD_ID_PIECES_SIZE           6
#define REDO_DENTRY_FIELD_ID_INDEX_BASIC      \
    (REDO_DENTRY_FIELD_INDEX_BASE + FDIR_PIECE_FIELD_INDEX_BASIC)
#define REDO_DENTRY_FIELD_ID_INDEX_CHILDREN   \
    (REDO_DENTRY_FIELD_INDEX_BASE + FDIR_PIECE_FIELD_INDEX_CHILDREN)
#define REDO_DENTRY_FIELD_ID_INDEX_XATTR      \
    (REDO_DENTRY_FIELD_INDEX_BASE + FDIR_PIECE_FIELD_INDEX_XATTR )

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
    int32_t values[4];
    int count;

    count = 0;
    values[count++] = store->file_id;
    values[count++] = store->offset;
    values[count++] = store->size;
    return sf_serializer_pack_int32_array(buffer, fid, values, count);
}

static int write_one_entry(FDIRDBUpdaterContext *ctx,
        const FDIRDBUpdaterDentry *entry)
{
    const FDIRDBUpdaterMessage *msg;
    const FDIRDBUpdaterMessage *end;
    //TODO
    int8_t field_indexes[FDIR_PIECE_FIELD_COUNT];
    int field_count;
    int pieces_size;
    int result;

    field_count = 0;
    pieces_size = 0;
    end = entry->mms.messages + entry->mms.msg_count;
    for (msg=entry->mms.messages; msg<end; msg++) {
        if (msg->buffer != NULL) {
            pieces_size += msg->buffer->length;
            //TODO
        }
        field_indexes[field_count++] = msg->field_index;
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

    if ((result=sf_serializer_pack_int8_array(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_PACKED_FIELD_INDEXES,
                    field_indexes, field_count)) != 0)
    {
        return result;
    }
    //TODO: REDO_DENTRY_FIELD_ID_EMPTY_FIELD_INDEXES

    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_DENTRY_FIELD_ID_PIECES_SIZE,
                    pieces_size)) != 0)
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
        if ((result=write_buffer_to_file(msg->buffer)) != 0) {
            return result;
        }
    }

    return 0;
}

static int do_write(FDIRDBUpdaterContext *ctx)
{
    int result;
    FDIRDBUpdaterDentry *entry;
    FDIRDBUpdaterDentry *end;

    if ((result=write_header(ctx)) != 0) {
        return result;
    }

    end = ctx->array.entries + ctx->array.count;
    for (entry=ctx->array.entries; entry<end; entry++) {
        if ((result=write_one_entry(ctx, entry)) != 0) {
            return result;
        }
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

static int compare_dentry_version(const FDIRDBUpdaterDentry *entry1,
        const FDIRDBUpdaterDentry *entry2)
{
    return fc_compare_int64(entry1->version, entry2->version);
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

    return 0;
}

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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include <assert.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/char_converter.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_producer.h"
#include "binlog_pack.h"

#define BINLOG_RECORD_SIZE_MIN_STRLEN     4
#define BINLOG_RECORD_SIZE_MIN_START   1000

#define BINLOG_RECORD_START_TAG_CHAR '<'
#define BINLOG_RECORD_START_TAG_STR  "<rec"
#define BINLOG_RECORD_START_TAG_LEN (sizeof(BINLOG_RECORD_START_TAG_STR) - 1)

#define BINLOG_RECORD_END_TAG_CHAR '\n'
#define BINLOG_RECORD_END_TAG_STR   "/rec>\n"
#define BINLOG_RECORD_END_TAG_LEN   (sizeof(BINLOG_RECORD_END_TAG_STR) - 1)


#define BINLOG_RECORD_FIELD_NAME_LENGTH         2

#define BINLOG_RECORD_FIELD_NAME_DATA_VERSION  "dv"
#define BINLOG_RECORD_FIELD_NAME_INODE         "id"
#define BINLOG_RECORD_FIELD_NAME_PARENT        "pt"  //parent inode
#define BINLOG_RECORD_FIELD_NAME_SRC_PARENT    "sp"  //src parent inode
#define BINLOG_RECORD_FIELD_NAME_SRC_SUBNAME   "sn"
#define BINLOG_RECORD_FIELD_NAME_FLAGS         "fl"  //flags for rename
#define BINLOG_RECORD_FIELD_NAME_OPERATION     "op"
#define BINLOG_RECORD_FIELD_NAME_TIMESTAMP     "ts"
#define BINLOG_RECORD_FIELD_NAME_NAMESPACE     "ns"
#define BINLOG_RECORD_FIELD_NAME_SUBNAME       "nm"
#define BINLOG_RECORD_FIELD_NAME_LINK          "ln"
#define BINLOG_RECORD_FIELD_NAME_MODE          "md"
#define BINLOG_RECORD_FIELD_NAME_ATIME         "at"
#define BINLOG_RECORD_FIELD_NAME_BTIME         "bt"
#define BINLOG_RECORD_FIELD_NAME_CTIME         "ct"
#define BINLOG_RECORD_FIELD_NAME_MTIME         "mt"
#define BINLOG_RECORD_FIELD_NAME_UID           "ui"
#define BINLOG_RECORD_FIELD_NAME_GID           "gi"
#define BINLOG_RECORD_FIELD_NAME_RDEV          "rd"
#define BINLOG_RECORD_FIELD_NAME_FILE_SIZE     "sz"
#define BINLOG_RECORD_FIELD_NAME_SPACE_END     "se"
#define BINLOG_RECORD_FIELD_NAME_HASH_CODE     "hc"
#define BINLOG_RECORD_FIELD_NAME_INC_ALLOC     "ia"
#define BINLOG_RECORD_FIELD_NAME_SRC_INODE     "si"
#define BINLOG_RECORD_FIELD_NAME_XATTR_NAME    "xn"
#define BINLOG_RECORD_FIELD_NAME_XATTR_VALUE   "xv"
#define BINLOG_RECORD_FIELD_NAME_XATTR_LIST    "xl"  //for data dump

#define BINLOG_RECORD_FIELD_NAME_DEST_PARENT   BINLOG_RECORD_FIELD_NAME_PARENT
#define BINLOG_RECORD_FIELD_NAME_DEST_SUBNAME  BINLOG_RECORD_FIELD_NAME_SUBNAME

#define BINLOG_RECORD_FIELD_INDEX_INODE         ('i' * 256 + 'd')
#define BINLOG_RECORD_FIELD_INDEX_PARENT        ('p' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_SRC_PARENT    ('s' * 256 + 'p')
#define BINLOG_RECORD_FIELD_INDEX_SRC_SUBNAME   ('s' * 256 + 'n')
#define BINLOG_RECORD_FIELD_INDEX_FLAGS         ('f' * 256 + 'l')
#define BINLOG_RECORD_FIELD_INDEX_DATA_VERSION  ('d' * 256 + 'v')
#define BINLOG_RECORD_FIELD_INDEX_OPERATION     ('o' * 256 + 'p')
#define BINLOG_RECORD_FIELD_INDEX_TIMESTAMP     ('t' * 256 + 's')
#define BINLOG_RECORD_FIELD_INDEX_NAMESPACE     ('n' * 256 + 's')
#define BINLOG_RECORD_FIELD_INDEX_SUBNAME       ('n' * 256 + 'm')
#define BINLOG_RECORD_FIELD_INDEX_LINK          ('l' * 256 + 'n')
#define BINLOG_RECORD_FIELD_INDEX_MODE          ('m' * 256 + 'd')
#define BINLOG_RECORD_FIELD_INDEX_ATIME         ('a' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_BTIME         ('b' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_CTIME         ('c' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_MTIME         ('m' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_UID           ('u' * 256 + 'i')
#define BINLOG_RECORD_FIELD_INDEX_GID           ('g' * 256 + 'i')
#define BINLOG_RECORD_FIELD_INDEX_RDEV          ('r' * 256 + 'd')
#define BINLOG_RECORD_FIELD_INDEX_FILE_SIZE     ('s' * 256 + 'z')
#define BINLOG_RECORD_FIELD_INDEX_SPACE_END     ('s' * 256 + 'e')
#define BINLOG_RECORD_FIELD_INDEX_HASH_CODE     ('h' * 256 + 'c')
#define BINLOG_RECORD_FIELD_INDEX_INC_ALLOC     ('i' * 256 + 'a')
#define BINLOG_RECORD_FIELD_INDEX_SRC_INODE     ('s' * 256 + 'i')
#define BINLOG_RECORD_FIELD_INDEX_XATTR_NAME    ('x' * 256 + 'n')
#define BINLOG_RECORD_FIELD_INDEX_XATTR_VALUE   ('x' * 256 + 'v')
#define BINLOG_RECORD_FIELD_INDEX_XATTR_LIST    ('x' * 256 + 'l')

#define BINLOG_RECORD_FIELD_TAG_DATA_VERSION_STR \
    BINLOG_RECORD_FIELD_NAME_DATA_VERSION"="
#define BINLOG_RECORD_FIELD_TAG_DATA_VERSION_LEN \
    (sizeof(BINLOG_RECORD_FIELD_TAG_DATA_VERSION_STR) - 1)

#define BINLOG_RECORD_FIELD_TAG_INODE_STR \
    BINLOG_RECORD_FIELD_NAME_INODE"="
#define BINLOG_RECORD_FIELD_TAG_INODE_LEN \
    (sizeof(BINLOG_RECORD_FIELD_TAG_INODE_STR) - 1)

#define BINLOG_RECORD_FIELD_TAG_OPERATION_STR \
    BINLOG_RECORD_FIELD_NAME_OPERATION"="
#define BINLOG_RECORD_FIELD_TAG_OPERATION_LEN \
    (sizeof(BINLOG_RECORD_FIELD_TAG_OPERATION_STR) - 1)

#define BINLOG_RECORD_FIELD_TAG_HASH_CODE_STR \
    BINLOG_RECORD_FIELD_NAME_HASH_CODE"="
#define BINLOG_RECORD_FIELD_TAG_HASH_CODE_LEN \
    (sizeof(BINLOG_RECORD_FIELD_TAG_HASH_CODE_STR) - 1)

#define BINLOG_RECORD_FIELD_TAG_MODE_STR \
    BINLOG_RECORD_FIELD_NAME_MODE"="
#define BINLOG_RECORD_FIELD_TAG_MODE_LEN \
    (sizeof(BINLOG_RECORD_FIELD_TAG_MODE_STR) - 1)

#define BINLOG_RECORD_FIELD_TAG_SRC_INODE_STR \
    BINLOG_RECORD_FIELD_NAME_SRC_INODE"="
#define BINLOG_RECORD_FIELD_TAG_SRC_INODE_LEN \
    (sizeof(BINLOG_RECORD_FIELD_TAG_SRC_INODE_STR) - 1)

#define BINLOG_FIELD_TYPE_INTEGER   'i'
#define BINLOG_FIELD_TYPE_STRING    's'

typedef struct {
    const char *name;
    int type;
    union {
        int64_t n;
        string_t s;
    } value;
} BinlogFieldValue;

typedef struct {
    BinlogPackContext *pctx;
    struct fast_mpool_man *mpool;
    const char *p;
    const char *rec_end;
    BinlogFieldValue fv;
    char *error_info;
    int error_size;
} FieldParserContext;

typedef struct {
    string_t data_version_tag;
    FastCharConverter char_converter;
} BinlogPackGlobalVars;

static BinlogPackGlobalVars pack_global_vars = {
    {
        BINLOG_RECORD_FIELD_TAG_DATA_VERSION_STR,
        BINLOG_RECORD_FIELD_TAG_DATA_VERSION_LEN
    }
};

#define DATA_VERSION_TAG  pack_global_vars.data_version_tag
#define CHAR_CONVERTER    pack_global_vars.char_converter

int binlog_pack_init()
{
#define ESCAPE_CHAR_PAIR_COUNT 8
    FastCharPair pairs[ESCAPE_CHAR_PAIR_COUNT];

    FAST_CHAR_MAKE_PAIR(pairs[0], '\0', '0');
    FAST_CHAR_MAKE_PAIR(pairs[1], '\n', 'n');
    FAST_CHAR_MAKE_PAIR(pairs[2], '\v', 'v');
    FAST_CHAR_MAKE_PAIR(pairs[3], '\f', 'f');
    FAST_CHAR_MAKE_PAIR(pairs[4], '\r', 'r');
    FAST_CHAR_MAKE_PAIR(pairs[5], '\\', '\\');
    FAST_CHAR_MAKE_PAIR(pairs[6], '<',  'l');
    FAST_CHAR_MAKE_PAIR(pairs[7], '>',  'g');

    return char_converter_init_ex(&CHAR_CONVERTER, pairs,
            ESCAPE_CHAR_PAIR_COUNT, FAST_CHAR_OP_ADD_BACKSLASH);
}

int binlog_pack_context_init_ex(BinlogPackContext *context,
        const bool decode_use_mpool, const int alloc_size_once,
        const int init_buff_size)
{
    const char *error_info = NULL;
    const int error_size = 0;
    return fc_init_json_context_ex(&context->json_ctx, decode_use_mpool,
            alloc_size_once, init_buff_size, (char *)error_info, error_size);
}

void binlog_pack_context_destroy(BinlogPackContext *context)
{
    fc_destroy_json_context(&context->json_ctx);
}

static inline const char *get_operation_label(const int operation)
{
    switch (operation) {
        case BINLOG_OP_CREATE_DENTRY_INT:
            return BINLOG_OP_CREATE_DENTRY_STR;
        case BINLOG_OP_REMOVE_DENTRY_INT:
            return BINLOG_OP_REMOVE_DENTRY_STR;
        case BINLOG_OP_RENAME_DENTRY_INT:
            return BINLOG_OP_RENAME_DENTRY_STR;
        case BINLOG_OP_UPDATE_DENTRY_INT:
            return BINLOG_OP_UPDATE_DENTRY_STR;
        case BINLOG_OP_SET_XATTR_INT:
            return BINLOG_OP_SET_XATTR_STR;
        case BINLOG_OP_REMOVE_XATTR_INT:
            return BINLOG_OP_REMOVE_XATTR_STR;
        case BINLOG_OP_DUMP_DENTRY_INT:
            return BINLOG_OP_DUMP_DENTRY_STR;
        case BINLOG_OP_NO_OP_INT:
            return BINLOG_OP_NO_OP_STR;
        default:
            return BINLOG_OP_NONE_STR;
    }
}

static inline int get_operation_integer(const string_t *operation)
{
    if (fc_string_equal2(operation, BINLOG_OP_UPDATE_DENTRY_STR,
                BINLOG_OP_UPDATE_DENTRY_LEN))
    {
        return BINLOG_OP_UPDATE_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_CREATE_DENTRY_STR,
                BINLOG_OP_CREATE_DENTRY_LEN))
    {
        return BINLOG_OP_CREATE_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_REMOVE_DENTRY_STR,
                BINLOG_OP_REMOVE_DENTRY_LEN))
    {
        return BINLOG_OP_REMOVE_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_RENAME_DENTRY_STR,
                BINLOG_OP_RENAME_DENTRY_LEN))
    {
        return BINLOG_OP_RENAME_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_SET_XATTR_STR,
                BINLOG_OP_SET_XATTR_LEN))
    {
        return BINLOG_OP_SET_XATTR_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_REMOVE_XATTR_STR,
                BINLOG_OP_REMOVE_XATTR_LEN))
    {
        return BINLOG_OP_REMOVE_XATTR_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_DUMP_DENTRY_STR,
                BINLOG_OP_DUMP_DENTRY_LEN))
    {
        return BINLOG_OP_DUMP_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_NO_OP_STR,
                BINLOG_OP_NO_OP_LEN))
    {
        return BINLOG_OP_NO_OP_INT;
    } else {
        return BINLOG_OP_NONE_INT;
    }
}

static void binlog_pack_stringl(FastBuffer *buffer, const char *tag_str,
        const int tag_len, const char *val_str, const int val_len,
        const bool need_escape)
{
    char tmp_buff[16];
    char len_buff[16];
    int len1, len2;
    int escape_len;

    memcpy(buffer->data + buffer->length, tag_str, tag_len);
    buffer->length += tag_len;
    buffer->length += fc_itoa(val_len, buffer->data + buffer->length);
    *(buffer->data + buffer->length++) = ',';
    if (need_escape) {
        fast_char_escape(&CHAR_CONVERTER, val_str, val_len,
                buffer->data + buffer->length, &escape_len,
                buffer->alloc_size - buffer->length);
        if (escape_len != val_len) {
            len1 = fc_itoa(val_len, tmp_buff);
            len2 = fc_itoa(escape_len, len_buff);
            if (len2 == len1) {
                memcpy(buffer->data + buffer->length - (len1 + 1),
                        len_buff, len2);  //replace the length only
            } else {
                buffer->length -= len1 + 1;
                memcpy(buffer->data + buffer->length, len_buff, len2);
                buffer->length += len2;
                *(buffer->data + buffer->length++) = ',';

                fast_char_escape(&CHAR_CONVERTER, val_str, val_len,
                        buffer->data + buffer->length, &escape_len,
                        buffer->alloc_size - buffer->length);
            }
        }
        buffer->length += escape_len;
    } else {
        memcpy(buffer->data + buffer->length, val_str, val_len);
        buffer->length += val_len;
    }
}

#define BINLOG_PACK_USER_STRING(buffer, name, value)  \
    binlog_pack_stringl(buffer, " "name"=", sizeof(name) + 1, \
            value.str, value.len, true)

#define BINLOG_PACK_SYS_STRING(buffer, name, val_str, val_len)  \
    binlog_pack_stringl(buffer, " "name"=", sizeof(name) + 1, \
            val_str, val_len, false)

#define BINLOG_PACK_INTEGER_EX(buffer, tag_str, tag_len, value) \
    memcpy(buffer->data + buffer->length, tag_str, tag_len); \
    buffer->length += tag_len; \
    buffer->length += fc_itoa(value, buffer->data + buffer->length)

#define BINLOG_PACK_INTEGER(buffer, name, value)  \
    BINLOG_PACK_INTEGER_EX(buffer, " "name"=", sizeof(name) + 1, value)

int binlog_pack_record_ex(BinlogPackContext *context,
        const FDIRBinlogRecord *record, FastBuffer *buffer)
{
    const BufferInfo *xattrs_buffer;
    string_t op_caption;
    int old_len;
    int expect_len;
    int record_len;
    int width;
    int result;

    expect_len = 512;
    if (record->options.path_info.flags != 0) {
        expect_len += record->ns.len +
                record->me.pname.name.len;
    }
    if (record->options.link) {
        expect_len += record->link.len;
    }
    expect_len *= 2;
    if (record->operation == BINLOG_OP_DUMP_DENTRY_INT &&
            record->xattr_kvarray.count > 0)
    {
        if (context != NULL) {
            xattrs_buffer = fc_encode_json_map(&context->json_ctx,
                    record->xattr_kvarray.elts, record->xattr_kvarray.count);
            if (xattrs_buffer == NULL) {
                result = fc_json_parser_get_error_no(&context->json_ctx);
                logError("file: "__FILE__", line: %d, "
                        "json encode fail, errno: %d, error info: %s",
                        __LINE__, result, fc_json_parser_get_error_info(
                            &context->json_ctx)->str);
                return result;
            }

            expect_len += xattrs_buffer->length;
        } else {
            xattrs_buffer = NULL;
        }
    } else {
        xattrs_buffer = NULL;
    }

    if ((result=fast_buffer_check_inc_size(buffer, expect_len)) != 0) {
        return result;
    }

    if (expect_len < 10 * BINLOG_RECORD_SIZE_MIN_START) {
        width = BINLOG_RECORD_SIZE_MIN_STRLEN;
    } else if (expect_len < 100 * BINLOG_RECORD_SIZE_MIN_START) {
        width = BINLOG_RECORD_SIZE_MIN_STRLEN + 1;
    } else {
        logError("file: "__FILE__", line: %d, "
                "inode: %"PRId64", x-attributes are too long",
                __LINE__, record->inode);
        return EOVERFLOW;
    }

    //reserve record size spaces
    old_len = buffer->length;
    buffer->length += width;

    memcpy(buffer->data + buffer->length, BINLOG_RECORD_START_TAG_STR,
            BINLOG_RECORD_START_TAG_LEN);
    buffer->length += BINLOG_RECORD_START_TAG_LEN;

    BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_DATA_VERSION,
            record->data_version);

    BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_INODE,
            record->inode);

    op_caption.str = (char *)get_operation_label(record->operation);
    op_caption.len = strlen(op_caption.str);
    BINLOG_PACK_SYS_STRING(buffer, BINLOG_RECORD_FIELD_NAME_OPERATION,
            op_caption.str, op_caption.len);

    BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_TIMESTAMP,
            record->timestamp);

    if (record->options.path_info.flags != 0) {
        if (record->me.pname.parent_inode == 0 &&
                record->me.pname.name.len > 0 &&
                record->operation != BINLOG_OP_DUMP_DENTRY_INT)
        {
            logError("file: "__FILE__", line: %d, "
                    "subname: %.*s, expect parent inode", __LINE__,
                    record->me.pname.name.len, record->me.pname.name.str);
            return EINVAL;
        }

        BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_NAMESPACE,
                record->ns);

        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_PARENT,
                record->me.pname.parent_inode);

        BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_SUBNAME,
                record->me.pname.name);
    }

    BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_HASH_CODE,
            record->hash_code);

    if (record->options.link) {
        BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_LINK,
                record->link);
    }

    if (record->options.mode) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_MODE,
                record->stat.mode);
    }

    if (record->options.btime) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_BTIME,
                record->stat.btime);
    }

    if (record->options.atime) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_ATIME,
                record->stat.atime);
    }

    if (record->options.ctime) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_CTIME,
                record->stat.ctime);
    }

    if (record->options.mtime) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_MTIME,
                record->stat.mtime);
    }

    if (record->options.uid) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_UID,
                record->stat.uid);
    }

    if (record->options.gid) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_GID,
                record->stat.gid);
    }

    if (record->options.rdev) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_RDEV,
                record->stat.rdev);
    }

    if (record->options.size) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_FILE_SIZE,
                record->stat.size);
    }

    if (record->options.space_end) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_SPACE_END,
                record->stat.space_end);
    }

    if (record->options.inc_alloc) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_INC_ALLOC,
                record->stat.alloc);
    }

    if (record->options.src_inode) {
        BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_SRC_INODE,
                record->hdlink.src.inode);
    }

    switch (record->operation) {
        case BINLOG_OP_RENAME_DENTRY_INT:
            BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_SRC_PARENT,
                record->rename.src.pname.parent_inode);

            BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_SRC_SUBNAME,
                    record->rename.src.pname.name);

            BINLOG_PACK_INTEGER(buffer, BINLOG_RECORD_FIELD_NAME_FLAGS,
                    record->flags);
            break;
        case BINLOG_OP_SET_XATTR_INT:
            BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_XATTR_NAME,
                    record->xattr.key);
            BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_XATTR_VALUE,
                    record->xattr.value);
            break;
        case BINLOG_OP_REMOVE_XATTR_INT:
            BINLOG_PACK_USER_STRING(buffer, BINLOG_RECORD_FIELD_NAME_XATTR_NAME,
                    record->xattr.key);
            break;
        case BINLOG_OP_DUMP_DENTRY_INT:
            if (xattrs_buffer != NULL) {
                BINLOG_PACK_SYS_STRING(buffer, BINLOG_RECORD_FIELD_NAME_XATTR_LIST,
                        xattrs_buffer->buff, xattrs_buffer->length);
            }
            break;
        default:
            break;
    }

    memcpy(buffer->data + buffer->length, BINLOG_RECORD_END_TAG_STR,
            BINLOG_RECORD_END_TAG_LEN);
    buffer->length += BINLOG_RECORD_END_TAG_LEN;

    record_len = buffer->length - old_len - width;
    if (record_len > FDIR_BINLOG_RECORD_MAX_SIZE - 1) {
        logError("file: "__FILE__", line: %d, "
                "record length: %d is too large, exceeds %d",
                __LINE__, record_len, FDIR_BINLOG_RECORD_MAX_SIZE - 1);
        return EOVERFLOW;
    }

    sprintf(buffer->data + old_len, "%0*d", width, record_len);
    /* restore the start char */
    *(buffer->data + old_len + width) = BINLOG_RECORD_START_TAG_CHAR;
    return 0;
}

int binlog_repack_buffer(const char *input, const int in_len,
        const int64_t new_data_version, char *output, int *out_len)
{
    int64_t old_data_version;
    int old_record_len;
    int new_record_len;
    int old_dv_len;
    int new_dv_len;
    int dv_len_sub;
    int front_len;
    int tail_len;
    string_t sin;
    char *rec_start;
    char *dv_tag;
    char *dv_val;
    char *dv_end;
    char *p;
    char new_dv_buff[32];

    old_record_len  = strtol(input, &rec_start, 10);
    if (*rec_start != BINLOG_RECORD_START_TAG_CHAR) {
        logError("file: "__FILE__", line: %d, "
                "unexpect char: %c(0x%02X), expect start tag char: %c",
                __LINE__, *rec_start, (unsigned char)*rec_start,
                BINLOG_RECORD_START_TAG_CHAR);
        return EINVAL;
    }

    sin.str = rec_start + BINLOG_RECORD_START_TAG_LEN;
    sin.len = in_len - (sin.str - input);
    if ((dv_tag=(char *)fc_memmem(&sin, &DATA_VERSION_TAG)) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "expect data version tag: %s",
                __LINE__, DATA_VERSION_TAG.str);
        return EINVAL;
    }

    dv_val = dv_tag + DATA_VERSION_TAG.len;
    old_data_version = strtoll(dv_val, &dv_end, 10);
    if (*dv_end != ' ') {
        logError("file: "__FILE__", line: %d, "
                "unexpect char: %c(0x%02X), expect space char",
                __LINE__, *dv_end, (unsigned char)*dv_end);
        return EINVAL;
    }

    if (new_data_version == old_data_version) {
        memcpy(output, input, in_len);
        *out_len = in_len;
        return 0;
    }

    old_dv_len = dv_end - dv_val;
    new_dv_len = sprintf(new_dv_buff, "%"PRId64, new_data_version);
    dv_len_sub = new_dv_len - old_dv_len;
    if (dv_len_sub == 0) {
        front_len = dv_val - input;
        memcpy(output, input, front_len);
        p = output + front_len;
    } else {
        new_record_len = old_record_len + dv_len_sub;
        p = output + sprintf(output, "%0*d",
                BINLOG_RECORD_SIZE_MIN_STRLEN,
                new_record_len);

        front_len = dv_val - rec_start;
        memcpy(p, rec_start, front_len);
        p += front_len;
    }

    memcpy(p, new_dv_buff, new_dv_len);
    p += new_dv_len;

    tail_len = (input + in_len) - dv_end;
    memcpy(p, dv_end, tail_len);
    p += tail_len;

    *out_len = p - output;
    return 0;
}

static int binlog_get_next_field_value(FieldParserContext *pcontext)
{
    int remain;
    unsigned char equal_ch;
    const char *value;
    char *endptr;
    int64_t n;

    remain = pcontext->rec_end - pcontext->p;
    if (*pcontext->p == '/') {
        if (remain == BINLOG_RECORD_END_TAG_LEN) {
            pcontext->p += BINLOG_RECORD_END_TAG_LEN;
            return ENOENT;
        } else {
            sprintf(pcontext->error_info, "expect field name, "
                    "but charactor \"/\" found");
            return EINVAL;
        }
    }
    if (*pcontext->p != ' ') {
        sprintf(pcontext->error_info, "expect space charactor, "
                "but charactor %c(0x%02X) found",
                *pcontext->p, (unsigned char)*pcontext->p);
        return EINVAL;
    }
    pcontext->p++;

    if (remain - 1 < BINLOG_RECORD_END_TAG_LEN + 4) {
        sprintf(pcontext->error_info, "record remain length: %d "
                "is too short", remain);
        return EINVAL;
    }

    pcontext->fv.name = pcontext->p;
    equal_ch = *(pcontext->fv.name + BINLOG_RECORD_FIELD_NAME_LENGTH);
    if (equal_ch != '=') {
        sprintf(pcontext->error_info, "unexpect char: %c(0x%02X), expect "
                "the equal char: =", equal_ch, equal_ch);
        return EINVAL;
    }

    value = pcontext->fv.name + BINLOG_RECORD_FIELD_NAME_LENGTH + 1;
    n = strtoll(value, &endptr, 10);
    if (*endptr == ',') {
        pcontext->fv.type = BINLOG_FIELD_TYPE_STRING;

        FC_SET_STRING_EX(pcontext->fv.value.s, (char *)endptr + 1, n);
        pcontext->p = pcontext->fv.value.s.str + n;
        if (pcontext->rec_end - pcontext->p < BINLOG_RECORD_END_TAG_LEN) {
            sprintf(pcontext->error_info, "field: %.*s, value length: %"PRId64
                    " out of bound", BINLOG_RECORD_FIELD_NAME_LENGTH,
                    pcontext->fv.name, n);
            return EINVAL;
        }

        if (memchr(pcontext->fv.value.s.str, '\\',
                    pcontext->fv.value.s.len) != NULL)
        {
            if (pcontext->mpool != NULL) {
                char *unescaped;

                if ((unescaped=fast_mpool_memdup(pcontext->mpool,
                                pcontext->fv.value.s.str,
                                pcontext->fv.value.s.len)) == NULL)
                {
                    sprintf(pcontext->error_info, "alloc %d bytes fail",
                            pcontext->fv.value.s.len);
                    return ENOMEM;
                }
                pcontext->fv.value.s.str = unescaped;
            }
            fast_char_unescape(&CHAR_CONVERTER, pcontext->fv.value.s.str,
                    &pcontext->fv.value.s.len);
        }
    } else if ((*endptr == ' ') || (*endptr == '/' && pcontext->rec_end -
                endptr == BINLOG_RECORD_END_TAG_LEN))
    {
        pcontext->fv.type = BINLOG_FIELD_TYPE_INTEGER;
        pcontext->fv.value.n = n;
        pcontext->p = endptr;
    } else {
        snprintf(pcontext->error_info, pcontext->error_size,
                "unexpect char: %c(0x%02X), expect comma or space char, "
                "record segment: %.*s", *endptr, (unsigned char)*endptr,
                (int)(pcontext->rec_end - pcontext->p), pcontext->p);
        return EINVAL;
    }

    return 0;
}

static inline const char *get_field_type_caption(const int type)
{
    switch (type) {
        case BINLOG_FIELD_TYPE_INTEGER:
            return "integer";
        case BINLOG_FIELD_TYPE_STRING:
            return "string";
        default:
            return "unkown";
    }
}

static int realloc_xattr_kvarray(SFKeyValueArray *kvarray,
        const int target_count)
{
    key_value_pair_t *new_elts;
    int new_alloc;

    if (kvarray->alloc == 0) {
        new_alloc = 64;
    } else {
        new_alloc = 2 * kvarray->alloc;
    }
    while (new_alloc < target_count) {
        new_alloc *= 2;
    }

    new_elts = fc_malloc(sizeof(key_value_pair_t) * new_alloc);
    if (new_elts == NULL) {
        return ENOMEM;
    }

    if (kvarray->elts != NULL) {
        free(kvarray->elts);
    }
    kvarray->elts = new_elts;
    kvarray->alloc = new_alloc;
    return 0;
}

static int parse_xattr_list(FieldParserContext *pcontext,
        FDIRBinlogRecord *record)
{
    const fc_json_map_t *jmap;
    const string_t *error_info;
    key_value_pair_t *src;
    key_value_pair_t *dest;
    key_value_pair_t *end;
    int result;

    jmap = fc_decode_json_map(&pcontext->pctx->json_ctx,
            &pcontext->fv.value.s);
    if (jmap == NULL) {
        error_info = fc_json_parser_get_error_info(
                &pcontext->pctx->json_ctx);
        snprintf(pcontext->error_info, pcontext->error_size,
                "inode: %"PRId64", json decode error: %.*s",
                record->inode, error_info->len, error_info->str);
        return fc_json_parser_get_error_no(&pcontext->pctx->json_ctx);
    }

    if (jmap->count == 0) {
        record->xattr_kvarray.count = 0;
        return 0;
    }

    if (record->xattr_kvarray.alloc < jmap->count) {
        if ((result=realloc_xattr_kvarray(&record->
                        xattr_kvarray, jmap->count)) != 0)
        {
            return result;
        }
    }

    if (pcontext->mpool == NULL) {
        memcpy(record->xattr_kvarray.elts, jmap->elements,
                sizeof(key_value_pair_t) * jmap->count);
        record->xattr_kvarray.count = jmap->count;
        return 0;
    }

    result = 0;
    end = jmap->elements + jmap->count;
    for (src=jmap->elements, dest=record->xattr_kvarray.elts;
            src<end; src++, dest++)
    {
        if ((result=fast_mpool_alloc_string_ex2(pcontext->mpool,
                        &dest->key, &src->key)) != 0)
        {
            break;
        }

        if ((result=fast_mpool_alloc_string_ex2(pcontext->mpool,
                        &dest->value, &src->value)) != 0)
        {
            break;
        }
    }

    record->xattr_kvarray.count = dest - record->xattr_kvarray.elts;
    return result;
}

static int binlog_set_field_value(FieldParserContext *pcontext,
        FDIRBinlogRecord *record)
{
    int index;
    int expect_type;
    int result;

    index = pcontext->fv.name[0] * 256 + pcontext->fv.name[1];
    switch (index) {
        case BINLOG_RECORD_FIELD_INDEX_INODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->inode = pcontext->fv.value.n;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_PARENT:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->me.pname.parent_inode = pcontext->fv.value.n;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_SRC_PARENT:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->rename.src.pname.parent_inode = pcontext->fv.value.n;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_SRC_SUBNAME:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->rename.src.pname.name = pcontext->fv.value.s;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_FLAGS:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->flags = pcontext->fv.value.n;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_DATA_VERSION:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->data_version = pcontext->fv.value.n;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_OPERATION:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->operation = get_operation_integer(
                        &pcontext->fv.value.s);
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_TIMESTAMP:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->timestamp = pcontext->fv.value.n;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_NAMESPACE:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->ns = pcontext->fv.value.s;
                record->options.path_info.ns = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_SUBNAME:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->me.pname.name = pcontext->fv.value.s;
                record->options.path_info.subname = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_LINK:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->link = pcontext->fv.value.s;
                record->options.link = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_MODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.mode = pcontext->fv.value.n;
                record->options.mode = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_BTIME:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.btime = pcontext->fv.value.n;
                record->options.btime = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_ATIME:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.atime = pcontext->fv.value.n;
                record->options.atime = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_CTIME:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.ctime = pcontext->fv.value.n;
                record->options.ctime = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_MTIME:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.mtime = pcontext->fv.value.n;
                record->options.mtime = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_UID:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.uid = pcontext->fv.value.n;
                record->options.uid = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_GID:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.gid = pcontext->fv.value.n;
                record->options.gid = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_RDEV:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.rdev = pcontext->fv.value.n;
                record->options.rdev = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_FILE_SIZE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.size = pcontext->fv.value.n;
                record->options.size = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_SPACE_END:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.space_end = pcontext->fv.value.n;
                record->options.space_end = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_INC_ALLOC:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.alloc = pcontext->fv.value.n;
                record->options.inc_alloc = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_HASH_CODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->hash_code = pcontext->fv.value.n;
                record->options.hash_code = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_SRC_INODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->hdlink.src.inode = pcontext->fv.value.n;
                record->options.src_inode = 1;
            }
            break;

        case BINLOG_RECORD_FIELD_INDEX_XATTR_NAME:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->xattr.key = pcontext->fv.value.s;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_XATTR_VALUE:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->xattr.value = pcontext->fv.value.s;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_XATTR_LIST:
            if (record->operation != BINLOG_OP_DUMP_DENTRY_INT) {
                sprintf(pcontext->error_info, "operation: %s, "
                        "unexpected field name: %.*s (xattr list)",
                        get_operation_label(record->operation),
                        BINLOG_RECORD_FIELD_NAME_LENGTH, pcontext->fv.name);
                return EINVAL;
            }
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                if (pcontext->pctx != NULL) {
                    if ((result=parse_xattr_list(pcontext, record)) != 0) {
                        return result;
                    }
                }
            }
            break;
        default:
            sprintf(pcontext->error_info, "unkown field name: %.*s",
                    BINLOG_RECORD_FIELD_NAME_LENGTH, pcontext->fv.name);
            return ENOENT;
    }

    if (pcontext->fv.type != expect_type) {
        sprintf(pcontext->error_info, "field: %.*s, value type: %s != "
                "expected: %s", BINLOG_RECORD_FIELD_NAME_LENGTH,
                pcontext->fv.name, get_field_type_caption(pcontext->fv.type),
                get_field_type_caption(expect_type));
        return EINVAL;
    }

    return 0;
}
 
static int binlog_check_required_fields(FieldParserContext *pcontext,
        FDIRBinlogRecord *record)
{
    if (record->inode <= 0) {
        sprintf(pcontext->error_info, "expect inode field: %s",
                BINLOG_RECORD_FIELD_NAME_INODE);
        return ENOENT;
    }

    if (record->data_version <= 0) {
        sprintf(pcontext->error_info, "inode: %"PRId64", "
                "expect data version field: %s", record->inode,
                BINLOG_RECORD_FIELD_NAME_DATA_VERSION);
        return ENOENT;
    }

    if (record->options.hash_code == 0) {
        sprintf(pcontext->error_info, "inode: %"PRId64", "
                "expect hash code field: %s", record->inode,
                BINLOG_RECORD_FIELD_NAME_HASH_CODE);
        return ENOENT;
    }

    if (record->operation == BINLOG_OP_NONE_INT) {
        sprintf(pcontext->error_info, "inode: %"PRId64", "
                "expect operation field: %s", record->inode,
                BINLOG_RECORD_FIELD_NAME_OPERATION);
        return ENOENT;
    }

    if (record->timestamp <= 0) {
        sprintf(pcontext->error_info, "inode: %"PRId64", "
                "expect timestamp field: %s", record->inode,
                BINLOG_RECORD_FIELD_NAME_TIMESTAMP);
        return ENOENT;
    }

    if (record->options.path_info.flags != 0 ||
            record->me.pname.parent_inode != 0)
    {
        if (record->options.path_info.ns == 0) {
            sprintf(pcontext->error_info, "inode: %"PRId64", "
                    "expect namespace field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_NAMESPACE);
            return ENOENT;
        }
        if (record->options.path_info.subname == 0) {
            sprintf(pcontext->error_info, "inode: %"PRId64", "
                    "expect subname field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_SUBNAME);
            return ENOENT;
        }
        if (record->me.pname.parent_inode == 0 &&
                record->me.pname.name.len > 0 &&
                record->operation != BINLOG_OP_DUMP_DENTRY_INT)
        {
            sprintf(pcontext->error_info, "inode: %"PRId64", "
                    "expect parent inode field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_PARENT);
            return ENOENT;
        }
    }

    if (record->operation == BINLOG_OP_RENAME_DENTRY_INT) {
        if (record->rename.dest.pname.parent_inode == 0) {
            sprintf(pcontext->error_info, "inode: %"PRId64", expect dest "
                    "parent inode field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_DEST_PARENT);
            return ENOENT;
        }
        if (record->rename.dest.pname.name.len == 0) {
            sprintf(pcontext->error_info, "inode: %"PRId64", expect dest "
                    "subname field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_DEST_SUBNAME);
            return ENOENT;
        }

        if (record->rename.src.pname.parent_inode == 0) {
            sprintf(pcontext->error_info, "inode: %"PRId64", expect src "
                    "parent inode field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_SRC_PARENT);
            return ENOENT;
        }
        if (record->rename.src.pname.name.len == 0) {
            sprintf(pcontext->error_info, "inode: %"PRId64", expect src "
                    "subname field: %s", record->inode,
                    BINLOG_RECORD_FIELD_NAME_SRC_SUBNAME);
            return ENOENT;
        }
    }

    return 0;
}

static int binlog_parse_first_field(FieldParserContext *pcontext,
        FDIRBinlogRecord *record)
{
    int result;

    if ((result=binlog_get_next_field_value(pcontext)) != 0) {
        return result;
    }

    if (memcmp(pcontext->fv.name, BINLOG_RECORD_FIELD_NAME_DATA_VERSION,
                BINLOG_RECORD_FIELD_NAME_LENGTH) != 0)
    {
        sprintf(pcontext->error_info, "first field must be data version (%s)",
                BINLOG_RECORD_FIELD_NAME_DATA_VERSION);
        return EINVAL;
    }

    return binlog_set_field_value(pcontext, record);
}

static int binlog_parse_fields(FieldParserContext *pcontext,
        FDIRBinlogRecord *record)
{
    int result;

    if ((result=binlog_parse_first_field(pcontext, record)) != 0) {
        return result;
    }

    while ((result=binlog_get_next_field_value(pcontext)) == 0) {
        if ((result=binlog_set_field_value(pcontext, record)) != 0) {
            if (result == ENOENT) {
                logWarning("file: "__FILE__", line: %d, "
                        "%s", __LINE__, pcontext->error_info);
            } else {
                return result;
            }
        }
    }

    if (result != ENOENT) {
        return result;
    }

    return binlog_check_required_fields(pcontext, record);
}

static inline int binlog_check_rec_length(const int len,
        FieldParserContext *pcontext)
{
    if (len < FDIR_BINLOG_RECORD_MIN_SIZE) {
        sprintf(pcontext->error_info, "string length: %d is too short", len);
        return EAGAIN;
    }

    return 0;
}

static int binlog_check_record(const char *str, const int len,
        FieldParserContext *pcontext)
{
    int result;
    int record_len;
    int width;
    char *rec_start;

    if ((result=binlog_check_rec_length(len, pcontext)) != 0) {
        return result;
    }

    record_len  = strtol(str, &rec_start, 10);
    if (*rec_start != BINLOG_RECORD_START_TAG_CHAR) {
        sprintf(pcontext->error_info, "unexpect char: %c(0x%02X), "
                "expect start tag char: %c", *rec_start,
                (unsigned char)*rec_start,
                BINLOG_RECORD_START_TAG_CHAR);
        return EINVAL;
    }

    if (memcmp(rec_start, BINLOG_RECORD_START_TAG_STR,
                BINLOG_RECORD_START_TAG_LEN) != 0)
    {
        sprintf(pcontext->error_info, "expect record start tag: %s, "
                "but the start string is: %.*s",
                BINLOG_RECORD_START_TAG_STR,
                (int)BINLOG_RECORD_START_TAG_LEN, rec_start);
        return EINVAL;
    }

    width = rec_start - str;
    if (record_len < FDIR_BINLOG_RECORD_MIN_SIZE - width)
    {
        sprintf(pcontext->error_info, "record length: %d is too short",
                record_len);
        return EINVAL;
    }
    if (record_len > len - width) {
        sprintf(pcontext->error_info, "record length: %d out of bound",
                record_len);
        return EOVERFLOW;
    }

    pcontext->rec_end = rec_start + record_len;
    if (memcmp(pcontext->rec_end - BINLOG_RECORD_END_TAG_LEN,
                BINLOG_RECORD_END_TAG_STR,
                BINLOG_RECORD_END_TAG_LEN) != 0)
    {
        char output[32];
        int out_len;

        fast_char_convert(&CHAR_CONVERTER, pcontext->rec_end -
                BINLOG_RECORD_END_TAG_LEN, BINLOG_RECORD_END_TAG_LEN,
                output, &out_len, sizeof(output));

        sprintf(pcontext->error_info, "expect record end tag: %s, "
                "but the string is: %.*s",
                BINLOG_RECORD_END_TAG_STR, out_len, output);
        return EINVAL;
    }

    pcontext->p = rec_start + BINLOG_RECORD_START_TAG_LEN;
    return 0;
}

#define BINLOG_PACK_SET_ERROR_INFO(pcontext, errinfo, errsize) \
    do { \
        *errinfo = '\0';   \
        pcontext.error_info = errinfo;  \
        pcontext.error_size = errsize;  \
    } while (0)


int binlog_unpack_record_ex(BinlogPackContext *context, const char *str,
        const int len, FDIRBinlogRecord *record, const char **record_end,
        char *error_info, const int error_size, struct fast_mpool_man *mpool)
{
    FieldParserContext pcontext;
    int result;

    memset(record, 0, (long)(&((FDIRBinlogRecord*)0)->notify));
    pcontext.pctx = context;
    pcontext.mpool = mpool;
    BINLOG_PACK_SET_ERROR_INFO(pcontext, error_info, error_size);
    if ((result=binlog_check_record(str, len, &pcontext)) != 0) {
        *record_end = NULL;
        return result;
    }

    record->record_type = fdir_record_type_update;
    record->dentry_type = fdir_dentry_type_inode;
    *record_end = pcontext.rec_end;
    return binlog_parse_fields(&pcontext, record);
}

#define INIT_RECORD_FOR_DETECT(record) \
    record.inode = 0; \
    record.operation = BINLOG_OP_NONE_INT; \
    record.xattr_kvarray.alloc = record.xattr_kvarray.count = 0; \
    record.xattr_kvarray.elts = NULL

int binlog_detect_record(const char *str, const int len,
        int64_t *data_version, const char **rec_end,
        char *error_info, const int error_size)
{
    FDIRBinlogRecord record;
    FieldParserContext pcontext;
    int result;

    BINLOG_PACK_SET_ERROR_INFO(pcontext, error_info, error_size);
    if ((result=binlog_check_record(str, len, &pcontext)) != 0) {
        return result;
    }

    INIT_RECORD_FOR_DETECT(record);
    record.data_version = 0;
    if ((result=binlog_parse_first_field(&pcontext, &record)) != 0) {
        return result;
    }

    *rec_end = pcontext.rec_end;
    *data_version = record.data_version;
    return 0;
}

static bool binlog_is_record_start(const char *str, const int len,
        FieldParserContext *pcontext)
{
    int record_len;
    int width;
    const char *rec_start;

    if (len < FDIR_BINLOG_RECORD_MIN_SIZE) {
        return false;
    }

    record_len = strtol(str, (char **)&rec_start, 10);
    if (*rec_start != BINLOG_RECORD_START_TAG_CHAR) {
       return false;
    }

    width = rec_start - str;
    if (record_len > len - width) {
        return false;
    }

    if (memcmp(rec_start, BINLOG_RECORD_START_TAG_STR,
                BINLOG_RECORD_START_TAG_LEN) != 0)
    {
        return false;
    }

    pcontext->rec_end = rec_start + record_len;
    if (memcmp(pcontext->rec_end - BINLOG_RECORD_END_TAG_LEN,
                BINLOG_RECORD_END_TAG_STR,
                BINLOG_RECORD_END_TAG_LEN) == 0)
    {
        pcontext->p = rec_start + BINLOG_RECORD_START_TAG_LEN;
        return true;
    }
    return false;
}

static int detect_record_start(FieldParserContext *pcontext,
        const char *str, const int len, int *rstart_offset)
{
    const char *rec_start;
    const char *p;
    const char *end;

    *rstart_offset = -1;
    p = str;
    end = str + len;
    while ((end - p >= FDIR_BINLOG_RECORD_MIN_SIZE) && (rec_start=
                (const char *)memchr(p, BINLOG_RECORD_START_TAG_CHAR,
                    end - p)) != NULL)
    {
        const char *start;
        start = rec_start - BINLOG_RECORD_SIZE_MIN_STRLEN;
        while ((start > str) && (*(start-1) >= '0' && *(start-1) <= '9')) {
            --start;
        }

        if ((start >= str) && binlog_is_record_start(
                    start, end - start, pcontext))
        {
            *rstart_offset = start - str;
            break;
        }

        p = rec_start + 1;
    }

    if (*rstart_offset < 0) {
        sprintf(pcontext->error_info, "can't found record start, "
                "input length: %d", len);
        return ENOENT;
    }

    return 0;
}

int binlog_unpack_inode(const char *str, const int len,
        int64_t *inode, const char **record_end,
        char *error_info, const int error_size)
{
    FieldParserContext pcontext;
    int result;

    BINLOG_PACK_SET_ERROR_INFO(pcontext, error_info, error_size);
    if ((result=binlog_check_record(str, len, &pcontext)) != 0) {
        return result;
    }

    while ((result=binlog_get_next_field_value(&pcontext)) == 0) {
        if (memcmp(pcontext.fv.name, BINLOG_RECORD_FIELD_NAME_INODE,
                    BINLOG_RECORD_FIELD_NAME_LENGTH) == 0)
        {
            *inode = pcontext.fv.value.n;
            *record_end = pcontext.rec_end;
            return 0;
        }
    }

    sprintf(error_info, "can't find field: inode");
    return ENOENT;
}

int binlog_extract_inode_operation(const char *str,
        const bool follow_hardlink, int64_t *inode,
        int *operation, unsigned int *hash_code)
{
    char *val;
    char *endptr;
    int mode;
    string_t op_type;

    if ((val=strstr(str, BINLOG_RECORD_FIELD_TAG_INODE_STR)) == NULL) {
        return EINVAL;
    }
    *inode = strtoll(val + BINLOG_RECORD_FIELD_TAG_INODE_LEN, NULL, 10);

    if ((val=strstr(str, BINLOG_RECORD_FIELD_TAG_OPERATION_STR)) == NULL) {
        return EINVAL;
    }
    op_type.len = strtol(val + BINLOG_RECORD_FIELD_TAG_OPERATION_LEN,
            &endptr, 10);
    op_type.str = endptr + 1;  //skip comma
    *operation = get_operation_integer(&op_type);

    if ((val=strstr(str, BINLOG_RECORD_FIELD_TAG_HASH_CODE_STR)) == NULL) {
        return EINVAL;
    }
    *hash_code = strtoll(val + BINLOG_RECORD_FIELD_TAG_HASH_CODE_LEN, NULL, 10);

    if (!(follow_hardlink && *operation == BINLOG_OP_CREATE_DENTRY_INT)) {
        return 0;
    }

    if ((val=strstr(str, BINLOG_RECORD_FIELD_TAG_MODE_STR)) == NULL) {
        return EINVAL;
    }
    mode = strtol(val + BINLOG_RECORD_FIELD_TAG_MODE_LEN, NULL, 10);
    if (!FDIR_IS_DENTRY_HARD_LINK(mode)) {
        return 0;
    }

    if ((val=strstr(str, BINLOG_RECORD_FIELD_TAG_SRC_INODE_STR)) == NULL) {
        return EINVAL;
    }
    *inode = strtoll(val + BINLOG_RECORD_FIELD_TAG_SRC_INODE_LEN, NULL, 10);
    return 0;
}

int binlog_detect_record_forward(const char *str, const int len,
        int64_t *data_version, int *rstart_offset, int *rend_offset,
        char *error_info, const int error_size)
{
    FDIRBinlogRecord record;
    FieldParserContext pcontext;
    int result;

    BINLOG_PACK_SET_ERROR_INFO(pcontext, error_info, error_size);
    if ((result=detect_record_start(&pcontext,
                    str, len, rstart_offset)) != 0)
    {
        return result;
    }

    INIT_RECORD_FOR_DETECT(record);
    if ((result=binlog_parse_first_field(&pcontext, &record)) != 0) {
        return result;
    }

    *rend_offset = pcontext.rec_end - str;
    *data_version = record.data_version;
    return 0;
}

int binlog_detect_record_reverse_ex(const char *str, const int len,
        int64_t *data_version, time_t *timestamp, const char **rec_end,
        char *error_info, const int error_size)
{
    FDIRBinlogRecord record;
    FieldParserContext pcontext;
    const char *rec_start;
    int offset;
    int l;
    int result;

    BINLOG_PACK_SET_ERROR_INFO(pcontext, error_info, error_size);
    if ((result=binlog_check_rec_length(len, &pcontext)) != 0) {
        return result;
    }

    offset = -1;
    l = len;
    while (l > 0 && (rec_start=fc_memrchr(str,
                    BINLOG_RECORD_START_TAG_CHAR, l)) != NULL)
    {
        const char *start;
        start = rec_start - BINLOG_RECORD_SIZE_MIN_STRLEN;
        while ((start > str) && (*(start-1) >= '0' && *(start-1) <= '9')) {
            --start;
        }
        if ((start >= str) && binlog_is_record_start(
                    start, len - (start - str), &pcontext))
        {
            offset = start - str;
            break;
        }

        l = (rec_start - 1) - str;
    }

    if (offset < 0) {
        sprintf(error_info, "can't find record start");
        return ENOENT;
    }

    if (rec_end != NULL) {
        *rec_end = pcontext.rec_end;
    }

    INIT_RECORD_FOR_DETECT(record);
    if ((result=binlog_parse_first_field(&pcontext, &record)) != 0) {
        return result;
    }
    *data_version = record.data_version;

    if (timestamp != NULL) {
        record.timestamp = 0;
        while ((result=binlog_get_next_field_value(&pcontext)) == 0) {
            if (memcmp(pcontext.fv.name, BINLOG_RECORD_FIELD_NAME_TIMESTAMP,
                        BINLOG_RECORD_FIELD_NAME_LENGTH) == 0)
            {
                if ((result=binlog_set_field_value(&pcontext, &record)) != 0) {
                    return result;
                }

                *timestamp = record.timestamp;
                break;
            }
        }

        if (record.timestamp == 0) {
            sprintf(error_info, "can't find field: timestamp");
            return ENOENT;
        }
    }

    return 0;
}

int binlog_detect_last_record_end(const char *str, const int len,
        const char **rec_end)
{
    int l;

    l = len;
    while (l > 0 && (*rec_end=fc_memrchr(str,
                    BINLOG_RECORD_END_TAG_CHAR, l)) != NULL)
    {
        *rec_end += 1;
        l = *rec_end - str;
        if ((l >= BINLOG_RECORD_END_TAG_LEN) && memcmp(
                    *rec_end - BINLOG_RECORD_END_TAG_LEN,
                    BINLOG_RECORD_END_TAG_STR,
                    BINLOG_RECORD_END_TAG_LEN) == 0)
        {
            return 0;
        }
    }

    return ENOENT;
}

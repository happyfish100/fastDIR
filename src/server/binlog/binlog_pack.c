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
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_producer.h"
#include "binlog_pack.h"

#define BINLOG_OP_NONE_INT           0
#define BINLOG_OP_CREATE_DENTRY_INT  1
#define BINLOG_OP_REMOVE_DENTRY_INT  2
#define BINLOG_OP_RENAME_DENTRY_INT  3
#define BINLOG_OP_UPDATE_DENTRY_INT  4

#define BINLOG_OP_NONE_STR           ""
#define BINLOG_OP_CREATE_DENTRY_STR  "create"
#define BINLOG_OP_REMOVE_DENTRY_STR  "remove"
#define BINLOG_OP_RENAME_DENTRY_STR  "rename"
#define BINLOG_OP_UPDATE_DENTRY_STR  "update"

#define BINLOG_RECORD_START_TAG_STR  "<rec"
#define BINLOG_RECORD_START_TAG_LEN (sizeof(BINLOG_RECORD_START_TAG_STR) - 1)

#define BINLOG_RECORD_END_TAG_STR   "/rec>\n"
#define BINLOG_RECORD_END_TAG_LEN   (sizeof(BINLOG_RECORD_END_TAG_STR) - 1)

#define BINLOG_RECORD_FIELD_NAME_LENGTH         2

#define BINLOG_RECORD_FIELD_NAME_INODE         "id"
#define BINLOG_RECORD_FIELD_NAME_DATA_VERSION  "dv"
#define BINLOG_RECORD_FIELD_NAME_OPERATION     "op"
#define BINLOG_RECORD_FIELD_NAME_NAMESPACE     "ns"
#define BINLOG_RECORD_FIELD_NAME_PATH          "pt"
#define BINLOG_RECORD_FIELD_NAME_EXTRA_DATA    "ex"
#define BINLOG_RECORD_FIELD_NAME_USER_DATA     "us"
#define BINLOG_RECORD_FIELD_NAME_MODE          "md"
#define BINLOG_RECORD_FIELD_NAME_CTIME         "ct"
#define BINLOG_RECORD_FIELD_NAME_MTIME         "mt"
#define BINLOG_RECORD_FIELD_NAME_FILE_SIZE     "sz"
#define BINLOG_RECORD_FIELD_NAME_HASH_CODE     "hc"

#define BINLOG_RECORD_FIELD_INDEX_INODE         ('i' * 256 + 'd')
#define BINLOG_RECORD_FIELD_INDEX_DATA_VERSION  ('d' * 265 + 'v')
#define BINLOG_RECORD_FIELD_INDEX_OPERATION     ('o' * 256 + 'p')
#define BINLOG_RECORD_FIELD_INDEX_NAMESPACE     ('n' * 256 + 's')
#define BINLOG_RECORD_FIELD_INDEX_PATH          ('p' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_EXTRA_DATA    ('e' * 256 + 'x')
#define BINLOG_RECORD_FIELD_INDEX_USER_DATA     ('u' * 256 + 's')
#define BINLOG_RECORD_FIELD_INDEX_MODE          ('m' * 256 + 'd')
#define BINLOG_RECORD_FIELD_INDEX_CTIME         ('c' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_MTIME         ('m' * 256 + 't')
#define BINLOG_RECORD_FIELD_INDEX_FILE_SIZE     ('s' * 256 + 'z')
#define BINLOG_RECORD_FIELD_INDEX_HASH_CODE     ('h' * 256 + 'c')

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
    const char *p;
    const char *end;
    BinlogFieldValue fv;
    char *error_info;
} FieldParserContext;

int binlog_pack_init()
{
   return 0;
}

static inline const char *get_operation_caption(const int operation)
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
        default:
            return BINLOG_OP_NONE_STR;
    }
}

static inline int get_operation_int(const char *operation)
{
    if (strcmp(operation, BINLOG_OP_UPDATE_DENTRY_STR) == 0) {
        return BINLOG_OP_UPDATE_DENTRY_INT;
    } else if (strcmp(operation, BINLOG_OP_CREATE_DENTRY_STR) == 0) {
        return BINLOG_OP_CREATE_DENTRY_INT;
    } else if (strcmp(operation, BINLOG_OP_REMOVE_DENTRY_STR) == 0) {
        return BINLOG_OP_REMOVE_DENTRY_INT;
    } else if (strcmp(operation, BINLOG_OP_RENAME_DENTRY_STR) == 0) {
        return BINLOG_OP_RENAME_DENTRY_INT;
    } else {
        return BINLOG_OP_NONE_INT;
    }
}

#define BINLOG_PACK_STRINGL(buffer, name, val, len) \
    do { \
        fast_buffer_append(buffer, " %s=%d,", name, len); \
        fast_buffer_append_buff(buffer, val, len);  \
    } while (0)

#define BINLOG_PACK_STRING(buffer, name, value) \
    BINLOG_PACK_STRINGL(buffer, name, value.str, value.len)

int binlog_pack_record(const FDIRBinlogRecord *record, FastBuffer *buffer)
{
    string_t op_caption;

    fast_buffer_append_buff(buffer, BINLOG_RECORD_START_TAG_STR,
            BINLOG_RECORD_START_TAG_LEN);

    fast_buffer_append(buffer, " %s=%"PRId64,
            BINLOG_RECORD_FIELD_NAME_DATA_VERSION, record->data_version);

    fast_buffer_append(buffer, " %s=%"PRId64,
            BINLOG_RECORD_FIELD_NAME_INODE, record->inode);

    op_caption.str = (char *)get_operation_caption(record->operation);
    op_caption.len = strlen(op_caption.str);
    BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_OPERATION, op_caption);

    if (record->options.fields.path) {
        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_NAMESPACE,
                record->path.fullname->ns);

        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_PATH,
                record->path.fullname->path);

        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_HASH_CODE,
                record->path.hash_code);
    }

    if (record->options.fields.extra_data) {
        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_EXTRA_DATA,
                record->extra_data);
    }

    if (record->options.fields.user_data) {
        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_USER_DATA,
                record->user_data);
    }

    if (record->options.fields.mode) {
        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_MODE, record->stat.mode);
    }

    if (record->options.fields.ctime) {
        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_CTIME, record->stat.ctime);
    }

    if (record->options.fields.mtime) {
        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_MTIME, record->stat.mtime);
    }

    if (record->options.fields.size) {
        fast_buffer_append(buffer, " %s=%"PRId64,
                BINLOG_RECORD_FIELD_NAME_FILE_SIZE, record->stat.size);
    }

    fast_buffer_append_buff(buffer, BINLOG_RECORD_END_TAG_STR,
            BINLOG_RECORD_END_TAG_LEN);
    return 0;
}

static int binlog_get_next_field_value(FieldParserContext *pcontext)
{
    int remain;
    unsigned char equal_ch;
    const char *value;
    char *endptr;
    int64_t n;

    remain = pcontext->end - pcontext->p;
    if (*pcontext->p == '/') {
        if (remain >= BINLOG_RECORD_END_TAG_LEN &&
                memcmp(pcontext->p, BINLOG_RECORD_END_TAG_STR,
                    BINLOG_RECORD_END_TAG_LEN) == 0)
        {
            pcontext->p += BINLOG_RECORD_END_TAG_LEN;
            return ENOENT;
        } else {
            sprintf(pcontext->error_info, "expect field name, "
                    "but charactor \"/\" found");
            return EINVAL;
        }
    }

    if (remain < BINLOG_RECORD_END_TAG_LEN + 4) {
        sprintf(pcontext->error_info, "record remain length: %d "
                "is too short", remain);
        return EINVAL;
    }

    pcontext->fv.name = pcontext->p;
    equal_ch = *(pcontext->fv.name + BINLOG_RECORD_FIELD_NAME_LENGTH);
    if (equal_ch != '=') {
        sprintf(pcontext->error_info, "unexpect char: %c(0x%02X), expect "
                "the equal char \"=\"", equal_ch, equal_ch);
        return EINVAL;
    }

    value = pcontext->fv.name + BINLOG_RECORD_FIELD_NAME_LENGTH + 1;
    endptr = NULL;
    n = strtoll(value, &endptr, 10);
    if (endptr >= pcontext->end) {
        sprintf(pcontext->error_info, "unexpect end of record, "
                "expect end tag: %s", BINLOG_RECORD_END_TAG_STR);
        return EINVAL;
    }
    if (*endptr == ',') {
        pcontext->fv.type = BINLOG_FIELD_TYPE_STRING;

        //TODO check boundary
        FC_SET_STRING_EX(pcontext->fv.value.s, (char *)endptr + 1, n);
        pcontext->p = pcontext->fv.value.s.str + n;
    } else if (*endptr == ' ') {
        pcontext->fv.type = BINLOG_FIELD_TYPE_INTEGER;
        pcontext->fv.value.n = n;
        pcontext->p = endptr;
    } else {
        sprintf(pcontext->error_info, "unexpect char: %c(0x%02X), "
                "expect comma or space char", *endptr, *endptr);
        return EINVAL;
    }

    return 0;
}

int binlog_unpack_record(const char *str, const int len,
        FDIRBinlogRecord *record, const char **record_end,
        char *error_info)
{
    int result;
    FieldParserContext pcontext;

    if (len <= 32 + BINLOG_RECORD_START_TAG_LEN + BINLOG_RECORD_END_TAG_LEN) {
        sprintf(error_info, "record length: %d is too short", len);
        return EINVAL;
    }

    if (memcmp(str, BINLOG_RECORD_START_TAG_STR,
                BINLOG_RECORD_START_TAG_LEN) != 0)
    {
        sprintf(error_info, "expect record start tag: %s, "
                "but the start string is: %.*s",
                BINLOG_RECORD_START_TAG_STR,
                (int)BINLOG_RECORD_START_TAG_LEN, str);
        return EINVAL;
    }

    pcontext.p = str + BINLOG_RECORD_START_TAG_LEN;
    pcontext.end = str + len;
    pcontext.error_info = error_info;
    if ((result=binlog_get_next_field_value(&pcontext)) != 0) {
        return result;
    }

    if (memcmp(pcontext.fv.name, BINLOG_RECORD_FIELD_NAME_DATA_VERSION,
                BINLOG_RECORD_FIELD_NAME_LENGTH) != 0)
    {
        sprintf(error_info, "first field must be data version (%s)",
                BINLOG_RECORD_FIELD_NAME_DATA_VERSION);
        return EINVAL;
    }

    //TODO set data version
    //...
    return 0;
}

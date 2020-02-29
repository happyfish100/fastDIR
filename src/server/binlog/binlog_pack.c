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

#define BINLOG_OP_CREATE_DENTRY_LEN  (sizeof(BINLOG_OP_CREATE_DENTRY_STR) - 1)
#define BINLOG_OP_REMOVE_DENTRY_LEN  (sizeof(BINLOG_OP_REMOVE_DENTRY_STR) - 1)
#define BINLOG_OP_RENAME_DENTRY_LEN  (sizeof(BINLOG_OP_RENAME_DENTRY_STR) - 1)
#define BINLOG_OP_UPDATE_DENTRY_LEN  (sizeof(BINLOG_OP_UPDATE_DENTRY_STR) - 1)


#define BINLOG_RECORD_START_TAG_CHAR '<'
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
    const char *rec_end;
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

static inline int get_operation_integer(const string_t *operation)
{
    if (fc_string_equal2(operation, BINLOG_OP_UPDATE_DENTRY_STR,
                BINLOG_OP_UPDATE_DENTRY_LEN) == 0)
    {
        return BINLOG_OP_UPDATE_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_CREATE_DENTRY_STR,
                BINLOG_OP_CREATE_DENTRY_LEN) == 0)
    {
        return BINLOG_OP_CREATE_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_REMOVE_DENTRY_STR,
                BINLOG_OP_REMOVE_DENTRY_LEN) == 0)
    {
        return BINLOG_OP_REMOVE_DENTRY_INT;
    } else if (fc_string_equal2(operation, BINLOG_OP_RENAME_DENTRY_STR,
                BINLOG_OP_RENAME_DENTRY_LEN) == 0)
    {
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
    int old_len;
    int record_len;
    int result;

    if ((result=fast_buffer_check(buffer, 1024)) != 0) {
        return result;
    }

    //reserve record size spaces
    old_len = buffer->length;
    buffer->length += BINLOG_RECORD_SIZE_STRLEN;

    fast_buffer_append(buffer, " %s=%"PRId64,
            BINLOG_RECORD_FIELD_NAME_DATA_VERSION, record->data_version);

    fast_buffer_append_buff(buffer, BINLOG_RECORD_START_TAG_STR,
            BINLOG_RECORD_START_TAG_LEN);

    fast_buffer_append(buffer, " %s=%"PRId64,
            BINLOG_RECORD_FIELD_NAME_DATA_VERSION, record->data_version);

    fast_buffer_append(buffer, " %s=%"PRId64,
            BINLOG_RECORD_FIELD_NAME_INODE, record->inode);

    op_caption.str = (char *)get_operation_caption(record->operation);
    op_caption.len = strlen(op_caption.str);
    BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_OPERATION, op_caption);

    if (record->options.path_info.flags != 0) {
        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_NAMESPACE,
                record->path.fullname.ns);

        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_PATH,
                record->path.fullname.path);

        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_HASH_CODE,
                record->path.hash_code);
    }

    if (record->options.extra_data) {
        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_EXTRA_DATA,
                record->extra_data);
    }

    if (record->options.user_data) {
        BINLOG_PACK_STRING(buffer, BINLOG_RECORD_FIELD_NAME_USER_DATA,
                record->user_data);
    }

    if (record->options.mode) {
        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_MODE, record->stat.mode);
    }

    if (record->options.ctime) {
        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_CTIME, record->stat.ctime);
    }

    if (record->options.mtime) {
        fast_buffer_append(buffer, " %s=%d",
                BINLOG_RECORD_FIELD_NAME_MTIME, record->stat.mtime);
    }

    if (record->options.size) {
        fast_buffer_append(buffer, " %s=%"PRId64,
                BINLOG_RECORD_FIELD_NAME_FILE_SIZE, record->stat.size);
    }

    fast_buffer_append_buff(buffer, BINLOG_RECORD_END_TAG_STR,
            BINLOG_RECORD_END_TAG_LEN);

    record_len = buffer->length - old_len - BINLOG_RECORD_SIZE_STRLEN;
    if (record_len > BINLOG_RECORD_MAX_SIZE) {
        logError("file: "__FILE__", line: %d, "
                "record length: %d is too large, exceeds %d",
                __LINE__, record_len, BINLOG_RECORD_MAX_SIZE);
        return EOVERFLOW;
    }

    sprintf(buffer->data + old_len, BINLOG_RECORD_SIZE_PRINTF_FMT, record_len);
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
    endptr = NULL;
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
    } else if (*endptr == ' ') {
        pcontext->fv.type = BINLOG_FIELD_TYPE_INTEGER;
        pcontext->fv.value.n = n;
        pcontext->p = endptr;
    } else {
        sprintf(pcontext->error_info, "unexpect char: %c(0x%02X), "
                "expect comma or space char", *endptr, (unsigned char)*endptr);
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

static int binlog_set_field_value(FieldParserContext *pcontext,
        FDIRBinlogRecord *record)
{
    int index;
    int expect_type;

    index = pcontext->fv.name[0] * 256 + pcontext->fv.name[1];
    switch (index) {
        case BINLOG_RECORD_FIELD_INDEX_INODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->inode = pcontext->fv.value.n;
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
        case BINLOG_RECORD_FIELD_INDEX_NAMESPACE:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->path.fullname.ns = pcontext->fv.value.s;
                record->options.path_info.ns = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_PATH:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->path.fullname.path = pcontext->fv.value.s;
                record->options.path_info.pt = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_EXTRA_DATA:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->extra_data = pcontext->fv.value.s;
                record->options.extra_data = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_USER_DATA:
            expect_type = BINLOG_FIELD_TYPE_STRING;
            if (pcontext->fv.type == expect_type) {
                record->user_data = pcontext->fv.value.s;
                record->options.user_data = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_MODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.mode = pcontext->fv.value.n;
                record->options.mode = 1;
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
        case BINLOG_RECORD_FIELD_INDEX_FILE_SIZE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->stat.size = pcontext->fv.value.n;
                record->options.size = 1;
            }
            break;
        case BINLOG_RECORD_FIELD_INDEX_HASH_CODE:
            expect_type = BINLOG_FIELD_TYPE_INTEGER;
            if (pcontext->fv.type == expect_type) {
                record->path.hash_code = pcontext->fv.value.n;
                record->options.path_info.hc = 1;
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
        sprintf(pcontext->error_info, "expect data version field: %s",
                BINLOG_RECORD_FIELD_NAME_DATA_VERSION);
        return ENOENT;
    }

    if (record->operation == BINLOG_OP_NONE_INT) {
        sprintf(pcontext->error_info, "expect operation field: %s",
                BINLOG_RECORD_FIELD_NAME_OPERATION);
        return ENOENT;
    }

    if (record->options.path_info.flags != 0) {
        if (record->options.path_info.ns == 0) {
            sprintf(pcontext->error_info, "expect namespace field: %s",
                    BINLOG_RECORD_FIELD_NAME_NAMESPACE);
            return ENOENT;
        }
        if (record->options.path_info.pt == 0) {
            sprintf(pcontext->error_info, "expect path field: %s",
                    BINLOG_RECORD_FIELD_NAME_PATH);
            return ENOENT;
        }
        if (record->options.path_info.hc == 0) {
            sprintf(pcontext->error_info, "expect hash code field: %s",
                    BINLOG_RECORD_FIELD_NAME_HASH_CODE);
            return ENOENT;
        }
    }

    return 0;
}

static int binlog_parse_fields(FieldParserContext *pcontext,
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

    if ((result=binlog_set_field_value(pcontext, record)) != 0) {
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

int binlog_unpack_record(const char *str, const int len,
        FDIRBinlogRecord *record, const char **record_end,
        char *error_info)
{
    int record_len;
    FieldParserContext pcontext;
    char *rec_start;

    memset(record, 0, sizeof(*record));
    *record_end = NULL;
    if (len <= 32 + BINLOG_RECORD_SIZE_STRLEN + BINLOG_RECORD_START_TAG_LEN
            + BINLOG_RECORD_END_TAG_LEN)
    {
        sprintf(error_info, "string length: %d is too short", len);
        return EINVAL;
    }

    rec_start = NULL;
    record_len  = strtoll(str, &rec_start, 10);
    if (*rec_start != BINLOG_RECORD_START_TAG_CHAR) {
        sprintf(error_info, "unexpect char: %c(0x%02X), "
                "expect start tag char: %c", *rec_start,
                (unsigned char)*rec_start,
                BINLOG_RECORD_START_TAG_CHAR);
        return EINVAL;
    }

    if (memcmp(rec_start, BINLOG_RECORD_START_TAG_STR,
                BINLOG_RECORD_START_TAG_LEN) != 0)
    {
        sprintf(error_info, "expect record start tag: %s, "
                "but the start string is: %.*s",
                BINLOG_RECORD_START_TAG_STR,
                (int)BINLOG_RECORD_START_TAG_LEN, rec_start);
        return EINVAL;
    }

    if (record_len <= 32 + BINLOG_RECORD_START_TAG_LEN +
            BINLOG_RECORD_END_TAG_LEN)
    {
        sprintf(error_info, "record length: %d is too short", record_len);
        return EINVAL;
    }
    if (record_len > len - BINLOG_RECORD_SIZE_STRLEN) {
        sprintf(error_info, "record length: %d out of bound", record_len);
        return EINVAL;
    }

    pcontext.rec_end = str + BINLOG_RECORD_SIZE_STRLEN + record_len;
    if (memcmp(pcontext.rec_end - BINLOG_RECORD_END_TAG_LEN,
                BINLOG_RECORD_END_TAG_STR,
                BINLOG_RECORD_END_TAG_LEN) != 0)
    {
        sprintf(error_info, "expect record end tag: %s, "
                "but the string is: %.*s",
                BINLOG_RECORD_END_TAG_STR,
                (int)BINLOG_RECORD_END_TAG_LEN,
                pcontext.rec_end - BINLOG_RECORD_END_TAG_LEN);
        return EINVAL;
    }

    *record_end = pcontext.rec_end;
    pcontext.p = rec_start + BINLOG_RECORD_START_TAG_LEN;
    pcontext.error_info = error_info;
    return binlog_parse_fields(&pcontext, record);
}

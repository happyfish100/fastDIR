//binlog_pack.h

#ifndef _BINLOG_PACK_H_
#define _BINLOG_PACK_H_

#include "binlog_types.h"

#define BINLOG_RECORD_MAX_SIZE          9999
#define BINLOG_RECORD_SIZE_STRLEN          4
#define BINLOG_RECORD_SIZE_PRINTF_FMT  "%04d"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_pack_init();

int binlog_pack_record(const FDIRBinlogRecord *record, FastBuffer *buffer);

int binlog_unpack_record(const char *str, const int len,
        FDIRBinlogRecord *record, const char **record_end,
        char *error_info);

int binlog_detect_record(const char *str, const int len,
        int64_t *data_version, char *error_info);

int binlog_detect_record_reverse(const char *str, const int len,
        int64_t *data_version, int *offset, char *error_info);

#ifdef __cplusplus
}
#endif

#endif

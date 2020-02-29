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

#ifdef __cplusplus
}
#endif

#endif

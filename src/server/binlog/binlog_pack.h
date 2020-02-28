//binlog_pack.h

#ifndef _BINLOG_PACK_H_
#define _BINLOG_PACK_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_pack_init();

int binlog_pack_record(const FDIRBinlogRecord *record, FastBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif

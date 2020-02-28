//binlog_reader.h

#ifndef _BINLOG_READER_H_
#define _BINLOG_READER_H_

#include "binlog_types.h"

#define BINLOG_FILE_PREFIX     "binlog"
#define BINLOG_FILE_EXT_FMT    ".%05d"

typedef struct {
    int index;      //current binlog file
    int64_t offset; //current file offset
} ServerBinlogFilePosition;

typedef struct {
    char filename[PATH_MAX];
    int fd;
    ServerBinlogFilePosition position;
    ServerBinlogBuffer binlog_buffer;
} ServerBinlogReader;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_reader_init(ServerBinlogReader *reader,
        const ServerBinlogFilePosition *position,
        const int64_t last_data_version);

int binlog_get_first_record_version(const ServerBinlogFilePosition *position,
        int64_t *data_version);

int binlog_get_last_record_version_offset(ServerBinlogFilePosition *position,
        int64_t *data_version);

#define GET_BINLOG_FILENAME(filename, size, binlog_index) \
    snprintf(filename, size, "%s/%s"BINLOG_FILE_EXT_FMT,  \
            DATA_PATH_STR, BINLOG_FILE_PREFIX, binlog_index)

#ifdef __cplusplus
}
#endif

#endif

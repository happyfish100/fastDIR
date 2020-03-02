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
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_producer.h"
#include "binlog_write_thread.h"
#include "binlog_pack.h"
#include "binlog_reader.h"

static int open_readable_binlog(ServerBinlogReader *reader)
{
    if (reader->fd >= 0) {
        close(reader->fd);
    }

    GET_BINLOG_FILENAME(reader->filename, sizeof(reader->filename),
            reader->position.index);

    reader->fd = open(reader->filename, O_RDONLY);
    if (reader->fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, reader->filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    if (reader->position.offset > 0 && lseek(reader->fd,
                reader->position.offset, SEEK_SET) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "lseek file \"%s\" fail,  offset: %"PRId64", "
                "errno: %d, error info: %s", __LINE__,
                reader->filename, reader->position.offset,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EIO;
    }

    return 0;
}

int binlog_reader_init(ServerBinlogReader *reader,
        const ServerBinlogFilePosition *position,
        const int64_t last_data_version)
{
    int result;

    if ((result=binlog_buffer_init(&reader->binlog_buffer)) != 0) {
        return result;
    }

    //TODO
    return open_readable_binlog(reader);
}

#define BINLOG_DETECT_READ_ONCE  2048

int binlog_get_first_record_version(const int file_index,
        int64_t *data_version)
{
    char filename[PATH_MAX];
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[FDIR_ERROR_INFO_SIZE];
    char *p;
    int result;
    int64_t bytes;
    int offset;

    GET_BINLOG_FILENAME(filename, sizeof(filename), file_index);

    *error_info = '\0';
    result = ENOENT;
    p = buff;
    offset = 0;
    bytes = BINLOG_DETECT_READ_ONCE;
    while (bytes > 0 && (result=getFileContentEx(filename,
                    p, offset, &bytes)) == 0)
    {
        offset += bytes;

        *error_info = '\0';
        result = binlog_detect_record(buff, offset,
                data_version, error_info);
        if (result == 0) {
            break;
        } else if (result != EOVERFLOW) {
            break;
        }

        p += bytes;
        bytes = BINLOG_RECORD_MAX_SIZE - offset;
        if (bytes > BINLOG_DETECT_READ_ONCE) {
            bytes = BINLOG_DETECT_READ_ONCE;
        }
    }

    if (result != 0) {
        if (*error_info != '\0') {
            logError("file: "__FILE__", line: %d, "
                    "get_first_record_version fail, "
                    "binlog file: %s, error info: %s",
                    __LINE__, filename, error_info);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "get_first_record_version fail, "
                    "binlog file: %s, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
        }
    }

    return result;
}

int binlog_get_last_record_version(const int file_index,
        int64_t *data_version, int64_t *offset)
{
    char filename[PATH_MAX];
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[FDIR_ERROR_INFO_SIZE];
    int result;
    int buff_off;
    int64_t file_size;
    int64_t bytes;

    GET_BINLOG_FILENAME(filename, sizeof(filename), file_index);

    if ((result=getFileSize(filename, &file_size)) != 0) {
        return result;
    }

    if (file_size == 0) {
        return ENOENT;
    }

    bytes = file_size < sizeof(buff) - 1 ? file_size : sizeof(buff) - 1;
    *offset = file_size - bytes;
    bytes += 1;   //for last \0
    if ((result=getFileContentEx(filename, buff, *offset, &bytes)) != 0) {
        return result;
    }

    *error_info = '\0';
    if ((result=binlog_detect_record_reverse(buff, bytes,
                    data_version, &buff_off, error_info)) != 0)
    {
        if (*error_info != '\0') {
            logError("file: "__FILE__", line: %d, "
                    "get_last_record_version fail, "
                    "binlog file: %s, error info: %s",
                    __LINE__, filename, error_info);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "get_last_record_version fail, "
                    "binlog file: %s, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
        }
    }

    *offset = file_size - bytes + buff_off;
    return result;
}

int binlog_get_max_record_version(int64_t *data_version, int64_t *offset)
{
    int file_index;

    file_index = binlog_get_current_write_index();
    return binlog_get_last_record_version(file_index, data_version, offset);
}

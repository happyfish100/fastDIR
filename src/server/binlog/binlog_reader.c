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
    int result;

    if (reader->fd >= 0) {
        close(reader->fd);
    }

    GET_BINLOG_FILENAME(reader->filename, sizeof(reader->filename),
            reader->position.index);
    reader->fd = open(reader->filename, O_RDONLY);
    if (reader->fd < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, reader->filename,
                result, STRERROR(result));
        return result;
    }

    if (reader->position.offset > 0) {
        int64_t file_size;
        if ((file_size=lseek(reader->fd, 0L, SEEK_END)) < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "lseek file \"%s\" fail, "
                    "errno: %d, error info: %s", __LINE__,
                    reader->filename, result, STRERROR(result));
            return result;
        }

        if (reader->position.offset > file_size) {
            logWarning("file: "__FILE__", line: %d, "
                    "offset %"PRId64" > file size: %"PRId64,
                    __LINE__, reader->position.offset, file_size);
            reader->position.offset = file_size;
        }

        if (lseek(reader->fd, reader->position.offset, SEEK_SET) < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "lseek file \"%s\" fail,  offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__,
                    reader->filename, reader->position.offset,
                    result, STRERROR(result));
            return result;
        }
    }

    reader->binlog_buffer.current = reader->binlog_buffer.end =
        reader->binlog_buffer.buff;
    return 0;
}

static int do_read_to_buffer(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;

    *read_bytes = read(reader->fd, buff, size);
    if (*read_bytes == 0) {
        return ENOENT;
    }
    if (*read_bytes < 0) {
        *read_bytes = 0;
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "read binlog file: %s, errno: %d, error info: %s",
                __LINE__, reader->filename, result, STRERROR(result));
        return result;
    }

    reader->position.offset += *read_bytes;
    return 0;
}

static int do_binlog_read(ServerBinlogReader *reader)
{
    int remain;
    int result;
    int read_bytes;

    if (reader->binlog_buffer.current != reader->binlog_buffer.buff) {
        remain = BINLOG_BUFFER_REMAIN(reader->binlog_buffer);
        if (remain > 0) {
            memmove(reader->binlog_buffer.buff, reader->binlog_buffer.current,
                    remain);
        }

        reader->binlog_buffer.current = reader->binlog_buffer.buff;
        reader->binlog_buffer.end = reader->binlog_buffer.buff + remain;
    }

    read_bytes = reader->binlog_buffer.size -
        BINLOG_BUFFER_LENGTH(reader->binlog_buffer);
    if (read_bytes == 0) {
        return ENOSPC;
    }
    if ((result=do_read_to_buffer(reader, reader->binlog_buffer.end,
                    read_bytes, &read_bytes)) != 0)
    {
        return result;
    }

    reader->binlog_buffer.end += read_bytes;
    return 0;
}

int binlog_reader_read(ServerBinlogReader *reader)
{
    int result;

    result = do_binlog_read(reader);
    if (result == 0 || result != ENOENT) {
        return result;
    }

    if (reader->position.index < binlog_get_current_write_index()) {
        reader->position.offset = 0;
        reader->position.index++;
        if ((result=open_readable_binlog(reader)) != 0) {
            return result;
        }
        result = do_binlog_read(reader);
    }

    return result;
}

int binlog_read_to_buffer(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;

    result = do_read_to_buffer(reader, buff, size, read_bytes);
    if (result == 0 || result != ENOENT) {
        return result;
    }

    if (reader->position.index < binlog_get_current_write_index()) {
        reader->position.offset = 0;
        reader->position.index++;
        if ((result=open_readable_binlog(reader)) != 0) {
            return result;
        }

        result = do_read_to_buffer(reader, buff, size, read_bytes);
    }

    return result;
}

int binlog_reader_integral_read(ServerBinlogReader *reader, char *buff,
        const int size, int *read_bytes, int64_t *data_version)
{
    int result;
    int remain_len;
    char *rec_end;
    char error_info[FDIR_ERROR_INFO_SIZE];

    if ((result=binlog_read_to_buffer(reader, buff, size,
                    read_bytes)) != 0)
    {
        *data_version = 0;
        return result;
    }

    if ((result=binlog_detect_record_reverse(buff, *read_bytes,
                    data_version, (const char **)&rec_end,
                    error_info, sizeof(error_info))) != 0)
    {
        int64_t line_count;

        fc_get_file_line_count_ex(reader->filename, reader->position.
                offset + *read_bytes, &line_count);
        if (*error_info == '\0') {
            snprintf(error_info, sizeof(error_info),
                    "%s", STRERROR(result));
        }
        logError("file: "__FILE__", line: %d, "
                "binlog_detect_record_reverse fail, "
                "binlog file: %s, line no: %"PRId64", error info: %s",
                __LINE__, reader->filename, line_count, error_info);

        *data_version = 0;
        return result == ENOENT ? EFAULT : result;
    }

    remain_len = (buff + *read_bytes) - rec_end;
    if (remain_len > 0) {
        *read_bytes -= remain_len;
        reader->position.offset -= remain_len;
        if (lseek(reader->fd, reader->position.offset, SEEK_SET) < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "lseek file \"%s\" fail,  offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__,
                    reader->filename, reader->position.offset,
                    result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

int binlog_reader_next_record(ServerBinlogReader *reader,
        FDIRBinlogRecord *record)
{
    int result;
    int len;
    char *rec_end;
    char error_info[FDIR_ERROR_INFO_SIZE];

    len = BINLOG_BUFFER_REMAIN(reader->binlog_buffer);
    if (len < BINLOG_RECORD_MIN_SIZE &&
            (result=binlog_reader_read(reader)) != 0)
    {
        return result;
    }

    result = binlog_unpack_record(reader->binlog_buffer.current, len,
            record, (const char **)&rec_end, error_info, sizeof(error_info));
    if (result == EAGAIN || result == EOVERFLOW) {
        if ((result=binlog_reader_read(reader)) != 0) {
            return result;
        }

        len = BINLOG_BUFFER_REMAIN(reader->binlog_buffer);
        result = binlog_unpack_record(reader->binlog_buffer.current, len,
                record, (const char **)&rec_end,
                error_info, sizeof(error_info));
    }

    if (result != 0) {
        if (*error_info != '\0') {
            logError("file: "__FILE__", line: %d, "
                    "binlog_unpack_record fail, "
                    "binlog file: %s, error info: %s",
                    __LINE__, reader->filename, error_info);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "binlog_unpack_record fail, "
                    "binlog file: %s, errno: %d, error info: %s",
                    __LINE__, reader->filename, result, STRERROR(result));
        }

        return result;
    }

    reader->binlog_buffer.current = rec_end;
    return result;
}

static int find_data_version(ServerBinlogReader *reader,
        const int64_t last_data_version)
{
    int result;
    bool found;
    int64_t data_version;
    char *rec_end;
    char error_info[FDIR_ERROR_INFO_SIZE];

    reader->position.offset = 0;
    if ((result=open_readable_binlog(reader)) != 0) {
        return result;
    }

    found = false;
    while ((result=do_binlog_read(reader)) == 0) {
        /*
        logInfo("binlog index: %d, binlog size: %d, offset: %"PRId64", "
                "buffer length: %d, last_data_version: %"PRId64,
                reader->position.index, reader->binlog_buffer.size,
                reader->position.offset, (int)BINLOG_BUFFER_LENGTH(
                    reader->binlog_buffer), last_data_version);
                    */

        while ((result=binlog_detect_record(reader->binlog_buffer.current,
                        BINLOG_BUFFER_REMAIN(reader->binlog_buffer),
                        &data_version, (const char **)&rec_end,
                        error_info, sizeof(error_info))) == 0)
        {
            /*
            logInfo("data_version==%"PRId64", record end offset: %d",
                    data_version, (int)(rec_end - reader->binlog_buffer.buff));
                    */

            if (last_data_version == data_version) {
                reader->position.offset -= reader->binlog_buffer.end - rec_end;
                found = true;
                break;
            } else if (last_data_version < data_version) {
                logWarning("file: "__FILE__", line: %d, "
                        "can't found data version %"PRId64", "
                        "skip to next data version %"PRId64,
                        __LINE__, last_data_version, data_version);
                reader->position.offset -= BINLOG_BUFFER_REMAIN(
                        reader->binlog_buffer);
                found = true;
                break;
            }

            reader->binlog_buffer.current = rec_end;
        }

        if (result == 0) {
            if (found) {
                break;
            }
            continue;
        }

        if (result == EAGAIN || result == EOVERFLOW) {
            continue;
        }
        logInfo("binlog_detect_record result: %d", result);

        if (*error_info != '\0') {
            logError("file: "__FILE__", line: %d, "
                    "binlog_detect_record fail, "
                    "binlog file: %s, error info: %s",
                    __LINE__, reader->filename, error_info);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "binlog_detect_record fail, "
                    "binlog file: %s, errno: %d, error info: %s",
                    __LINE__, reader->filename, result, STRERROR(result));
        }

        return result;
    }

    if (result != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "found position, index: %d, offset: %"PRId64, __LINE__,
            reader->position.index, reader->position.offset);
    return open_readable_binlog(reader);
}

static int binlog_reader_search_data_version(ServerBinlogReader *reader,
        const int64_t last_data_version)
{
    int64_t min_data_version;
    int64_t max_data_version;
    int dirction;
    int binlog_write_index;
    int result;

    binlog_write_index = binlog_get_current_write_index();
    dirction = 0;

    do {
        if ((result=binlog_get_first_record_version(reader->position.index,
                        &min_data_version)) != 0)
        {
            return result;
        }

        if ((result=binlog_get_last_record_version(reader->position.index,
                        &max_data_version)) != 0)
        {
            return result;
        }

        logInfo("binlog index: %d, min_data_version: %"PRId64", "
                "max_data_version: %"PRId64, reader->position.index,
                min_data_version,  max_data_version);

        if (last_data_version < min_data_version) {
            if (dirction == 0) {
                dirction = -1;
            } else if (dirction > 0) {  //disordered
                return EBUSY;
            }

            if (reader->position.index > 0) {
                reader->position.index--;
            } else {
                return EFAULT;
            }
        } else if (last_data_version > max_data_version) {
            if (dirction == 0) {
                dirction = 1;
            } else if (dirction < 0) {  //disordered
                return EBUSY;
            }

            if (reader->position.index < binlog_write_index) {
                reader->position.index++;
            } else {
                return EFAULT;
            }
        } else {
            return find_data_version(reader, last_data_version);
        }
    } while (1);

    return ENOENT;
}

static int binlog_reader_detect_open(ServerBinlogReader *reader,
        const int64_t last_data_version)
{
    int result;
    int bytes;
    int remain;
    int rstart_offset;
    int rend_offset;
    int64_t data_version;
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[FDIR_ERROR_INFO_SIZE];
    char *p;

    if ((result=open_readable_binlog(reader)) != 0) {
        if (result == ENOENT) {
            if (reader->position.index > 0) {
                reader->position.index -= 1;
                reader->position.offset = 0;
                result = open_readable_binlog(reader);
            }
            if (result != 0) {
                return result;
            }
        } else {
            return result;
        }
    }

    if ((bytes=read(reader->fd, buff, sizeof(buff))) < 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "get_first_record_version fail, "
                "binlog file: %s, errno: %d, error info: %s",
                __LINE__, reader->filename, result, STRERROR(result));
        return result;
    }

    p = buff;
    remain = bytes;
    while (remain >= BINLOG_RECORD_MIN_SIZE) {
        result = binlog_detect_record_forward(p, remain, &data_version,
                &rstart_offset, &rend_offset, error_info, sizeof(error_info));
        if (result == 0) {
            if (last_data_version == data_version) {
                reader->position.offset += (p - buff) + rend_offset;

                logInfo("file: "__FILE__", line: %d, "
                        "found position, index: %d, offset: %"PRId64, __LINE__,
                        reader->position.index, reader->position.offset);
                return open_readable_binlog(reader);
            }

            p += rend_offset;
            remain -= rend_offset;

            /*
            logInfo("file: "__FILE__", line: %d, "
                    "====remain length: %d", __LINE__, remain);
                    */
        } else {
            break;
        }
    }

    logInfo("binlog index: %d, reader->position.offset: %"PRId64", "
                "last_data_version: %"PRId64, reader->position.index,
                reader->position.offset,  last_data_version);
    return binlog_reader_search_data_version(reader, last_data_version);
}

int binlog_reader_init(ServerBinlogReader *reader,
        const FDIRBinlogFilePosition *hint_pos,
        const int64_t last_data_version)
{
    int result;

    if ((result=binlog_buffer_init(&reader->binlog_buffer)) != 0) {
        return result;
    }

    reader->fd = -1;
    if (last_data_version == 0) {
        reader->position.index = 0;
        reader->position.offset = 0;
        return open_readable_binlog(reader);
    }

    reader->position = *hint_pos;
    if (reader->position.offset > BINLOG_RECORD_MAX_SIZE / 4) {
        reader->position.offset -= BINLOG_RECORD_MAX_SIZE / 4;
    } else if (reader->position.offset > BINLOG_RECORD_MAX_SIZE / 8) {
        reader->position.offset -= BINLOG_RECORD_MAX_SIZE / 8;
    }
    return binlog_reader_detect_open(reader, last_data_version);
}

void binlog_reader_destroy(ServerBinlogReader *reader)
{
    if (reader->fd >= 0) {
        close(reader->fd);
        reader->fd = -1;
    }

    binlog_buffer_destroy(&reader->binlog_buffer);
}

int binlog_get_first_record_version(const int file_index,
        int64_t *data_version)
{
#define BINLOG_DETECT_READ_ONCE  2048

    char filename[PATH_MAX];
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[FDIR_ERROR_INFO_SIZE];
    char *p;
    char *rec_end;
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
                data_version, (const char **)&rec_end,
                error_info, sizeof(error_info));
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
        int64_t *data_version)
{
    char filename[PATH_MAX];
    char buff[BINLOG_RECORD_MAX_SIZE + 1];
    char error_info[FDIR_ERROR_INFO_SIZE];
    int result;
    int offset;
    int64_t file_size = 0;
    int64_t bytes;

    GET_BINLOG_FILENAME(filename, sizeof(filename), file_index);
    if (access(filename, F_OK) == 0) {
        result = getFileSize(filename, &file_size);
    } else {
        result = errno != 0 ? errno : EPERM;
    }
    if ((result == 0 && file_size == 0) || (result == ENOENT)) {
        return ENOENT;
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "access file: %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(errno));
        return result;
    }

    bytes = file_size < sizeof(buff) - 1 ? file_size : sizeof(buff) - 1;
    offset = file_size - bytes;
    bytes += 1;   //for last \0
    if ((result=getFileContentEx(filename, buff, offset, &bytes)) != 0) {
        return result;
    }

    *error_info = '\0';
    if ((result=binlog_detect_record_reverse(buff, bytes,
                    data_version, NULL, error_info,
                    sizeof(error_info))) != 0)
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

    return result;
}

int binlog_get_max_record_version(int64_t *data_version)
{
    int file_index;
    int result;

    file_index = binlog_get_current_write_index();
    if ((result=binlog_get_last_record_version(file_index,
                    data_version)) == ENOENT)
    {
        if (file_index == 0) {
            *data_version = 0;
            return 0;
        }

        result = binlog_get_last_record_version(file_index - 1,
                data_version);
    }

    return result;
}

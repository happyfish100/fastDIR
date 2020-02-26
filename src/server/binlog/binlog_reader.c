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

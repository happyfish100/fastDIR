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

#define BINLOG_FILE_MAX_SIZE   (1024 * 1024 * 1024)
#define BINLOG_FILE_PREFIX     "binlog"
#define BINLOG_INDEX_FILENAME  BINLOG_FILE_PREFIX"_index.dat"
#define BINLOG_FILE_EXT_FMT    ".%05d"

#define BINLOG_INDEX_ITEM_CURRENT_WRITE     "current_write"
#define BINLOG_INDEX_ITEM_CURRENT_COMPRESS  "current_compress"

typedef struct {
    char filename[PATH_MAX];
    int binlog_index;
    int binlog_compress_index;
    int file_size;
    int fd;
    ServerBinlogBuffer binlog_buffer;
} BinlogWriterContext;

static BinlogWriterContext writer_context = {{'\0'}, 0, 0, 0, -1};
static struct common_blocked_queue *writer_queue = NULL;
static volatile bool write_thread_running = false;

static int write_to_binlog_index_file()
{
    char full_filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", DATA_PATH_STR, BINLOG_INDEX_FILENAME);

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE,
            writer_context.binlog_index,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS,
            writer_context.binlog_compress_index);
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "write to file \"%s\" fail, "
            "errno: %d, error info: %s",
            __LINE__, full_filename,
            result, STRERROR(result));
    }

    return result;
}

static int get_binlog_index_from_file()
{
    char full_filename[PATH_MAX];
    IniContext iniContext;
    int result;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", DATA_PATH_STR, BINLOG_INDEX_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return write_to_binlog_index_file();
        }
    }

    if ((result=iniLoadFromFile(full_filename, &iniContext)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    writer_context.binlog_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_WRITE, &iniContext, 0);
    writer_context.binlog_compress_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS, &iniContext, 0);

    iniFreeContext(&iniContext);
    return 0;
}

static int open_writable_binlog()
{
    if (writer_context.fd >= 0) {
        close(writer_context.fd);
    }

    snprintf(writer_context.filename, sizeof(writer_context.filename),
            "%s/%s"BINLOG_FILE_EXT_FMT, DATA_PATH_STR,
            BINLOG_FILE_PREFIX, writer_context.binlog_index);
    writer_context.fd = open(writer_context.filename,
            O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (writer_context.fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    writer_context.file_size = lseek(writer_context.fd, 0, SEEK_END);
    if (writer_context.file_size < 0) {
        logError("file: "__FILE__", line: %d, "
                "lseek file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EIO;
    }

    return 0;
}

static int binlog_write_to_file()
{
    int result;

    if (writer_context.binlog_buffer.length == 0) {
        return 0;
    }

    if (fc_safe_write(writer_context.fd, writer_context.binlog_buffer.buffer,
                writer_context.binlog_buffer.length) !=
            writer_context.binlog_buffer.length)
    {
        logError("file: "__FILE__", line: %d, "
                "write to binlog file \"%s\" fail, fd: %d, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                writer_context.fd, errno, STRERROR(errno));
        result = errno != 0 ? errno : EIO;
    } else if (fsync(writer_context.fd) != 0) {
        logError("file: "__FILE__", line: %d, "
                "fsync to binlog file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                errno, STRERROR(errno));
        result = errno != 0 ? errno : EIO;
    } else {
        writer_context.file_size += writer_context.binlog_buffer.length;
        if (writer_context.file_size >= BINLOG_FILE_MAX_SIZE) {
            writer_context.binlog_index++;
            if ((result=write_to_binlog_index_file()) == 0) {
                result = open_writable_binlog();
            }

            writer_context.file_size = 0;
            if (result != 0) {
                logError("file: "__FILE__", line: %d, "
                        "open binlog file \"%s\" fail",
                        __LINE__, writer_context.filename);
            }
        } else {
            result = 0;
        }
    }

    writer_context.binlog_buffer.length = 0;  //reset cache buff
    return result;
}

int binlog_write_thread_init()
{
    int result;

    if ((result=binlog_buffer_init(&writer_context.binlog_buffer)) != 0) {
        return result;
    }

    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    return open_writable_binlog();
}

static inline int deal_binlog_one_record(ServerBinlogRecordBuffer *rb)
{
    int result;
    if (writer_context.binlog_buffer.size - writer_context.binlog_buffer.length
            < rb->record.length)
    {
        if ((result=binlog_write_to_file()) != 0) {
            return result;
        }
    }

    memcpy(writer_context.binlog_buffer.buffer +
            writer_context.binlog_buffer.length,
            rb->record.data, rb->record.length);
    writer_context.binlog_buffer.length += rb->record.length;
    return 0;
}

static int deal_binlog_records(struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rb;
    int result;

    do {
        rb = (ServerBinlogRecordBuffer *)node->data;
        if ((result=deal_binlog_one_record(rb)) != 0) {
            return result;
        }

        server_binlog_release_rbuffer(rb);
        node = node->next;
    } while (node != NULL);

    return binlog_write_to_file();
}

void binlog_write_thread_finish()
{
    struct common_blocked_node *node;
    int count;

    if (writer_queue != NULL) {
        count = 0;
        while (write_thread_running && ++count < 100) {
            usleep(100 * 1000);
        }
        
        if (write_thread_running) {
            logWarning("file: "__FILE__", line: %d, "
                    "binlog write thread still running, "
                    "exit anyway!", __LINE__);
        }

        node = common_blocked_queue_try_pop_all_nodes(writer_queue);
        if (node != NULL) {
            deal_binlog_records(node);
            common_blocked_queue_free_all_nodes(writer_queue, node);
        }
        writer_queue = NULL;
    }

    if (writer_context.fd >= 0) {
        close(writer_context.fd);
        writer_context.fd = -1;
    }
}

void *binlog_write_thread_func(void *arg)
{
    struct common_blocked_node *node;

    write_thread_running = true;
    writer_queue = &((ServerBinlogConsumerContext *)arg)->queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(writer_queue);
        if (node == NULL) {
            continue;
        }

        deal_binlog_records(node);
        common_blocked_queue_free_all_nodes(writer_queue, node);
    }

    write_thread_running = false;
    return NULL;
}

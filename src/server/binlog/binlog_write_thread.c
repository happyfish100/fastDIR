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
} BinlogWriterContext;

static BinlogWriterContext writer_context = {{'\0'}, 0, 0, 0, -1};

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

static int storage_binlog_fsync()
{
    /*
    int result;
    int write_ret;

    if (binlog_write_cache_len == 0) //ignore
    {
        write_ret = 0;  //skip
    }
    else if (fc_safe_write(writer_context.fd, binlog_write_cache_buff, \
        binlog_write_cache_len) != binlog_write_cache_len)
    {
        logError("file: "__FILE__", line: %d, " \
            "write to binlog file \"%s\" fail, fd=%d, " \
            "errno: %d, error info: %s",  \
            __LINE__, get_writable_binlog_filename(NULL), \
            writer_context.fd, errno, STRERROR(errno));
        write_ret = errno != 0 ? errno : EIO;
    }
    else if (fsync(writer_context.fd) != 0)
    {
        logError("file: "__FILE__", line: %d, " \
            "sync to binlog file \"%s\" fail, " \
            "errno: %d, error info: %s",  \
            __LINE__, get_writable_binlog_filename(NULL), \
            errno, STRERROR(errno));
        write_ret = errno != 0 ? errno : EIO;
    }
    else
    {
        writer_context.file_size += binlog_write_cache_len;
        if (writer_context.file_size >= BINLOG_FILE_MAX_SIZE)
        {
            writer_context.binlog_index++;
            if ((write_ret=write_to_binlog_index_file()) == 0)
            {
                write_ret = open_writable_binlog();
            }

            writer_context.file_size = 0;
            if (write_ret != 0)
            {
                g_continue_flag = false;
                logCrit("file: "__FILE__", line: %d, " \
                    "open binlog file \"%s\" fail, " \
                    "program exit!", \
                    __LINE__, \
                    get_writable_binlog_filename(NULL));
            }
        }
        else
        {
            write_ret = 0;
        }
    }

    binlog_write_version++;
    binlog_write_cache_len = 0;  //reset cache buff

    return write_ret;
    */

    return 0;
}

int binlog_write_thread_init()
{
    int result;

    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    return open_writable_binlog();
}

static void binlog_write_thread_done()
{
    if (writer_context.fd >= 0) {
        close(writer_context.fd);
        writer_context.fd = -1;
    }
}

void deal_binlog_records(struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rb;

    rb = (ServerBinlogRecordBuffer *)node->data;

    //TODO
    //rb->record;
    server_binlog_release_rbuffer(rb);
}

void *binlog_write_thread_func(void *arg)
{
    struct common_blocked_queue *queue;
    struct common_blocked_node *node;

    queue = &((ServerBinlogConsumerContext *)arg)->queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(queue);
        if (node == NULL) {
            continue;
        }

        deal_binlog_records(node);
        common_blocked_queue_free_all_nodes(queue, node);
    }

    binlog_write_thread_done();
    return NULL;
}

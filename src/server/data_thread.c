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
#include "server_global.h"
#include "data_thread.h"

DataThreadArray g_data_thread_array;
static void *data_thread_func(void *arg);

static int init_data_thread_array()
{
    int result;
    int bytes;
    DataThreadContext *context;
    DataThreadContext *end;

    bytes = sizeof(DataThreadContext) * DATA_THREAD_COUNT;
    g_data_thread_array.contexts = (DataThreadContext *)malloc(bytes);
    if (g_data_thread_array.contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    end = g_data_thread_array.contexts + DATA_THREAD_COUNT;
    for (context=g_data_thread_array.contexts; context<end; context++) {
        if ((result=common_blocked_queue_init_ex(&context->queue, 4096)) != 0) {
            return result;
        }
    }
    g_data_thread_array.count = DATA_THREAD_COUNT;
    return 0;
}

static int data_thread_start()
{
    int result;
    int i;
    pthread_t tid;
    pthread_attr_t thread_attr;

    if ((result=init_pthread_attr(&thread_attr, SF_G_THREAD_STACK_SIZE)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "init_pthread_attr fail, program exit!", __LINE__);
        return result;
    }

    for (i=0; i<g_data_thread_array.count; i++) {
        if ((result=pthread_create(&tid, &thread_attr, data_thread_func,
                        g_data_thread_array.contexts + i)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "create thread failed, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    pthread_attr_destroy(&thread_attr);
    return 0;
}

int data_thread_init()
{
    int result;

    if ((result=init_data_thread_array()) != 0) {
        return result;
    }

    return data_thread_start();
}

void data_thread_destroy()
{
    if (g_data_thread_array.contexts != NULL) {
        DataThreadContext *context;
        DataThreadContext *end;

        end = g_data_thread_array.contexts + g_data_thread_array.count;
        for (context=g_data_thread_array.contexts; context<end; context++) {
            common_blocked_queue_destroy(&context->queue);
        }
        free(g_data_thread_array.contexts);
        g_data_thread_array.contexts = NULL;
    }
}

void data_thread_terminate()
{
    DataThreadContext *context;
    DataThreadContext *end;

    end = g_data_thread_array.contexts + g_data_thread_array.count;
    for (context=g_data_thread_array.contexts; context<end; context++) {
        common_blocked_queue_terminate(&context->queue);
    }
}

static inline int deal_binlog_one_record(DataThreadContext *data_context,
        FDIRBinlogRecord *record)
{
    return 0;
}

static int deal_binlog_records(DataThreadContext *data_context,
        struct common_blocked_node *node)
{
    FDIRBinlogRecord *record;
    int result;

    do {
        record = (FDIRBinlogRecord *)node->data;
        if ((result=deal_binlog_one_record(data_context, record)) != 0) {
            return result;
        }

        node = node->next;
    } while (node != NULL);

    return 0;
}

static void *data_thread_func(void *arg)
{
    struct common_blocked_queue *queue;
    struct common_blocked_node *node;
    DataThreadContext *data_context;

    data_context = (DataThreadContext *)arg;
    queue = &data_context->queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(queue);
        if (node == NULL) {
            continue;
        }

        deal_binlog_records(data_context, node);
        common_blocked_queue_free_all_nodes(queue, node);
    }

    return NULL;
}

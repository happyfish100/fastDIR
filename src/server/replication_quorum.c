/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "server_global.h"
#include "replication_quorum.h"

#define VERSION_CONFIRMED_FILE_COUNT  2  //double files for safty

typedef struct fdir_replication_quorum_context {
    struct fast_mblock_man quorum_entry_allocator; //element: FDIRReplicationQuorumEntry
    pthread_mutex_t lock;
    volatile int dealing;
    struct {
        FDIRReplicationQuorumEntry *head;
        FDIRReplicationQuorumEntry *tail;
    } list;

    struct {
        volatile int64_t counter;
    } confirmed;
} FDIRReplicationQuorumContext;

static FDIRReplicationQuorumContext fdir_replication_quorum;

#define QUORUM_LIST_HEAD  fdir_replication_quorum.list.head
#define QUORUM_LIST_TAIL  fdir_replication_quorum.list.tail
#define QUORUM_ENTRY_ALLOCATOR fdir_replication_quorum.quorum_entry_allocator
#define CONFIRMED_COUNTER fdir_replication_quorum.confirmed.counter

static inline const char *get_confirmed_filename(
        const int index, char *filename, const int size)
{
    snprintf(filename, size, "%s/version-confirmed.%d", DATA_PATH_STR, index);
    return filename;
}

static int get_confirmed_version_from_file(const int index,
        int64_t *confirmed_version)
{
    char filename[PATH_MAX];
    char buff[32];
    char *endptr;
    int64_t file_size;
    struct {
        int value;
        int calc;
    } crc32;
    int result;

    get_confirmed_filename(index, filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            *confirmed_version = 0;
            return 0;
        }
        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    file_size = sizeof(buff) - 1;
    if ((result=getFileContentEx(filename, buff, 0, &file_size)) != 0) {
        return result;
    }

    *confirmed_version = strtoll(buff, &endptr, 10);
    if (*endptr != ' ') {
        return EINVAL;
    }

    crc32.calc = CRC32(buff, endptr - buff);
    crc32.value = strtol(endptr + 1, NULL, 16);
    return (crc32.value == crc32.calc ? 0 : EINVAL);
}

static int load_confirmed_version(int64_t *confirmed_version)
{
    int result;
    int invalid_count;
    int i;
    int64_t current_version;

    invalid_count = 0;
    *confirmed_version = 0;
    for (i=0; i<VERSION_CONFIRMED_FILE_COUNT; i++) {
        result = get_confirmed_version_from_file(i, &current_version);
        if (result == 0) {
            if (current_version > *confirmed_version) {
                *confirmed_version = current_version;
            }
        } else if (result == EINVAL) {
            ++invalid_count;
        } else {
            return result;
        }
    }

    if (invalid_count == 0) {
        return 0;
    } else {
        if (*confirmed_version > 0) {  //one confirmed file is OK
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "all data version confirmed files "
                    "are invalid!", __LINE__);
            return EINVAL;
        }
    }
}

int replication_quorum_init()
{
    int result;

    if ((result=fast_mblock_init_ex1(&QUORUM_ENTRY_ALLOCATOR,
                    "repl_quorum_entry", sizeof(FDIRReplicationQuorumEntry),
                    4096, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock(&fdir_replication_quorum.lock)) != 0) {
        return result;
    }

    if ((result=load_confirmed_version((int64_t *)
                    &MY_CONFIRMED_VERSION)) != 0)
    {
        return result;
    }

    fdir_replication_quorum.dealing = 0;
    CONFIRMED_COUNTER = 0;
    QUORUM_LIST_HEAD = QUORUM_LIST_TAIL = NULL;
    return 0;
}

void replication_quorum_destroy()
{
}

int replication_quorum_add(struct fast_task_info *task,
        const int64_t data_version)
{
    FDIRReplicationQuorumEntry *previous;
    FDIRReplicationQuorumEntry *entry;

    if ((entry=fast_mblock_alloc_object(&QUORUM_ENTRY_ALLOCATOR)) == NULL) {
        return ENOMEM;
    }
    entry->task = task;
    entry->data_version = data_version;

    PTHREAD_MUTEX_LOCK(&fdir_replication_quorum.lock);
    if (QUORUM_LIST_HEAD == NULL) {
        entry->next = NULL;
        QUORUM_LIST_HEAD = entry;
        QUORUM_LIST_TAIL = entry;
    } else if (data_version >= QUORUM_LIST_TAIL->data_version) {
        entry->next = NULL;
        QUORUM_LIST_TAIL->next = entry;
        QUORUM_LIST_TAIL = entry;
    } else if (data_version <= QUORUM_LIST_HEAD->data_version) {
        entry->next = QUORUM_LIST_HEAD;
        QUORUM_LIST_HEAD = entry;
    } else {
        previous = QUORUM_LIST_HEAD;
        while (data_version > previous->next->data_version) {
            previous = previous->next;
        }
        entry->next = previous->next;
        previous->next = entry;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);
    return 0;
}

static int compare_int64(const int64_t *n1, const int64_t *n2)
{
    return fc_compare_int64(*n1, *n2);
}

static int write_to_confirmed_file(const int index,
        const int64_t confirmed_version)
{
    const int flags = O_WRONLY | O_CREAT | O_TRUNC;
    FilenameFDPair pair;
    char buff[32];
    int crc32;
    int len;
    int result;

    len = sprintf(buff, "%"PRId64, confirmed_version);
    crc32 = CRC32(buff, len);
    len += sprintf(buff + len, " %08x", crc32);

    get_confirmed_filename(index, pair.filename, sizeof(pair.filename));
    if ((pair.fd=open(pair.filename, flags, 0644)) < 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, pair.filename, result, STRERROR(result));
        return result;
    }

    if (fc_safe_write(pair.fd, buff, len) != len) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write file %s fail, errno: %d, error info: %s",
                __LINE__, pair.filename, result, STRERROR(result));
        close(pair.fd);
        return result;
    }

    close(pair.fd);
    return 0;
}

static void notify_waiting_tasks(const int64_t my_confirmed_version)
{
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;

    chain.head = chain.tail = NULL;
    PTHREAD_MUTEX_LOCK(&fdir_replication_quorum.lock);
    if (QUORUM_LIST_HEAD != NULL && my_confirmed_version >=
            QUORUM_LIST_HEAD->data_version)
    {
        do {
            node = fast_mblock_to_node_ptr(QUORUM_LIST_HEAD);
            if (chain.head == NULL) {
                chain.head = node;
            } else {
                chain.tail->next = node;
            }
            chain.tail = node;

            sf_nio_notify(QUORUM_LIST_HEAD->task, SF_NIO_STAGE_CONTINUE);
            QUORUM_LIST_HEAD = QUORUM_LIST_HEAD->next;
        } while (QUORUM_LIST_HEAD != NULL && my_confirmed_version >=
                QUORUM_LIST_HEAD->data_version);

        if (QUORUM_LIST_HEAD == NULL) {
            QUORUM_LIST_TAIL = NULL;
        }
        chain.tail->next = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);

    if (chain.head != NULL) {
        fast_mblock_batch_free(&QUORUM_ENTRY_ALLOCATOR, &chain);
    }
}

static void clear_waiting_tasks()
{
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;

    chain.head = chain.tail = NULL;
    PTHREAD_MUTEX_LOCK(&fdir_replication_quorum.lock);
    if (QUORUM_LIST_HEAD != NULL) {
        do {
            node = fast_mblock_to_node_ptr(QUORUM_LIST_HEAD);
            if (chain.head == NULL) {
                chain.head = node;
            } else {
                chain.tail->next = node;
            }
            chain.tail = node;

            ((FDIRServerTaskArg *)QUORUM_LIST_HEAD->task->arg)->context.
                common.response.header.status = EAGAIN;
            sf_nio_notify(QUORUM_LIST_HEAD->task, SF_NIO_STAGE_CONTINUE);
            QUORUM_LIST_HEAD = QUORUM_LIST_HEAD->next;
        } while (QUORUM_LIST_HEAD != NULL);

        QUORUM_LIST_TAIL = NULL;
        chain.tail->next = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);

    if (chain.head != NULL) {
        fast_mblock_batch_free(&QUORUM_ENTRY_ALLOCATOR, &chain);
    }
}

void replication_quorum_deal_version_change()
{
#define FIXED_SERVER_COUNT  8
    FDIRClusterServerInfo *server;
    FDIRClusterServerInfo *end;
    int64_t my_current_version;
    int64_t my_confirmed_version;
    int64_t confirmed_version;
    int64_t fixed_data_versions[FIXED_SERVER_COUNT];
    int64_t *data_versions;
    int half_server_count;
    int count;
    int index;

    if (!__sync_bool_compare_and_swap(
                &fdir_replication_quorum.
                dealing, 0, 1))
    {
        return;
    }

    if (CLUSTER_SERVER_ARRAY.count <= FIXED_SERVER_COUNT) {
        data_versions = fixed_data_versions;
    } else {
        data_versions = fc_malloc(sizeof(int64_t) *
                CLUSTER_SERVER_ARRAY.count);
    }

    count = 0;
    my_current_version = FC_ATOMIC_GET(DATA_CURRENT_VERSION);
    my_confirmed_version = FC_ATOMIC_GET(MY_CONFIRMED_VERSION);
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (server=CLUSTER_SERVER_ARRAY.servers; server<end; server++) {
        if (server == CLUSTER_MYSELF_PTR) {
            continue;
        }

        confirmed_version=FC_ATOMIC_GET(server->confirmed_data_version);
        if (confirmed_version > my_confirmed_version &&
                confirmed_version <= my_current_version)
        {
            data_versions[count++] = confirmed_version;
        }
    }

    half_server_count = (CLUSTER_SERVER_ARRAY.count + 1) / 2;
    if (count + 1 >= half_server_count) {  //quorum majority
        if (CLUSTER_SERVER_ARRAY.count == 3) {  //fast path
            if (count == 2) {
                my_confirmed_version = FC_MAX(data_versions[0],
                        data_versions[1]);
            } else {  //count == 1
                my_confirmed_version = data_versions[0];
            }
        } else {
            qsort(data_versions, count, sizeof(int64_t),
                    (int (*)(const void *, const void *))
                    compare_int64);
            index = (count + 1) - half_server_count;
            my_confirmed_version = data_versions[index];
        }

        if (write_to_confirmed_file(FC_ATOMIC_INC(CONFIRMED_COUNTER) %
                    VERSION_CONFIRMED_FILE_COUNT, my_confirmed_version) != 0)
        {
            sf_terminate_myself();
            return;
        }

        FC_ATOMIC_SET(MY_CONFIRMED_VERSION, my_confirmed_version);
        notify_waiting_tasks(my_confirmed_version);
    }

    if (data_versions != fixed_data_versions) {
        free(data_versions);
    }

    __sync_bool_compare_and_swap(
            &fdir_replication_quorum.
            dealing, 1, 0);
}

void replication_quorum_deal_master_change()
{
    int64_t my_confirmed_version;

    clear_waiting_tasks();
    my_confirmed_version = FC_ATOMIC_GET(MY_CONFIRMED_VERSION);
    if (my_confirmed_version >= FC_ATOMIC_GET(DATA_CURRENT_VERSION)) {
        return;
    }
    /*
    if (rollback_data_and_binlog(my_confirmed_version) != 0) {
        sf_terminate_myself();
    }
    */
}

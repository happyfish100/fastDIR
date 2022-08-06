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
#include "binlog/binlog_write.h"
#include "binlog/binlog_reader.h"
#include "server_global.h"
#include "replication_quorum.h"

#define VERSION_CONFIRMED_FILE_COUNT     2  //double files for safty
#define REPLICATION_QUORUM_DAT_FILENAME  ".repl_quorum.dat"

typedef struct fdir_replication_quorum_entry {
    int64_t data_version;
    struct fast_task_info *task;
    struct fdir_replication_quorum_entry *next;
} FDIRReplicationQuorumEntry;

typedef struct fdir_replication_quorum_dv_entry {
    SFVersionRange data_version;
    struct fdir_replication_quorum_dv_entry *next;
} ReplicationQuorumDVEntry;

typedef struct fdir_replication_quorum_context {
    pthread_mutex_t lock;
    volatile int master_generation;

    struct {
        volatile int dealing;
        volatile int running;
        pthread_lock_cond_pair_t lcp;
        int64_t *data_versions;
    } thread;

    struct {
        struct fast_mblock_man allocator;
        volatile int count;
        FDIRReplicationQuorumEntry *head;
        FDIRReplicationQuorumEntry *tail;
    } list;

    struct {
        volatile int64_t counter;

        volatile char slave_version_changed;
        struct {
            volatile char flag;
            volatile int64_t value;
        } set_version;

        struct {
            struct fast_mblock_man allocator;
            pthread_mutex_t lock;
            int64_t next_data_verson;
            ReplicationQuorumDVEntry *head;
            ReplicationQuorumDVEntry *tail;
        } waiting_list;
    } confirmed;
} FDIRReplicationQuorumContext;

static FDIRReplicationQuorumContext fdir_replication_quorum;

#define QUORUM_LIST_HEAD  fdir_replication_quorum.list.head
#define QUORUM_LIST_TAIL  fdir_replication_quorum.list.tail
#define QUORUM_LIST_COUNT fdir_replication_quorum.list.count
#define QUORUM_ENTRY_ALLOCATOR fdir_replication_quorum.list.allocator
#define CONFIRMED_COUNTER fdir_replication_quorum.confirmed.counter
#define DV_ENTRY_ALLOCATOR fdir_replication_quorum.confirmed.waiting_list.allocator
#define MASTER_GENERATION fdir_replication_quorum.master_generation
#define THREAD_DATA_VERSIONS fdir_replication_quorum.thread.data_versions

#define SLAVE_VERSION_CHANGED fdir_replication_quorum.confirmed.slave_version_changed
#define SET_VERSION_FLAG fdir_replication_quorum.confirmed.set_version.flag
#define SET_VERSION_VALUE fdir_replication_quorum.confirmed.set_version.value

#define DV_WAITING_LIST fdir_replication_quorum.confirmed.waiting_list
#define CONFIRMED_NEXT_DATA_VERSON  DV_WAITING_LIST.next_data_verson

static inline const char *get_quorum_dat_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s", DATA_PATH_STR,
            REPLICATION_QUORUM_DAT_FILENAME);
    return filename;
}

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

int replication_quorum_unlink_confirmed_files()
{
    int result;
    int index;
    char filename[PATH_MAX];

    for (index=0; index<VERSION_CONFIRMED_FILE_COUNT; index++) {
        get_confirmed_filename(index, filename, sizeof(filename));
        if ((result=fc_delete_file_ex(filename, "confirmed")) != 0) {
            return result;
        }
    }

    return 0;
}

static int rollback_binlog(const int64_t my_confirmed_version)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    int64_t last_data_version;
    SFBinlogFilePosition hint_pos;
    SFBinlogFilePosition position;
    char filename[PATH_MAX];

    if ((result=binlog_get_max_record_version(&last_data_version)) != 0) {
        return result;
    }

    if (my_confirmed_version >= last_data_version) {
        return replication_quorum_unlink_confirmed_files();
    }

    if ((result=binlog_get_indexes(&start_index, &last_index)) != 0) {
        return result;
    }

    hint_pos.index = last_index;
    hint_pos.offset = 0;
    if ((result=binlog_find_position(&hint_pos,
                    my_confirmed_version,
                    &position)) != 0)
    {
        return result;
    }

    if (position.index < last_index) {
        if ((result=binlog_writer_set_indexes(start_index,
                        position.index)) != 0)
        {
            return result;
        }

        for (binlog_index=position.index+1;
                binlog_index<last_index;
                binlog_index++)
        {
            binlog_get_filename(binlog_index, filename, sizeof(filename));
            if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
                return result;
            }
        }
    }

    binlog_get_filename(position.index, filename, sizeof(filename));
    if (truncate(filename, position.offset) != 0) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "truncate file %s to length: %"PRId64" fail, "
                "errno: %d, error info: %s", __LINE__, filename,
                position.offset, result, STRERROR(result));
        return result;
    }

    if ((result=binlog_get_max_record_version(&last_data_version)) != 0) {
        return result;
    }
    if (last_data_version != my_confirmed_version) {
        logError("file: "__FILE__", line: %d, "
                "binlog last_data_version: %"PRId64" != "
                "confirmed data version: %"PRId64", program exit!",
                __LINE__, last_data_version, my_confirmed_version);
        return EBUSY;
    }

    if ((result=binlog_writer_change_write_index(position.index)) != 0) {
        return result;
    }

    return replication_quorum_unlink_confirmed_files();
}

int replication_quorum_init()
{
    char quorum_filename[PATH_MAX];
    char buff[64];
    int len;
    int result;

    get_quorum_dat_filename(quorum_filename, sizeof(quorum_filename));
    if (!(REPLICA_QUORUM_NEED_MAJORITY || REPLICA_QUORUM_NEED_DETECT)) {
        if (access(quorum_filename, F_OK) == 0) {
            if ((result=replication_quorum_unlink_confirmed_files()) != 0) {
                return result;
            }
            unlink(quorum_filename);
        } else {
            result = errno != 0 ? errno : EPERM;
            if (result != ENOENT) {
                logError("file: "__FILE__", line: %d, "
                        "access file %s fail, errno: %d, error info: %s",
                        __LINE__, quorum_filename, result, STRERROR(result));
                return result;
            }
        }

        return 0;
    }

    if ((result=fast_mblock_init_ex1(&QUORUM_ENTRY_ALLOCATOR,
                    "repl_quorum_entry", sizeof(FDIRReplicationQuorumEntry),
                    4096, 0, NULL, NULL, false)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&DV_ENTRY_ALLOCATOR, "quorum_dv_entry",
                    sizeof(ReplicationQuorumDVEntry), 4096, 0,
                    NULL, NULL, false)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock(&fdir_replication_quorum.lock)) != 0) {
        return result;
    }

    if ((result=init_pthread_lock(&DV_WAITING_LIST.lock)) != 0) {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(
                    &fdir_replication_quorum.
                    thread.lcp)) != 0)
    {
        return result;
    }

    THREAD_DATA_VERSIONS = fc_malloc(sizeof(int64_t) *
            CLUSTER_SERVER_ARRAY.count);
    if (THREAD_DATA_VERSIONS == NULL) {
        return ENOMEM;
    }

    if ((result=load_confirmed_version((int64_t *)
                    &MY_CONFIRMED_VERSION)) != 0)
    {
        return result;
    }

    if (MY_CONFIRMED_VERSION > 0) {
        if ((result=rollback_binlog(MY_CONFIRMED_VERSION)) != 0) {
            return result;
        }
    }

    fdir_replication_quorum.thread.dealing = 0;
    fdir_replication_quorum.thread.running = 0;
    CONFIRMED_COUNTER = 0;
    QUORUM_LIST_COUNT = 0;
    QUORUM_LIST_HEAD = QUORUM_LIST_TAIL = NULL;

    DV_WAITING_LIST.head = NULL;
    DV_WAITING_LIST.tail = NULL;
    CONFIRMED_NEXT_DATA_VERSON = 0;
    SLAVE_VERSION_CHANGED = 0;
    SET_VERSION_FLAG = 0;
    SET_VERSION_VALUE = 0;

    if (access(quorum_filename, F_OK) == 0) {
        return 0;
    } else {
        len = sprintf(buff, "enabled=%d\n", 1);
        return writeToFile(quorum_filename, buff, len);
    }
}

void replication_quorum_destroy()
{
}

int replication_quorum_add(struct fast_task_info *task,
        const int64_t data_version, bool *finished)
{
    int result;
    FDIRReplicationQuorumEntry *previous;
    FDIRReplicationQuorumEntry *entry;

    PTHREAD_MUTEX_LOCK(&fdir_replication_quorum.lock);
    do {
        if (data_version <= FC_ATOMIC_GET(MY_CONFIRMED_VERSION)) {
            *finished = true;
            result = 0;
            break;
        }

        *finished = false;
        if (!FC_ATOMIC_GET(MYSELF_IN_MASTER_TERM)) {
            result = EAGAIN;
            break;
        }

        entry = fast_mblock_alloc_object(&QUORUM_ENTRY_ALLOCATOR);
        if (entry == NULL) {
            result = ENOMEM;
            break;
        }
        entry->task = task;
        entry->data_version = data_version;
        FC_ATOMIC_INC(QUORUM_LIST_COUNT);

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

        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);

    return result;
}

static int compare_int64(const int64_t *n1, const int64_t *n2)
{
    return fc_compare_int64(*n1, *n2);
}

static int write_to_confirmed_file(const int index,
        const int64_t confirmed_version)
{
    const int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC;
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
    int count;

    count = 0;
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
            ++count;
        } while (QUORUM_LIST_HEAD != NULL && my_confirmed_version >=
                QUORUM_LIST_HEAD->data_version);

        if (QUORUM_LIST_HEAD == NULL) {
            QUORUM_LIST_TAIL = NULL;
        }
        chain.tail->next = NULL;
    }

    if (chain.head != NULL) {
        fast_mblock_batch_free(&QUORUM_ENTRY_ALLOCATOR, &chain);
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);

    if (count > 0) {
        FC_ATOMIC_DEC_EX(QUORUM_LIST_COUNT, count);
    }
}

static void clear_waiting_tasks()
{
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;
    int count;

    count = 0;
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

            sf_nio_notify(QUORUM_LIST_HEAD->task, SF_NIO_STAGE_CONTINUE);
            QUORUM_LIST_HEAD = QUORUM_LIST_HEAD->next;
            ++count;
        } while (QUORUM_LIST_HEAD != NULL);

        if (QUORUM_LIST_HEAD == NULL) {
            QUORUM_LIST_TAIL = NULL;
        }
        chain.tail->next = NULL;
    }

    if (chain.head != NULL) {
        fast_mblock_batch_free(&QUORUM_ENTRY_ALLOCATOR, &chain);
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);

    if (count > 0) {
        FC_ATOMIC_DEC_EX(QUORUM_LIST_COUNT, count);
    }
}

void replication_quorum_deal_version_change(
        const int64_t slave_confirmed_version)
{
    if (slave_confirmed_version <= FC_ATOMIC_GET(MY_CONFIRMED_VERSION)) {
        return;
    }

    __sync_bool_compare_and_swap(&SLAVE_VERSION_CHANGED, 0, 1);
    if (__sync_bool_compare_and_swap(&fdir_replication_quorum.
                thread.dealing, 0, 1))
    {
        pthread_cond_signal(&fdir_replication_quorum.thread.lcp.cond);
    }
}

static void deal_waiting_version_list()
{
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;
    int64_t confirmed_version;

    if (DV_WAITING_LIST.head != NULL && DV_WAITING_LIST.head->
            data_version.first == CONFIRMED_NEXT_DATA_VERSON)
    {
        chain.head = chain.tail = NULL;
        do {
            node = fast_mblock_to_node_ptr(DV_WAITING_LIST.head);
            if (chain.head == NULL) {
                chain.head = node;
            } else {
                chain.tail->next = node;
            }
            chain.tail = node;

            CONFIRMED_NEXT_DATA_VERSON = DV_WAITING_LIST.
                head->data_version.last + 1;
            DV_WAITING_LIST.head = DV_WAITING_LIST.head->next;
        } while (DV_WAITING_LIST.head != NULL && DV_WAITING_LIST.head->
                data_version.first == CONFIRMED_NEXT_DATA_VERSON);

        if (DV_WAITING_LIST.head == NULL) {
            DV_WAITING_LIST.tail = NULL;
        }
        chain.tail->next = NULL;
        fast_mblock_batch_free(&DV_ENTRY_ALLOCATOR, &chain);
    }

    confirmed_version = CONFIRMED_NEXT_DATA_VERSON - 1;
    if (confirmed_version > FC_ATOMIC_GET(MY_CONFIRMED_VERSION)) {
        __sync_bool_compare_and_swap(&SET_VERSION_VALUE,
                SET_VERSION_VALUE, confirmed_version);
        __sync_bool_compare_and_swap(&SET_VERSION_FLAG, 0, 1);
        if (__sync_bool_compare_and_swap(&fdir_replication_quorum.
                    thread.dealing, 0, 1))
        {
            pthread_cond_signal(&fdir_replication_quorum.thread.lcp.cond);
        }
    }
}

void replication_quorum_push_confirmed_version(const SFVersionRange *version)
{
    ReplicationQuorumDVEntry *entry;
    ReplicationQuorumDVEntry *previous;

    if (version->last <= FC_ATOMIC_GET(MY_CONFIRMED_VERSION)) {
        return;
    }

    PTHREAD_MUTEX_LOCK(&DV_WAITING_LIST.lock);
    do {
        if (version->first == CONFIRMED_NEXT_DATA_VERSON) {
            CONFIRMED_NEXT_DATA_VERSON = version->last + 1;
            deal_waiting_version_list();
            break;
        }

        entry = fast_mblock_alloc_object(&DV_ENTRY_ALLOCATOR);
        if (entry == NULL) {
            break;
        }

        entry->data_version = *version;
        if (DV_WAITING_LIST.head == NULL) {
            entry->next = NULL;
            DV_WAITING_LIST.head = entry;
            DV_WAITING_LIST.tail = entry;
        } else if (version->first >= DV_WAITING_LIST.
                tail->data_version.first)
        {
            entry->next = NULL;
            DV_WAITING_LIST.tail->next = entry;
            DV_WAITING_LIST.tail = entry;
        } else if (version->first <= DV_WAITING_LIST.
                head->data_version.first)
        {
            entry->next = DV_WAITING_LIST.head;
            DV_WAITING_LIST.head = entry;
        } else {
            previous = DV_WAITING_LIST.head;
            while (version->first > previous->next->data_version.first) {
                previous = previous->next;
            }
            entry->next = previous->next;
            previous->next = entry;
        }
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&DV_WAITING_LIST.lock);
}

static void consume_version_chain(const int64_t my_confirmed_version)
{
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;

    PTHREAD_MUTEX_LOCK(&DV_WAITING_LIST.lock);
    if (DV_WAITING_LIST.head != NULL && DV_WAITING_LIST.head->
            data_version.first <= my_confirmed_version)
    {
        chain.head = chain.tail = NULL;
        do {
            node = fast_mblock_to_node_ptr(DV_WAITING_LIST.head);
            if (chain.head == NULL) {
                chain.head = node;
            } else {
                chain.tail->next = node;
            }
            chain.tail = node;

            CONFIRMED_NEXT_DATA_VERSON = DV_WAITING_LIST.
                head->data_version.last + 1;
            DV_WAITING_LIST.head = DV_WAITING_LIST.head->next;
        } while (DV_WAITING_LIST.head != NULL && (DV_WAITING_LIST.head->
                    data_version.first <= my_confirmed_version ||
                    DV_WAITING_LIST.head->data_version.first ==
                    CONFIRMED_NEXT_DATA_VERSON));

        if (DV_WAITING_LIST.head == NULL) {
            DV_WAITING_LIST.tail = NULL;
        }

        chain.tail->next = NULL;
        fast_mblock_batch_free(&DV_ENTRY_ALLOCATOR, &chain);
    }
    PTHREAD_MUTEX_UNLOCK(&DV_WAITING_LIST.lock);
}

static void deal_version_change(int64_t *my_confirmed_version)
{
    FDIRClusterServerInfo *server;
    FDIRClusterServerInfo *end;
    int64_t my_current_version;
    int64_t confirmed_version;
    int more_than_half;
    int count;
    int index;

    count = 0;
    my_current_version = FC_ATOMIC_GET(DATA_CURRENT_VERSION);
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (server=CLUSTER_SERVER_ARRAY.servers; server<end; server++) {
        if (server == CLUSTER_MYSELF_PTR) {
            continue;
        }

        confirmed_version=FC_ATOMIC_GET(server->confirmed_data_version);
        if (confirmed_version > *my_confirmed_version &&
                confirmed_version <= my_current_version)
        {
            THREAD_DATA_VERSIONS[count++] = confirmed_version;
        }
    }
    more_than_half = CLUSTER_SERVER_ARRAY.count / 2 + 1;

    if (count + 1 >= more_than_half) {  //quorum majority
        if (CLUSTER_SERVER_ARRAY.count == 3) {  //fast path
            if (count == 2) {
                *my_confirmed_version = FC_MAX(THREAD_DATA_VERSIONS[0],
                        THREAD_DATA_VERSIONS[1]);
            } else {  //count == 1
                *my_confirmed_version = THREAD_DATA_VERSIONS[0];
            }
        } else {
            qsort(THREAD_DATA_VERSIONS, count, sizeof(int64_t),
                    (int (*)(const void *, const void *))
                    compare_int64);
            index = (count + 1) - more_than_half;
            *my_confirmed_version = THREAD_DATA_VERSIONS[index];
        }

        FC_ATOMIC_SET(MY_CONFIRMED_VERSION, *my_confirmed_version);
        consume_version_chain(*my_confirmed_version);
    }
}

static void *replication_quorum_thread_run(void *arg)
{
    struct timespec ts;
    int generation;
    int timeout;
    int64_t old_confirmed_version;
    int64_t new_confirmed_version;
    bool set_version;
    bool by_slave;

    generation = (long)arg;
    __sync_bool_compare_and_swap(&fdir_replication_quorum.
            thread.running, 0, 1);

    timeout = (REPLICA_QUORUM_NEED_DETECT ? 2 : 3);
    while (FC_ATOMIC_GET(MASTER_GENERATION) == generation) {
        ts.tv_sec = g_current_time + timeout;
        ts.tv_nsec = 0;
        PTHREAD_MUTEX_LOCK(&fdir_replication_quorum.thread.lcp.lock);
        pthread_cond_timedwait(&fdir_replication_quorum.
                thread.lcp.cond, &fdir_replication_quorum.
                thread.lcp.lock, &ts);

        if (__sync_bool_compare_and_swap(&fdir_replication_quorum.
                    thread.dealing, 1, 0))
        {
            old_confirmed_version = FC_ATOMIC_GET(MY_CONFIRMED_VERSION);
            if ((set_version=__sync_bool_compare_and_swap(
                            &SET_VERSION_FLAG, 1, 0)))
            {
                new_confirmed_version = FC_ATOMIC_GET(SET_VERSION_VALUE);
                if (new_confirmed_version > old_confirmed_version) {
                    __sync_bool_compare_and_swap(&MY_CONFIRMED_VERSION,
                            old_confirmed_version, new_confirmed_version);
                } else if (new_confirmed_version < old_confirmed_version) {
                    new_confirmed_version = old_confirmed_version;
                }
            } else {
                new_confirmed_version = old_confirmed_version;
            }

            if ((by_slave=__sync_bool_compare_and_swap(
                            &SLAVE_VERSION_CHANGED, 1, 0)))
            {
                if (CLUSTER_MYSELF_PTR == CLUSTER_MASTER_ATOM_PTR) {
                    deal_version_change(&new_confirmed_version);
                }
            }

            /*
            logInfo("set_version: %d, by_slave: %d, confirmed_versions "
                    "{old: %"PRId64", new: %"PRId64"}, waiting count: %d",
                    set_version, by_slave, old_confirmed_version,
                    new_confirmed_version, FC_ATOMIC_GET(QUORUM_LIST_COUNT));
                    */

            if (new_confirmed_version > old_confirmed_version &&
                    FC_ATOMIC_GET(REPLICA_QUORUM_NEED_MAJORITY))
            {
                if (write_to_confirmed_file(FC_ATOMIC_INC(CONFIRMED_COUNTER) %
                            VERSION_CONFIRMED_FILE_COUNT,
                            new_confirmed_version) != 0)
                {
                    sf_terminate_myself();
                    break;
                }

                if (FC_ATOMIC_GET(QUORUM_LIST_COUNT) > 0) {
                    notify_waiting_tasks(new_confirmed_version);
                }

                if (REPLICA_QUORUM_NEED_DETECT && !FC_ATOMIC_GET(
                            REPLICA_QUORUM_NEED_MAJORITY))
                {
                    replication_quorum_unlink_confirmed_files();
                }
            }
        } else if (REPLICA_QUORUM_NEED_DETECT &&
                !FC_ATOMIC_GET(REPLICA_QUORUM_NEED_MAJORITY) &&
                FC_ATOMIC_GET(QUORUM_LIST_COUNT) > 0)
        {
            clear_waiting_tasks();
        }
        PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.thread.lcp.lock);
    }

    __sync_bool_compare_and_swap(&fdir_replication_quorum.
            thread.running, 1, 0);
    return NULL;
}

int replication_quorum_start_master_term()
{
    int i;
    int master_generation;
    pthread_t tid;

    if (!(REPLICA_QUORUM_NEED_MAJORITY | REPLICA_QUORUM_NEED_DETECT)) {
        return 0;
    }

    PTHREAD_MUTEX_LOCK(&DV_WAITING_LIST.lock);
    DV_WAITING_LIST.next_data_verson = FC_ATOMIC_GET(
            DATA_CURRENT_VERSION) + 1;
    PTHREAD_MUTEX_UNLOCK(&DV_WAITING_LIST.lock);

    master_generation = FC_ATOMIC_INC(MASTER_GENERATION);
    i = 0;
    while (FC_ATOMIC_GET(fdir_replication_quorum.
                thread.running) && i++ < 30)
    {
        pthread_cond_signal(&fdir_replication_quorum.thread.lcp.cond);
        fc_sleep_ms(100);
    }

    if (FC_ATOMIC_GET(fdir_replication_quorum.thread.running)) {
        logWarning("file: "__FILE__", line: %d, "
                "thread alread exist", __LINE__);
        return 0;
    }

    return fc_create_thread(&tid, replication_quorum_thread_run,
            (void *)(long)master_generation, SF_G_THREAD_STACK_SIZE);
}

int replication_quorum_end_master_term()
{
    int64_t my_confirmed_version;
    int64_t current_data_version;
    pid_t pid;

    if (!(REPLICA_QUORUM_NEED_MAJORITY || REPLICA_QUORUM_NEED_DETECT)) {
        return 0;
    }

    FC_ATOMIC_INC(MASTER_GENERATION);
    pthread_cond_signal(&fdir_replication_quorum.thread.lcp.cond);
    my_confirmed_version = FC_ATOMIC_GET(MY_CONFIRMED_VERSION);
    current_data_version = FC_ATOMIC_GET(DATA_CURRENT_VERSION);
    if (my_confirmed_version >= current_data_version) {
        return replication_quorum_unlink_confirmed_files();
    }

    pid = fork();
    if (pid < 0) {
        return (errno != 0 ? errno : EBUSY);
    } else if (pid > 0) {
        logInfo("file: "__FILE__", line: %d, "
                "i am not the master, restart to rollback data",
                __LINE__);
        return 0;
    }

    //child process
    if (execlp(CMDLINE_PROGRAM_FILENAME, CMDLINE_PROGRAM_FILENAME,
                CMDLINE_CONFIG_FILENAME, "restart", NULL) < 0)
    {
        int result;
        result = errno != 0 ? errno : EBUSY;
        logError("file: "__FILE__", line: %d, "
                "exec \"%s %s restart\" fail, errno: %d, error info: %s",
                __LINE__, CMDLINE_PROGRAM_FILENAME, CMDLINE_CONFIG_FILENAME,
                result, STRERROR(result));

        log_sync_func(&g_log_context);
        kill(getppid(), SIGQUIT);
        exit(result);
    }

    return 0;
}

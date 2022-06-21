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
#include "common/fdir_types.h"
#include "server_global.h"
#include "replication_quorum.h"

typedef struct fdir_replication_quorum_context {
    struct fast_mblock_man allocator; //element: FDIRReplicationQuorumEntry
    pthread_mutex_t lock;
    volatile int dealing;
    struct {
        FDIRReplicationQuorumEntry *head;
        FDIRReplicationQuorumEntry *tail;
    } list;
} FDIRReplicationQuorumContext;

static FDIRReplicationQuorumContext fdir_replication_quorum;

#define QUORUM_LIST_HEAD  fdir_replication_quorum.list.head
#define QUORUM_LIST_TAIL  fdir_replication_quorum.list.tail

int replication_quorum_init()
{
    int result;

    if ((result=fast_mblock_init_ex1(&fdir_replication_quorum.allocator,
                    "repl_quorum_entry", sizeof(FDIRReplicationQuorumEntry),
                    4096, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock(&fdir_replication_quorum.lock)) != 0) {
        return result;
    }

    fdir_replication_quorum.dealing = 0;
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

    if ((entry=fast_mblock_alloc_object(&fdir_replication_quorum.
                    allocator)) == NULL)
    {
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

        //TODO
    }

    if (data_versions != fixed_data_versions) {
        free(data_versions);
    }

    __sync_bool_compare_and_swap(
            &fdir_replication_quorum.
            dealing, 1, 0);
}

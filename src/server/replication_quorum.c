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

    QUORUM_LIST_HEAD = QUORUM_LIST_TAIL = NULL;
    return 0;
}

void replication_quorum_destroy()
{
}

int replication_quorum_add(struct fast_task_info *task,
        const int64_t data_version)
{
    FDIRReplicationQuorumEntry *entry;

    if ((entry=fast_mblock_alloc_object(&fdir_replication_quorum.
                    allocator)) == NULL)
    {
        return ENOMEM;
    }
    entry->task = task;
    entry->data_version = data_version;
    entry->next = NULL;

    PTHREAD_MUTEX_LOCK(&fdir_replication_quorum.lock);
    if (QUORUM_LIST_HEAD == NULL) {
        QUORUM_LIST_HEAD = entry;
    } else {
        QUORUM_LIST_TAIL->next = entry;
    }
    QUORUM_LIST_TAIL = entry;
    PTHREAD_MUTEX_UNLOCK(&fdir_replication_quorum.lock);
    return 0;
}

int replication_quorum_remove(const int64_t data_version)
{
    return 0;
}

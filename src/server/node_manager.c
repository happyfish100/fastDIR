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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/hash.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "common/fdir_types.h"
#include "server_global.h"
#include "node_manager.h"

typedef struct fdir_node_entry {
    uint32_t id;
    int64_t key;
    char ip[IP_ADDRESS_SIZE];
    struct fdir_node_entry *next;
} FDIRNodeEntry;

typedef struct fdir_node_hashtable {
    int count;
    FDIRNodeEntry **buckets;
} FDIRNodeHashtable;

typedef struct fdir_node_manager {
    FDIRNodeHashtable hashtable;

    struct fast_mblock_man node_allocator;  //element: FDIRNodeEntry
    pthread_mutex_t lock;
    int current_id;
} FDIRNodeManager;

static FDIRNodeManager fdir_node_manager = {{0, NULL}};

int node_manager_init()
{
    int result;
    int bytes;

    memset(&fdir_node_manager, 0, sizeof(fdir_node_manager));
    if ((result=fast_mblock_init_ex1(&fdir_node_manager.node_allocator,
                    "node_entry", sizeof(FDIRNodeEntry), 4096, 0,
                    NULL, NULL, false)) != 0)
    {
        return result;
    }

    fdir_node_manager.hashtable.count = 0;
    bytes = sizeof(FDIRNodeEntry *) * g_server_global_vars.
        node_hashtable_capacity;
    fdir_node_manager.hashtable.buckets = fc_malloc(bytes);
    if (fdir_node_manager.hashtable.buckets == NULL) {
        return ENOMEM;
    }
    memset(fdir_node_manager.hashtable.buckets, 0, bytes);

    if ((result=init_pthread_lock(&fdir_node_manager.lock)) != 0) {
        return result;
    }

    return 0;
}

void node_manager_destroy()
{
}

#define NODE_SET_HT_BUCKET(id) \
    FDIRNodeEntry **bucket; \
    \
    bucket = fdir_node_manager.hashtable.buckets + (id) % \
        g_server_global_vars.node_hashtable_capacity


static int create_node(FDIRNodeEntry **bucket, const int id,
         const int64_t key, const char *ip_addr)
{
    FDIRNodeEntry *entry;

    entry = (FDIRNodeEntry *)fast_mblock_alloc_object(
            &fdir_node_manager.node_allocator);
    if (entry == NULL) {
        return ENOMEM;
    }

    entry->id = id;
    entry->key = key;
    snprintf(entry->ip, sizeof(entry->ip), "%s", ip_addr); 
    entry->next = *bucket;
    *bucket = entry;
    return 0;
}

int node_manager_add_node(uint32_t *id, int64_t *key, const char *ip_addr)
{
    int result = EAGAIN;
    int64_t ip;
    FDIRNodeEntry *entry;

    PTHREAD_MUTEX_LOCK(&fdir_node_manager.lock);
    while (1) {
        //logInfo("id: %u, key: %"PRId64", ip_addr: %s", *id, *key, ip_addr);
        NODE_SET_HT_BUCKET(*id);
        entry = *bucket;
        while (entry != NULL && entry->id != *id) {
            entry = entry->next;
        }

        if (entry != NULL) {
            if (*key == entry->key) {
                if (strcmp(entry->ip, ip_addr) != 0) {
                    snprintf(entry->ip, sizeof(entry->ip), "%s", ip_addr);
                }
                result = 0;
                break;
            }
        } else if (*id != 0) {
            if (*id > fdir_node_manager.current_id) {
                fdir_node_manager.current_id = *id;
            }
            result = create_node(bucket, *id, *key, ip_addr);
            break;
        }

        ip = getIpaddrByName(ip_addr, NULL, 0);
        *id = ++fdir_node_manager.current_id;
        *key = (ip << 32) | (int64_t)rand();
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_node_manager.lock);

    return result;
}

int node_manager_get_ip_addr(const uint32_t id, char *ip_addr)
{
    int result;
    FDIRNodeEntry *entry;

    NODE_SET_HT_BUCKET(id);
    PTHREAD_MUTEX_LOCK(&fdir_node_manager.lock);
    entry = *bucket;
    while (entry != NULL && entry->id != id) {
        entry = entry->next;
    }

    if (entry != NULL) {
        result = 0;
        strcpy(ip_addr, entry->ip);
    } else {
        result = ENOENT;
    }
    PTHREAD_MUTEX_UNLOCK(&fdir_node_manager.lock);

    return result;
}

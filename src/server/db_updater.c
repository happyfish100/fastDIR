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
#include "fastcommon/pthread_func.h"
#include "server_global.h"
#include "db_updater.h"

typedef struct fdir_db_updater_context {
    struct fc_list_head head;
    pthread_lock_cond_pair_t lc_pair;
} FDIRDBUpdaterContext;

static FDIRDBUpdaterContext db_updater_ctx;

static void *db_updater_func(void *arg)
{

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "db-updater");
#endif

    while (SF_G_CONTINUE_FLAG) {
        //TODO
    }

    return NULL;
}

int db_updater_init()
{
    int result;
    pthread_t tid;

    if ((result=init_pthread_lock_cond_pair(&db_updater_ctx.lc_pair)) != 0) {
        return result;
    }

    FC_INIT_LIST_HEAD(&db_updater_ctx.head);
    return fc_create_thread(&tid, db_updater_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void db_updater_destroy()
{
}

void db_updater_push_to_queue(FDIRServerDentry *dentry,
        const int64_t version, const DABinlogOpType op_type)
{
    bool notify;

    PTHREAD_MUTEX_LOCK(&db_updater_ctx.lc_pair.lock);
    dentry->db_args->version = version;
    dentry->db_args->op_type = op_type;
    notify = fc_list_empty(&db_updater_ctx.head);
    if (!dentry->db_args->in_queue) {
        dentry->db_args->in_queue = true;
        __sync_add_and_fetch(&dentry->db_args->reffer_count, 1);
        fc_list_add_tail(&dentry->db_args->dlink, &db_updater_ctx.head);
    } else {
        fc_list_move_tail(&dentry->db_args->dlink, &db_updater_ctx.head);
    }
    PTHREAD_MUTEX_UNLOCK(&db_updater_ctx.lc_pair.lock);

    if (notify) {
        pthread_cond_signal(&db_updater_ctx.lc_pair.cond);
    }
}

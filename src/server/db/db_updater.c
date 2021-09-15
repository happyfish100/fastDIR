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
#include "../server_global.h"
#include "../dentry.h"
#include "dentry_serializer.h"
#include "db_updater.h"

typedef struct fdir_db_updater_context {
    //pthread_lock_cond_pair_t lc_pair;
} FDIRDBUpdaterContext;

//static FDIRDBUpdaterContext db_updater_ctx;

int db_updater_init()
{
    return 0;
}

void db_updater_destroy()
{
}

int db_updater_realloc_dentry_array(FDIRDBUpdaterDentryArray *array)
{
    FDIRDBUpdaterDentry *entries;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    entries = (FDIRDBUpdaterDentry *)fc_malloc(
            sizeof(FDIRDBUpdaterDentry) * array->alloc);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (array->entries != NULL) {
        memcpy(entries, array->entries, sizeof(
                    FDIRDBUpdaterDentry) * array->count);
        free(array->entries);
    }

    array->entries = entries;
    return 0;
}

int db_updater_deal(const FDIRDBUpdaterDentryArray *array,
        const int64_t last_version)
{
    return 0;
}

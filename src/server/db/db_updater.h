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


#ifndef _FDIR_DB_UPDATER_H
#define _FDIR_DB_UPDATER_H

#include "../server_types.h"

typedef struct fdir_db_updater_context {
    FDIRDBUpdateFieldArray array;
    struct {
        int64_t dentry;  //for check with FDIR server's data version
        int64_t field;   //for check internal storage engine
    } last_versions;
    FastBuffer buffer;
} FDIRDBUpdaterContext;

#ifdef __cplusplus
extern "C" {
#endif

    int db_updater_init(FDIRDBUpdaterContext *ctx);
    void db_updater_destroy();

    int db_updater_realloc_dentry_array(FDIRDBUpdateFieldArray *array);

    int db_updater_deal(FDIRDBUpdaterContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

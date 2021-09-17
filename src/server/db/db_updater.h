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
#include "change_notify.h"

typedef struct fdir_db_updater_message {
    int field_index;
    FastBuffer *buffer;
} FDIRDBUpdaterMessage;

typedef struct fdir_dentry_merged_messages {
    FDIRDBUpdaterMessage messages[FDIR_PIECE_FIELD_COUNT];
    int msg_count;
    int merge_count;
} FDIRDentryMergedMessages;

typedef struct fdir_db_updater_dentry {
    int64_t version;
    int64_t inode;
    DABinlogOpType op_type;
    FDIRServerPieceStorage fields[FDIR_PIECE_FIELD_COUNT];
    FDIRDentryMergedMessages mms;
    FDIRServerDentry *dentry;
} FDIRDBUpdaterDentry;

typedef struct fdir_db_updater_dentry_array {
    FDIRDBUpdaterDentry *entries;
    int count;
    int alloc;
} FDIRDBUpdaterDentryArray;

typedef struct fdir_db_updater_context {
    FDIRDBUpdaterDentryArray array;
    int64_t last_version;
    FastBuffer buffer;
} FDIRDBUpdaterContext;

#ifdef __cplusplus
extern "C" {
#endif

    int db_updater_init();
    void db_updater_destroy();

    int db_updater_realloc_dentry_array(FDIRDBUpdaterDentryArray *array);

    int db_updater_deal(FDIRDBUpdaterContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

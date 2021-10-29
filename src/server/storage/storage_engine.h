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

#ifndef _FDIR_STORAGE_ENGINE_H
#define _FDIR_STORAGE_ENGINE_H

#include "fastcommon/ini_file_reader.h"
#include "diskallocator/global.h"
#include "storage_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fdir_storage_engine_init(IniFullContext *ini_ctx,
            const int my_server_id, const FDIRStorageEngineConfig *db_cfg,
            const DADataGlobalConfig *data_cfg);


    int fdir_storage_engine_start();

    void fdir_storage_engine_terminate();

    int fdir_storage_engine_store(FDIRDBUpdateFieldArray *array);

    int fdir_storage_engine_fetch(const int64_t inode,
            const int field_index, FastBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif

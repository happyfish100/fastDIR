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

#ifndef _FDIR_POOLED_CONNECTION_MANAGER_H
#define _FDIR_POOLED_CONNECTION_MANAGER_H

#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdir_pooled_connection_manager_init(FDIRClientContext *client_ctx,
        SFConnectionManager *cm, const int max_count_per_entry,
        const int max_idle_time, const bool bg_thread_enabled);

void fdir_pooled_connection_manager_destroy(SFConnectionManager *cm);

#ifdef __cplusplus
}
#endif

#endif

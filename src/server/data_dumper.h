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


#ifndef _FDIR_DATA_DUMPER_H
#define _FDIR_DATA_DUMPER_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef FDIR_DUMP_DATA_FOR_DEBUG
    int server_dump_init();
    int server_dump_data(struct fdir_data_thread_context *thread_ctx);
#endif

#ifdef __cplusplus
}
#endif

#endif

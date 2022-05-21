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


#ifndef _FDIR_CLIENT_GLOBAL_H
#define _FDIR_CLIENT_GLOBAL_H

#include "fdir_global.h"
#include "client_types.h"

typedef struct fdir_client_global_vars {
    char base_path[MAX_PATH_SIZE];

    FDIRClientContext client_ctx;
} FDIRClientGlobalVars;

#define FDIR_CLIENT_NODE_ID g_fdir_client_vars.client_ctx.node_id

#ifdef __cplusplus
extern "C" {
#endif

    extern FDIRClientGlobalVars g_fdir_client_vars;

#ifdef __cplusplus
}
#endif

#endif

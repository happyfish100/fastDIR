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


#ifndef _FDIR_FUNC_H
#define _FDIR_FUNC_H

#include "fdir_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdir_validate_xattr(const key_value_pair_t *xattr);

int fdir_load_cluster_config_ex(FDIRClusterConfig *cluster,
        IniFullContext *ini_ctx, char *full_server_filename,
        const int size);

static inline int fdir_load_cluster_config(FDIRClusterConfig *cluster,
        IniFullContext *ini_ctx)
{
    char full_server_filename[PATH_MAX];
    return fdir_load_cluster_config_ex(cluster, ini_ctx,
            full_server_filename, sizeof(full_server_filename));
}

#ifdef __cplusplus
}
#endif

#endif

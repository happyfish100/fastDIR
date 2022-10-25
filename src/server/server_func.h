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


#ifndef _FDIR_SERVER_FUNC_H
#define _FDIR_SERVER_FUNC_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_load_config(const char *filename);

#define SERVER_PROTO_PACK_IDENTITY(si)     \
    int2buff(CLUSTER_ID, (si).cluster_id); \
    int2buff(CLUSTER_MY_SERVER_ID, (si).server_id);   \
    (si).auth_enabled = (AUTH_ENABLED ? 1 : 0);       \
    memcpy((si).config_sign, CLUSTER_CONFIG_SIGN_BUF, \
            SF_CLUSTER_CONFIG_SIGN_LEN)

#ifdef __cplusplus
}
#endif

#endif

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


#ifndef _FDIR_NODE_MANAGER_H
#define _FDIR_NODE_MANAGER_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int node_manager_init();
    void node_manager_destroy();

    int node_manager_add_node(uint32_t *id,
            int64_t *key, const char *ip_addr);

    int node_manager_get_ip_addr(const uint32_t id, char *ip_addr);

#ifdef __cplusplus
}
#endif

#endif

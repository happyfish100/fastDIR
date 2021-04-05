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


#ifndef _NS_SUBSCRIBE_H
#define _NS_SUBSCRIBE_H

#include "fastcommon/fast_mblock.h"
#include "server_types.h"

struct fdir_namespace_entry;

typedef struct fdir_ns_subscribe_for_queue {
    volatile int in_queue;
    struct fdir_ns_subscribe_entry *next;
} FDIRNSSubscribeForQueue;

typedef struct fdir_ns_subscribe_entry {
    struct fdir_namespace_entry *ns;
    FDIRNSSubscribeForQueue entries[2];
} FDIRNSSubscribeEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int ns_subscribe_init();
    void ns_subscribe_destroy();

#ifdef __cplusplus
}
#endif

#endif

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

//push_result_ring.h

#ifndef _PUSH_RESULT_RING_H_
#define _PUSH_RESULT_RING_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int push_result_ring_check_init(FDIRBinlogPushResultContext *ctx,
        const int alloc_size);

void push_result_ring_destroy(FDIRBinlogPushResultContext *ctx);

int push_result_ring_add(FDIRBinlogPushResultContext *ctx,
        const SFVersionRange *data_version,
        struct fast_task_info *waiting_task);

int push_result_ring_remove(FDIRBinlogPushResultContext *ctx,
        const uint64_t data_version);

void push_result_ring_clear_all(FDIRBinlogPushResultContext *ctx);

void push_result_ring_clear_timeouts(FDIRBinlogPushResultContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

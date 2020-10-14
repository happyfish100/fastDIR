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

//binlog_producer.h

#ifndef _BINLOG_PRODUCER_H_
#define _BINLOG_PRODUCER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_producer_init();
int binlog_producer_start();
void binlog_producer_destroy();

ServerBinlogRecordBuffer *server_binlog_alloc_hold_rbuffer();

void server_binlog_release_rbuffer(ServerBinlogRecordBuffer *rbuffer);

void server_binlog_free_rbuffer(ServerBinlogRecordBuffer *rbuffer);

//int server_binlog_dispatch(ServerBinlogRecordBuffer *rbuffer);
void binlog_push_to_producer_queue(ServerBinlogRecordBuffer *rbuffer);

#ifdef __cplusplus
}
#endif

#endif

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

//binlog_local_consumer.h

#ifndef _BINLOG_LOCAL_CONSUMER_H_
#define _BINLOG_LOCAL_CONSUMER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_local_consumer_init();
void binlog_local_consumer_destroy();
void binlog_local_consumer_terminate();

void binlog_local_consumer_waiting_replication_finish();
int binlog_local_consumer_replication_start();
int binlog_local_consumer_push_to_queues(ServerBinlogRecordBuffer *rbuffer);

#ifdef __cplusplus
}
#endif

#endif

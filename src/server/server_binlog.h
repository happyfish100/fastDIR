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

//server_binlog.h

#ifndef _SERVER_BINLOG_H_
#define _SERVER_BINLOG_H_

#include "binlog/binlog_types.h"
#include "binlog/binlog_producer.h"
#include "binlog/binlog_local_consumer.h"
#include "binlog/binlog_write.h"
#include "binlog/binlog_read_thread.h"
#include "binlog/replica_consumer_thread.h"
#include "binlog/binlog_pack.h"
#include "binlog/binlog_replay.h"
#include "binlog/binlog_replay_mt.h"
#include "binlog/binlog_dump.h"
#include "binlog/binlog_sort.h"
#include "binlog/binlog_dedup.h"
#include "binlog/binlog_clean.h"
#include "binlog/binlog_sync.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_binlog_init();
void server_binlog_destroy();
void server_binlog_terminate();

#ifdef __cplusplus
}
#endif

#endif

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

//binlog_replication.h

#ifndef _BINLOG_REPLICATION_H_
#define _BINLOG_REPLICATION_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replication_bind_thread(FDIRSlaveReplication *replication);
int binlog_replication_rebind_thread(FDIRSlaveReplication *replication);

int binlog_replication_process(FDIRServerContext *server_ctx);

void binlog_replication_connect_done(struct fast_task_info *task,
        const int err_no);
int binlog_replication_join_slave(struct fast_task_info *task);

void clean_master_replications(FDIRServerContext *server_ctx);

int binlog_replications_check_response_data_version(
        FDIRSlaveReplication *replication,
        const int64_t data_version, const int err_no);

#ifdef __cplusplus
}
#endif

#endif

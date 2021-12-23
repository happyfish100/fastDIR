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

//service_handler.h

#ifndef FDIR_SERVER_HANDLER_H
#define FDIR_SERVER_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"
#include "binlog/binlog_types.h"
#include "inode_index.h"

#ifdef __cplusplus
extern "C" {
#endif

int service_handler_init();
int service_handler_destroy();
int service_deal_task(struct fast_task_info *task, const int stage);
void service_task_finish_cleanup(struct fast_task_info *task);
void *service_alloc_thread_extra_data(const int thread_index);
//int service_thread_loop(struct nio_thread_data *thread_data);

int service_set_record_pname_info(FDIRBinlogRecord *record,
        struct fast_task_info *task);

int service_set_record_link(FDIRBinlogRecord *record,
        struct fast_task_info *task);

void service_record_deal_error_log_ex1(FDIRBinlogRecord *record,
        const int result, const bool is_error, const char *filename,
        const int line_no, struct fast_task_info *task);

#define service_record_deal_error_log_ex(record, result, is_error, task) \
    service_record_deal_error_log_ex1(record, result, is_error, \
            __FILE__, __LINE__, task)

#define service_record_deal_error_log(record, result, is_error) \
    service_record_deal_error_log_ex(record, result, is_error, NULL)

static inline int service_sys_lock_release(struct fast_task_info *task,
        const bool need_check)
{
    int result;

    if (need_check && __sync_add_and_fetch(&task->canceled, 0)) {
        logWarning("file: "__FILE__", line: %d, "
                "task: %p, already canceled!",
                __LINE__, task);
        return ECANCELED;
    }

    if (SYS_LOCK_TASK != NULL) {
        result = inode_index_sys_lock_release(SYS_LOCK_TASK);
        SYS_LOCK_TASK = NULL;
        return result;
    } else {
        return ENOENT;
    }
}

#ifdef __cplusplus
}
#endif

#endif

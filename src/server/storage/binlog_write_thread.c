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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_func.h"
#include "binlog_write_thread.h"

#define FIELD_TMP_FILENAME  ".field.tmp"
#define FIELD_REDO_FILENAME  "field.redo"
#define SPACE_TMP_FILENAME  ".space.tmp"
#define SPACE_REDO_FILENAME  "space.redo"

static int write_redo_logs(struct fc_queue_info *qinfo)
{
    int result;
    FDIRInodeUpdateRecord *record;

    if ((result=fc_safe_write_file_open(&BINLOG_WRITE_THREAD_CTX.
                    field_redo)) != 0)
    {
        return result;
    }
    if ((result=fc_safe_write_file_open(&BINLOG_WRITE_THREAD_CTX.
                    space_redo)) != 0)
    {
        return result;
    }

    record = (FDIRInodeUpdateRecord *)qinfo->head;
    do {

    } while ((record=record->next) != NULL);

    return 0;
}

static int deal_records(struct fc_queue_info *qinfo)
{
    int result;

    if ((result=write_redo_logs(qinfo)) != 0) {
        return result;
    }

    fc_queue_free_chain(&BINLOG_WRITE_THREAD_CTX.queue,
            &UPDATE_RECORD_ALLOCATOR, qinfo);
    return 0;
}

static void *binlog_write_thread_func(void *arg)
{
    struct fc_queue_info qinfo;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "SE-binlog-write");
#endif

    while (SF_G_CONTINUE_FLAG) {
        fc_queue_try_pop_to_queue(&BINLOG_WRITE_THREAD_CTX.queue, &qinfo);
        if (qinfo.head != NULL) {
            if (deal_records(&qinfo) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "deal notify events fail, "
                        "program exit!", __LINE__);
                sf_terminate_myself();
            }
        }
    }

    return NULL;
}

int binlog_write_thread_init()
{
    int result;
    pthread_t tid;

    if ((result=fc_safe_write_file_init(&BINLOG_WRITE_THREAD_CTX.
                    field_redo, STORAGE_PATH_STR, FIELD_REDO_FILENAME,
                    FIELD_TMP_FILENAME)) != 0)
    {
        return result;
    }

    if ((result=fc_safe_write_file_init(&BINLOG_WRITE_THREAD_CTX.
                    space_redo, STORAGE_PATH_STR, SPACE_REDO_FILENAME,
                    SPACE_TMP_FILENAME)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&BINLOG_WRITE_THREAD_CTX.queue, (long)
                    (&((FDIRInodeUpdateRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, binlog_write_thread_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void binlog_write_thread_destroy()
{
}

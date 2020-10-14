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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <limits.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "server_global.h"
#include "inode_generator.h"

#define INODE_SN_FILENAME  ".inode.sn"

#define GET_INODE_SN_FILENAME(filename, size) \
    snprintf(filename, size, "%s/%s", DATA_PATH_STR, INODE_SN_FILENAME)

#define write_to_inode_sn_file()  write_to_inode_sn_file_func(NULL)

static int write_to_inode_sn_file_func(void *args)
{
    static volatile int64_t last_sn = -1;
    char filename[PATH_MAX];
    char buff[256];
    int len;

    if (CURRENT_INODE_SN == last_sn) {
        return 0;
    }

    last_sn = __sync_add_and_fetch(&CURRENT_INODE_SN, 0);
    len = sprintf(buff, "%"PRId64, last_sn);
    GET_INODE_SN_FILENAME(filename, sizeof(filename));
    return safeWriteToFile(filename, buff, len);
}

static int setup_inode_sn_flush_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 1, write_to_inode_sn_file_func, NULL);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

int inode_generator_init()
{
    char filename[PATH_MAX];
    int result;

    GET_INODE_SN_FILENAME(filename, sizeof(filename));
    if (access(filename, F_OK) == 0) {
        char content[32];
        char *endptr;
        int64_t file_size;

        file_size = sizeof(content);
        if ((result=getFileContentEx(filename, content, 0, &file_size)) != 0) {
            return result;
        }
        endptr = NULL;
        CURRENT_INODE_SN = strtoll(content, &endptr, 10);
        if (!(endptr == NULL || *endptr == '\0')) {
            logError("file: "__FILE__", line: %d, "
                    "inode sn filename: %s, invalid inode sn: %s",
                    __LINE__, filename, content);
            return EINVAL;
        }
    } else {
        CURRENT_INODE_SN = 0;
    }

    INODE_CLUSTER_PART = ((int64_t)CLUSTER_ID) << (63 - FDIR_CLUSTER_ID_BITS);
	return setup_inode_sn_flush_task();
}

void inode_generator_destroy()
{
    write_to_inode_sn_file();
}

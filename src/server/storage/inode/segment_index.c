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
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "segment_index.h"

typedef struct inode_segment_index_info {
    struct {
        volatile uint64_t first;
        volatile uint64_t last;
    } inodes;
    int count;
    int file_id;
    pthread_mutex_t lock;
} InodeSegmentIndexInfo;

typedef struct inode_segment_index_array {
    InodeSegmentIndexInfo *indexes;
    int count;
    int alloc;
} InodeSegmentIndexArray;

typedef struct inode_segment_index_context {
    volatile InodeSegmentIndexArray *si_array;
} InodeSegmentIndexContext;

static InodeSegmentIndexContext segment_index_ctx;

int inode_segment_index_init()
{
    return 0;
}

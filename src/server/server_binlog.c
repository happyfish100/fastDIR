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
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "binlog/binlog_pack.h"
#include "binlog/binlog_producer.h"
#include "binlog/binlog_local_consumer.h"
#include "server_global.h"
#include "server_binlog.h"

int server_binlog_init()
{
    int result;

    if ((result=binlog_pack_init()) != 0) {
        return result;
    }

    if ((result=binlog_dump_load_from_mark_file()) != 0) {
        return result;
    }

    if ((result=binlog_local_consumer_init()) != 0) {
        return result;
    }

	return 0;
}

void server_binlog_destroy()
{
    binlog_local_consumer_destroy();
}
 
void server_binlog_terminate()
{
    binlog_local_consumer_terminate();
}

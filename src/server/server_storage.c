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

#include "server_global.h"
#include "server_storage.h"

int server_storage_init()
{
    int result;

    if ((result=dentry_serializer_init()) != 0) {
        return result;
    }

    if ((result=dentry_lru_init()) != 0) {
        return result;
    }

    if ((result=dentry_loader_init()) != 0) {
        return result;
    }

    if ((result=change_notify_init()) != 0) {
        return result;
    }

    if ((result=STORAGE_ENGINE_START_API()) != 0) {
        return result;
    }

    if ((result=event_dealer_init()) != 0) {
        return result;
    }

    return 0;
}

void server_storage_destroy()
{
}
 
void server_storage_terminate()
{
}

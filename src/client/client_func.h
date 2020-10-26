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


#ifndef _FDIR_CLIENT_FUNC_H
#define _FDIR_CLIENT_FUNC_H

#include "fdir_global.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fdir_client_load_from_file(filename) \
    fdir_client_load_from_file_ex((&g_fdir_client_vars.client_ctx), \
            filename, NULL)

#define fdir_client_init(filename, conn_manager)  \
    fdir_client_init_ex((&g_fdir_client_vars.client_ctx),  \
            filename, NULL, conn_manager)

#define fdir_client_simple_init(filename) \
    fdir_client_simple_init_ex((&g_fdir_client_vars.client_ctx), \
            filename, NULL)

#define fdir_client_pooled_init(filename, max_count_per_entry, max_idle_time) \
    fdir_client_pooled_init_ex(&g_fdir_client_vars.client_ctx, filename,  \
            NULL, max_count_per_entry, max_idle_time)

#define fdir_client_clone(client_ctx) \
    fdir_client_clone_ex(client_ctx, &g_fdir_client_vars.client_ctx)

#define fdir_client_destroy() \
    fdir_client_destroy_ex((&g_fdir_client_vars.client_ctx))


int fdir_client_load_from_file_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx);

/**
* client initial from config file
* params:
*       client_ctx: the client context
*       config_filename: the client config filename
*       section_name: the section name, NULL or empty for global section
* return: 0 success, != 0 fail, return the error code
**/
static inline int fdir_client_load_from_file_ex(FDIRClientContext *client_ctx,
        const char *config_filename, const char *section_name)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fdir_client_load_from_file_ex1(client_ctx, &ini_ctx);
}

int fdir_client_init_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx, const FDIRConnectionManager *conn_manager);

static inline int fdir_client_init_ex(FDIRClientContext *client_ctx,
        const char *config_filename, const char *section_name,
        const FDIRConnectionManager *conn_manager)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fdir_client_init_ex1(client_ctx, &ini_ctx, conn_manager);
}

int fdir_client_simple_init_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx);

static inline int fdir_client_simple_init_ex(FDIRClientContext *client_ctx,
        const char *config_filename, const char *section_name)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fdir_client_simple_init_ex1(client_ctx, &ini_ctx);
}

int fdir_client_pooled_init_ex1(FDIRClientContext *client_ctx,
        IniFullContext *ini_ctx, const int max_count_per_entry,
        const int max_idle_time);

static inline int fdir_client_pooled_init_ex(FDIRClientContext *client_ctx,
        const char *config_filename, const char *section_name,
        const int max_count_per_entry, const int max_idle_time)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fdir_client_pooled_init_ex1(client_ctx, &ini_ctx,
            max_count_per_entry, max_idle_time);
}

static inline void fdir_client_clone_ex(FDIRClientContext *dest_ctx,
        const FDIRClientContext *src_ctx)
{
    *dest_ctx = *src_ctx;
    dest_ctx->cloned = true;
}

/**
* client destroy function
* params:
*       client_ctx: tracker group
* return: none
**/
void fdir_client_destroy_ex(FDIRClientContext *client_ctx);


int fdir_alloc_group_servers(FDIRServerGroup *server_group,
        const int alloc_size);

void fdir_client_log_config(FDIRClientContext *client_ctx);

#ifdef __cplusplus
}
#endif

#endif

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

#include "client_global.h"
#include "fastcfs/auth/fcfs_auth_client.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fdir_client_clone(client_ctx) \
    fdir_client_clone_ex(client_ctx, &g_fdir_client_vars.client_ctx)

#define fdir_client_destroy() \
    fdir_client_destroy_ex((&g_fdir_client_vars.client_ctx))

#define fdir_client_log_config(client_ctx) \
    fdir_client_log_config_ex(client_ctx, NULL)

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
        IniFullContext *ini_ctx, const SFConnectionManager *cm);

static inline int fdir_client_init_ex(FDIRClientContext *client_ctx,
        const char *config_filename, const char *section_name,
        const SFConnectionManager *cm)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fdir_client_init_ex1(client_ctx, &ini_ctx, cm);
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
        const int max_idle_time, const bool bg_thread_enabled);

static inline int fdir_client_pooled_init_ex(FDIRClientContext *client_ctx,
        const char *config_filename, const char *section_name,
        const int max_count_per_entry, const int max_idle_time,
        const bool bg_thread_enabled)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fdir_client_pooled_init_ex1(client_ctx, &ini_ctx,
            max_count_per_entry, max_idle_time, bg_thread_enabled);
}

static inline int fdir_client_load_from_file(const char *config_filename)
{
    const char *section_name = NULL;

    fcfs_auth_client_init_full_ctx(&g_fdir_client_vars.client_ctx.auth);
    return fdir_client_load_from_file_ex(&g_fdir_client_vars.
            client_ctx, config_filename, section_name);
}

static inline int fdir_client_init(const char *config_filename,
        const SFConnectionManager *cm)
{
    const char *section_name = NULL;

    fcfs_auth_client_init_full_ctx(&g_fdir_client_vars.client_ctx.auth);
    return fdir_client_init_ex(&g_fdir_client_vars.client_ctx,
            config_filename, section_name, cm);
}

static inline int fdir_client_simple_init(const char *config_filename)
{
    const char *section_name = NULL;

    fcfs_auth_client_init_full_ctx(&g_fdir_client_vars.client_ctx.auth);
    return fdir_client_simple_init_ex(&g_fdir_client_vars.client_ctx,
            config_filename, section_name);
}

static inline int fdir_client_pooled_init(const char *config_filename,
        const int max_count_per_entry, const int max_idle_time)
{
    const char *section_name = NULL;
    const bool bg_thread_enabled = true;

    fcfs_auth_client_init_full_ctx(&g_fdir_client_vars.client_ctx.auth);
    return fdir_client_pooled_init_ex(&g_fdir_client_vars.client_ctx,
            config_filename, section_name, max_count_per_entry,
            max_idle_time, bg_thread_enabled);
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

void fdir_client_log_config_ex(FDIRClientContext *client_ctx,
        const char *extra_config);

#define fdir_client_auth_session_create1_ex(client_ctx, poolname, publish) \
    fcfs_auth_client_session_create_ex(&(client_ctx)->auth, poolname, publish)

#define fdir_client_auth_session_create1(poolname, publish)  \
    fdir_client_auth_session_create1_ex(&g_fdir_client_vars. \
            client_ctx, poolname, publish)

#define fdir_client_auth_session_create_ex(client_ctx, publish) \
    fcfs_auth_client_session_create(&(client_ctx)->auth, publish)

#define fdir_client_auth_session_create(publish) \
    fdir_client_auth_session_create_ex(  \
            &g_fdir_client_vars.client_ctx, publish)

#ifdef __cplusplus
}
#endif

#endif

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
#include "server_global.h"
#include "dentry.h"
#include "db/dentry_loader.h"
#include "data_dumper.h"

#ifdef FDIR_DUMP_DATA_FOR_DEBUG

typedef struct {
    char filename[PATH_MAX];
    FILE *fp;
    int64_t dentry_count;
} DataDumperContext;

static int output_xattr(DataDumperContext *dump_ctx,
        FDIRServerDentry *dentry)
{
    int result;
    key_value_pair_t *kv;
    key_value_pair_t *end;

    if (STORAGE_ENABLED) {
        if ((result=dentry_load_xattr(dentry->context->
                        thread_ctx, dentry)) != 0)
        {
            return result;
        }
    }

    if (dentry->kv_array != NULL) {
        fprintf(dump_ctx->fp, "\nxattrs:\n");
        end = dentry->kv_array->elts + dentry->kv_array->count;
        for (kv=dentry->kv_array->elts; kv<end; kv++) {
            fprintf(dump_ctx->fp, "%d. %.*s=>%.*s\n",
                    (int)(kv - dentry->kv_array->elts),
                    kv->key.len, kv->key.str,
                    kv->value.len, kv->value.str);
        }
    }

    return 0;
}

static int output_dentry(DataDumperContext *dump_ctx,
        FDIRServerDentry *dentry)
{
    int result;

    fprintf(dump_ctx->fp, "\n[%"PRId64"] id=%"PRId64" pt=%"PRId64" "
            "nm=%.*s", ++dump_ctx->dentry_count, dentry->inode,
            (dentry->parent != NULL ? dentry->parent->inode : 0),
            dentry->name.len, dentry->name.str);

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        fprintf(dump_ctx->fp, " si=%"PRId64, dentry->src_dentry->inode);
    } else if (S_ISLNK(dentry->stat.mode)) {
        fprintf(dump_ctx->fp, " ln=%.*s", dentry->link.len, dentry->link.str);
    }

    fprintf(dump_ctx->fp, " md=%d ui=%d gi=%d bt=%u at=%u ct=%u mt=%u "
            "nl=%d sz=%"PRId64" ac=%"PRId64" se=%"PRId64"\n",
            dentry->stat.mode, dentry->stat.uid, dentry->stat.gid,
            dentry->stat.btime, dentry->stat.atime, dentry->stat.ctime,
            dentry->stat.mtime, dentry->stat.nlink, dentry->stat.size,
            dentry->stat.alloc, dentry->stat.space_end);

    if ((result=output_xattr(dump_ctx, dentry)) != 0) {
        return result;
    }

    return 0;
}

static void output_child_list(DataDumperContext *dump_ctx,
        FDIRServerDentry *dentry)
{
    FDIRServerDentry *current;
    UniqSkiplistIterator iterator;
    int count;

    fprintf(dump_ctx->fp, "\nchilren:\n");
    count = 0;
    uniq_skiplist_iterator(dentry->children, &iterator);
    while ((current=uniq_skiplist_next(&iterator)) != NULL) {
        fprintf(dump_ctx->fp, "%d. %"PRId64" %.*s\n", ++count,
                current->inode, current->name.len, current->name.str);
    }
}

static int dentry_dump(DataDumperContext *dump_ctx, FDIRServerDentry *dentry)
{
    int result;
    FDIRServerDentry *current;
    UniqSkiplistIterator iterator;

    if (STORAGE_ENABLED) {
        if ((result=dentry_check_load(dentry->context->
                        thread_ctx, dentry)) != 0)
        {
            return result;
        }
    }

    if ((result=output_dentry(dump_ctx, dentry)) != 0) {
        return result;
    }

    if (!S_ISDIR(dentry->stat.mode)) {
        return 0;
    }

    output_child_list(dump_ctx, dentry);

    uniq_skiplist_iterator(dentry->children, &iterator);
    while ((current=uniq_skiplist_next(&iterator)) != NULL) {
        if ((result=dentry_dump(dump_ctx, current)) != 0) {
            return result;
        }
    }

    if (STORAGE_ENABLED && dentry->context->thread_ctx->
            lru_ctx.target_reclaims > 0)
    {
        FDIRDataThreadContext *thread_ctx;
        int64_t target_reclaims;

        thread_ctx = dentry->context->thread_ctx;
        if ((target_reclaims=thread_ctx->lru_ctx.target_reclaims) > 0) {
            thread_ctx->lru_ctx.target_reclaims = 0;
            dentry_lru_eliminate(thread_ctx, target_reclaims);
        }
    }

    return 0;
}

static int dump_namespace(FDIRNamespaceEntry *ns_entry)
{
    int result;
    DataDumperContext dump_ctx;

    dump_ctx.dentry_count = 0;
    snprintf(dump_ctx.filename, sizeof(dump_ctx.filename),
            "%s/dump/%.*s.dat", DATA_PATH_STR,
            ns_entry->name.len, ns_entry->name.str);
    if ((dump_ctx.fp=fopen(dump_ctx.filename, "wb")) == NULL) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "open file %s to write fail, errno: %d, error info: %s",
                __LINE__, dump_ctx.filename, result, STRERROR(result));
        return result;
    }

    if (ns_entry->current.root.ptr != NULL) {
        result = dentry_dump(&dump_ctx, ns_entry->current.root.ptr);
    } else {
        result = 0;
    }

    fclose(dump_ctx.fp);
    return result;
}

static void sigUsr1Handler(int sig)
{
    if (data_thread_set_dump_flag() == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "set dump flag successfully", __LINE__);
    }
}

static int setup_dump_data_signal_handler()
{
    struct sigaction act;

    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);
    act.sa_handler = sigUsr1Handler;
    if(sigaction(SIGUSR1, &act, NULL) < 0) {
        logCrit("file: "__FILE__", line: %d, "
                "call sigaction fail, errno: %d, error info: %s",
                __LINE__, errno, strerror(errno));
        return errno;
    }

    return 0;
}

int server_dump_init()
{
    int result;
    char path[PATH_MAX];

    snprintf(path, sizeof(path), "%s/dump", DATA_PATH_STR);
    if ((result=fc_check_mkdir(path, 0755)) != 0) {
        return result;
    }

    return setup_dump_data_signal_handler();
}

int server_dump_data(struct fdir_data_thread_context *thread_ctx)
{
    const FDIRNamespacePtrArray *ns_parray;
    FDIRNamespaceEntry **ns_entry;
    FDIRNamespaceEntry **ns_end;
    int result;
    int old_status;
    int64_t time_used_ms;
    char buff[16];

    old_status = FC_ATOMIC_GET(DATA_DUMP_STATUS);
    if (old_status != FDIR_DATA_DUMP_STATUS_DUMPING) {
        if (__sync_bool_compare_and_swap(&DATA_DUMP_STATUS,
                    old_status, FDIR_DATA_DUMP_STATUS_DUMPING))
        {
            DUMP_START_TIME_MS = get_current_time_ms();
            logInfo("file: "__FILE__", line: %d, "
                    "begin dump data ...", __LINE__);
        }
    }

    result = 0;
    ns_parray = fdir_namespace_get_all();
    ns_end = ns_parray->namespaces + ns_parray->count;
    for (ns_entry=ns_parray->namespaces; ns_entry<ns_end; ns_entry++) {
        if ((*ns_entry)->thread_ctx != thread_ctx) {
            continue;
        }

        if ((result=dump_namespace(*ns_entry)) != 0) {
            break;
        }
    }

    if (FC_ATOMIC_DEC(DATA_DUMP_THREADS) == 0) {
        __sync_bool_compare_and_swap(&DATA_DUMP_STATUS,
                FDIR_DATA_DUMP_STATUS_DUMPING,
                FDIR_DATA_DUMP_STATUS_DONE);

        time_used_ms = get_current_time_ms() - DUMP_START_TIME_MS;
        logInfo("file: "__FILE__", line: %d, "
                "dump data done, time used: %s ms", __LINE__,
                long_to_comma_str(time_used_ms, buff));
    }

    return result;
}

#endif

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

typedef struct {
    const FDIRNamespaceEntry *ns_entry;
    char path[PATH_MAX];
    char filename[PATH_MAX];
    FILE *fp;
    int64_t dentry_count;
} DataDumperContext;

static DataDumperContext dump_ctx;

static int output_xattr(FDIRServerDentry *dentry)
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
        fprintf(dump_ctx.fp, "\nxattrs:\n");
        end = dentry->kv_array->elts + dentry->kv_array->count;
        for (kv=dentry->kv_array->elts; kv<end; kv++) {
            fprintf(dump_ctx.fp, "%d. %.*s=>%.*s\n",
                    (int)(kv - dentry->kv_array->elts),
                    kv->key.len, kv->key.str,
                    kv->value.len, kv->value.str);
        }
    }

    return 0;
}

static int output_dentry(FDIRServerDentry *dentry)
{
    int result;

    fprintf(dump_ctx.fp, "\n[%"PRId64"] id=%"PRId64" nm=%.*s rr=%d",
            ++dump_ctx.dentry_count, dentry->inode,
            dentry->name.len, dentry->name.str, dentry->reffer_count);

    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        fprintf(dump_ctx.fp, " si=%"PRId64, dentry->src_dentry->inode);
    } else if (S_ISLNK(dentry->stat.mode)) {
        fprintf(dump_ctx.fp, " ln=%.*s", dentry->link.len, dentry->link.str);
    }

    fprintf(dump_ctx.fp, " md=%d ui=%d gi=%d bt=%u at=%u ct=%u mt=%u "
            "nl=%d sz=%"PRId64" ac=%"PRId64" se=%"PRId64"\n",
            dentry->stat.mode, dentry->stat.uid, dentry->stat.gid,
            dentry->stat.btime, dentry->stat.atime, dentry->stat.ctime,
            dentry->stat.mtime, dentry->stat.nlink, dentry->stat.size,
            dentry->stat.alloc, dentry->stat.space_end);

    if ((result=output_xattr(dentry)) != 0) {
        return result;
    }

    return 0;
}

static void output_child_list(FDIRServerDentry *dentry)
{
    FDIRServerDentry *current;
    UniqSkiplistIterator iterator;
    int count;

    fprintf(dump_ctx.fp, "\nchilren:\n");
    count = 0;
    uniq_skiplist_iterator(dentry->children, &iterator);
    while ((current=uniq_skiplist_next(&iterator)) != NULL) {
        fprintf(dump_ctx.fp, "%d. %"PRId64" %.*s\n", ++count,
                current->inode, current->name.len, current->name.str);
    }
}

static int dentry_dump(FDIRServerDentry *dentry)
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

    if ((result=output_dentry(dentry)) != 0) {
        return result;
    }

    if (!S_ISDIR(dentry->stat.mode)) {
        return 0;
    }

    output_child_list(dentry);

    uniq_skiplist_iterator(dentry->children, &iterator);
    while ((current=uniq_skiplist_next(&iterator)) != NULL) {
        if ((result=dentry_dump(current)) != 0) {
            return result;
        }
    }

    return 0;
}

static int namespace_dump(FDIRNamespaceEntry *ns_entry)
{
    int result;

    dump_ctx.ns_entry = ns_entry;
    dump_ctx.dentry_count = 0;
    snprintf(dump_ctx.filename, sizeof(dump_ctx.filename), "%s/%.*s.dat",
            dump_ctx.path, ns_entry->name.len, ns_entry->name.str);
    if ((dump_ctx.fp=fopen(dump_ctx.filename, "wb")) == NULL) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "open file %s to write fail, errno: %d, error info: %s",
                __LINE__, dump_ctx.filename, result, STRERROR(result));
        return result;
    }

    if (ns_entry->current.root.ptr != NULL) {
        result = dentry_dump(ns_entry->current.root.ptr);
    } else {
        result = 0;
    }

    fclose(dump_ctx.fp);
    return result;
}

int server_dump_data()
{
    const FDIRNamespacePtrArray *ns_parray;
    FDIRNamespaceEntry **ns_entry;
    FDIRNamespaceEntry **ns_end;
    int result;

    snprintf(dump_ctx.path, sizeof(dump_ctx.path), "%s/dump", DATA_PATH_STR);
    if ((result=fc_check_mkdir(dump_ctx.path, 0755)) != 0) {
        return result;
    }

    ns_parray = fdir_namespace_get_all();
    ns_end = ns_parray->namespaces + ns_parray->count;
    for (ns_entry=ns_parray->namespaces; ns_entry<ns_end; ns_entry++) {
        if ((result=namespace_dump(*ns_entry)) != 0) {
            break;
        }
    }

    return result;
}

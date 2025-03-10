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

#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "dentry.h"
#include "inode_index.h"
#include "service_handler.h"
#include "data_dumper.h"
#include "server_binlog.h"
#include "db/change_notify.h"
#include "db/dentry_serializer.h"
#include "db/dentry_loader.h"
#include "db/dentry_lru.h"
#include "data_thread.h"

#define DATA_THREAD_RUNNING_COUNT g_data_thread_vars.running_count

FDIRDataThreadVariables g_data_thread_vars = {{NULL, 0}, 0, 0};
static void *data_thread_func(void *arg);

void data_thread_sum_counters(FDIRDentryCounters *counters)
{
    FDIRDataThreadContext *context;

    counters->ns = 0;
    counters->dir = 0;
    counters->file = 0;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<DATA_THREAD_END; context++)
    {
        counters->ns += context->dentry_context.counters.ns;
        counters->dir += context->dentry_context.counters.dir;
        counters->file += context->dentry_context.counters.file;
    }
}

void data_thread_sum_reclaim_counters(FDIRDentryReclaimCounters *counters)
{
    FDIRDataThreadContext *context;

    counters->total_count = 0;
    counters->success_count = 0;
    counters->reclaimed_count = 0;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<DATA_THREAD_END; context++)
    {
        counters->total_count += context->lru_ctx.counters.total_count;
        counters->success_count += context->lru_ctx.counters.success_count;
        counters->reclaimed_count += context->lru_ctx.counters.reclaimed_count;
    }
}

int server_add_to_immediate_free_queue_ex(ServerFreeContext *free_ctx,
        void *ctx, void *ptr, server_free_func_ex free_func_ex)
{
    ServerImmediateFreeNode *node;

    node = fast_mblock_alloc_object(&free_ctx->allocator);
    if (node == NULL) {
        return ENOMEM;
    }

    node->free_func = NULL;
    node->free_func_ex = free_func_ex;
    node->ctx = ctx;
    node->ptr = ptr;
    __sync_add_and_fetch(&free_ctx->immediate.waiting_count, 1);
    fc_queue_push_silence(&free_ctx->immediate.queue, node);
    return 0;
}

int server_add_to_immediate_free_queue(ServerFreeContext *free_ctx,
        void *ptr, server_free_func free_func)
{
    ServerImmediateFreeNode *node;

    node = fast_mblock_alloc_object(&free_ctx->allocator);
    if (node == NULL) {
        return ENOMEM;
    }

    node->free_func = free_func;
    node->free_func_ex = NULL;
    node->ctx = NULL;
    node->ptr = ptr;
    __sync_add_and_fetch(&free_ctx->immediate.waiting_count, 1);
    fc_queue_push_silence(&free_ctx->immediate.queue, node);
    return 0;
}

static void deal_immediate_free_queue(FDIRDataThreadContext *thread_ctx)
{
    struct fc_queue_info qinfo;
    ServerImmediateFreeNode *node;
    int count;

    fc_queue_try_pop_to_queue(&thread_ctx->free_context.
            immediate.queue, &qinfo);
    if (qinfo.head == NULL) {
        return;
    }

    count = 0;
    node = qinfo.head;
    do {
        if (node->free_func != NULL) {
            node->free_func(node->ptr);
        } else {
            node->free_func_ex(node->ctx, node->ptr);
        }

        ++count;
        node = node->next;
    } while (node != NULL);

    fc_queue_free_chain(&thread_ctx->free_context.immediate.queue,
            &thread_ctx->free_context.allocator, &qinfo);

    /*
    logInfo("file: "__FILE__", line: %d, "
            "free count: %d, free_context.immediate.waiting_count: %d",
            __LINE__, count,  __sync_add_and_fetch(&thread_ctx->
                free_context.immediate.waiting_count, 0));
                */

    __sync_sub_and_fetch(&thread_ctx->free_context.
            immediate.waiting_count, count);
}

static int event_alloc_init_func(void *element, void *args)
{
    ((FDIRChangeNotifyEvent *)element)->thread_ctx = args;
    return 0;
}

static int init_thread_ctx(FDIRDataThreadContext *context)
{
    int result;
    if ((result=dentry_init_context(context)) != 0) {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&context->free_context.allocator,
                    "delay_free_node", sizeof(ServerImmediateFreeNode),
                    16 * 1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&context->free_context.immediate.queue,
                    (long)(&((ServerImmediateFreeNode *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&context->queue, (long)
                    (&((FDIRBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((result=fast_mblock_init_ex1(&context->event.allocator,
                        "chg-event", sizeof(FDIRChangeNotifyEvent),
                        EVENT_ALLOC_ELEMENTS_ONCE, EVENT_ALLOC_ELEMENTS_LIMIT,
                        event_alloc_init_func, context, true)) != 0)
        {
            return result;
        }
        fast_mblock_set_need_wait(&context->event.allocator,
                true, (bool *)&SF_G_CONTINUE_FLAG);

        if ((result=init_db_fetch_context(&context->db_fetch_ctx)) != 0) {
            return result;
        }

        FC_INIT_LIST_HEAD(&context->lru_ctx.head);
    }

    return 0;
}

static int init_data_thread_array()
{
    int result;
    int bytes;
    FDIRDataThreadContext *context;

    bytes = sizeof(FDIRDataThreadContext) * DATA_THREAD_COUNT;
    g_data_thread_vars.thread_array.contexts =
        (FDIRDataThreadContext *)fc_malloc(bytes);
    if (g_data_thread_vars.thread_array.contexts == NULL) {
        return ENOMEM;
    }
    memset(g_data_thread_vars.thread_array.contexts, 0, bytes);

    DATA_THREAD_END = g_data_thread_vars.thread_array.
        contexts + DATA_THREAD_COUNT;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<DATA_THREAD_END; context++)
    {
        context->index = context - g_data_thread_vars.thread_array.contexts;
        if ((result=init_thread_ctx(context)) != 0) {
            return result;
        }
    }
    g_data_thread_vars.thread_array.count = DATA_THREAD_COUNT;
    return 0;
}

int data_thread_init()
{
    int result;
    int count;
    int limit;

    if (STORAGE_ENABLED) {
        if (BATCH_STORE_ON_MODIFIES < 1000) {
            EVENT_ALLOC_ELEMENTS_ONCE = 1 * 1024;
            limit = 8 * 1024;
        } else if (BATCH_STORE_ON_MODIFIES < 10 * 1000) {
            EVENT_ALLOC_ELEMENTS_ONCE = 2 * 1024;
            limit = BATCH_STORE_ON_MODIFIES * 4;
        } else if (BATCH_STORE_ON_MODIFIES < 100 * 1000) {
            EVENT_ALLOC_ELEMENTS_ONCE = 4 * 1024;
            limit = BATCH_STORE_ON_MODIFIES * 2;
        } else {
            EVENT_ALLOC_ELEMENTS_ONCE = 8 * 1024;
            limit = BATCH_STORE_ON_MODIFIES;
        }

        EVENT_ALLOC_ELEMENTS_LIMIT = EVENT_ALLOC_ELEMENTS_ONCE;
        while (EVENT_ALLOC_ELEMENTS_LIMIT < limit) {
            EVENT_ALLOC_ELEMENTS_LIMIT *= 2;
        }
    }

    if ((result=init_data_thread_array()) != 0) {
        return result;
    }

    g_data_thread_vars.error_mode = FDIR_DATA_ERROR_MODE_LOOSE;
    count = g_data_thread_vars.thread_array.count;
    if ((result=create_work_threads_ex(&count, data_thread_func,
            g_data_thread_vars.thread_array.contexts,
            sizeof(FDIRDataThreadContext), NULL,
            SF_G_THREAD_STACK_SIZE)) == 0)
    {
        count = 0;
        while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) <
                g_data_thread_vars.thread_array.count && count++ < 100)
        {
            fc_sleep_ms(1);
        }
    }
    return result;
}

void data_thread_destroy()
{
    if (g_data_thread_vars.thread_array.contexts != NULL) {
        FDIRDataThreadContext *context;

        for (context=g_data_thread_vars.thread_array.contexts;
                context<DATA_THREAD_END; context++)
        {
            fc_queue_destroy(&context->queue);
        }
        free(g_data_thread_vars.thread_array.contexts);
        g_data_thread_vars.thread_array.contexts = NULL;
    }
}

void data_thread_terminate()
{
    FDIRDataThreadContext *context;
    int count;

    for (context=g_data_thread_vars.thread_array.contexts;
            context<DATA_THREAD_END; context++)
    {
        fc_queue_terminate(&context->queue);
    }

    count = 0;
    while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) != 0 &&
            count++ < 100)
    {
        fc_sleep_ms(1);
    }
}

static inline int check_parent(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    if (record->me.pname.parent_inode == 0) {
        record->me.parent = NULL;
        return 0;
    }

    return inode_index_get_dentry_ex(thread_ctx, record->me.
            pname.parent_inode, &record->me.parent, true);
}

static int find_or_check_parent(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    bool is_create;

    if (record->dentry_type != fdir_dentry_type_fullname) {
        return check_parent(thread_ctx, record);
    }

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "ns: %.*s, path: %.*s", __LINE__, __FUNCTION__,
            record->me.fullname.ns.len, record->me.fullname.ns.str,
            record->me.fullname.path.len, record->me.fullname.path.str);
            */

    is_create = (record->operation == BINLOG_OP_CREATE_DENTRY_INT);
    if ((result=dentry_find_parent(&record->me.fullname, &record->oper,
                    &record->me.parent, &record->me.pname.name)) != 0)
    {
        if (!(result == ENOENT && is_create)) {
            return result;
        }
        if (!FDIR_IS_ROOT_PATH(record->me.fullname.path)) {
            return result;
        }
    } else if (is_create && FDIR_IS_ROOT_PATH(record->me.fullname.path)) {
        return EEXIST;
    }

    record->me.pname.parent_inode = (record->me.parent != NULL ?
            record->me.parent->inode : 0);
    record->me.dentry = NULL;
    return service_set_record_pname_info(record,
            (struct fast_task_info *)record->notify.args);
}

static inline int set_pname_by_fullname_ex(FDIRRecordDEntry *entry,
        const FDIRDentryOperator *oper, const bool allow_root_path)
{
    int result;

    if ((result=dentry_find_parent(&entry->fullname, oper,
                    &entry->parent, &entry->pname.name)) != 0)
    {
        return result;
    }

    if (allow_root_path) {
        entry->pname.parent_inode = (entry->parent != NULL ?
                entry->parent->inode : 0);
    } else {
        if (entry->parent == NULL) {
            return EINVAL;
        }
        entry->pname.parent_inode = entry->parent->inode;
    }

    return 0;
}

#define set_pname_by_fullname(entry, oper) \
    set_pname_by_fullname_ex(entry, oper, false)

static inline int set_hdlink_src_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;

    if (record->dentry_type == fdir_dentry_type_fullname) {
        if ((result=dentry_find(&record->hdlink.src.fullname, &record->oper,
                        &record->hdlink.src.dentry)) != 0)
        {
            return result;
        }

        record->hdlink.src.inode = record->hdlink.src.dentry->inode;
    } else {
        if ((result=inode_index_get_dentry(thread_ctx, record->hdlink.
                        src.inode, &record->hdlink.src.dentry)) != 0)
        {
            return result;
        }
    }

    if ((record->flags & FDIR_FLAGS_FOLLOW_SYMLINK) &&
            S_ISLNK(record->hdlink.src.dentry->stat.mode))
    {
        if ((result=dentry_resolve_symlink(&record->hdlink.
                        src.dentry, &record->oper)) != 0)
        {
            return result;
        }
        record->hdlink.src.inode = record->hdlink.src.dentry->inode;
    }

    if (FDIR_IS_DENTRY_HARD_LINK(record->hdlink.src.dentry->stat.mode)) {
        record->hdlink.src.dentry = record->hdlink.src.dentry->src_dentry;
        record->hdlink.src.inode = record->hdlink.src.dentry->inode;
    }

    if (S_ISDIR(record->hdlink.src.dentry->stat.mode)) {
        return EPERM;
    }

    record->stat.mode |= (record->hdlink.src.dentry->stat.mode & S_IFMT);
    return 0;
}

static inline int deal_record_rename_op(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    char *src_name;

    if (record->dentry_type == fdir_dentry_type_fullname) {
        if ((result=set_pname_by_fullname(&record->rename.src,
                        &record->oper)) != 0)
        {
            return result;
        }
        if ((result=set_pname_by_fullname(&record->rename.dest,
                        &record->oper)) != 0)
        {
            return result;
        }

        if ((result=service_set_record_pname_info(record,
                        (struct fast_task_info *)record->notify.args)) != 0)
        {
            return result;
        }
    } else {
        if ((result=inode_index_get_dentry_ex(thread_ctx,
                        record->rename.src.pname.parent_inode,
                        &record->rename.src.parent, true)) != 0)
        {
            return result;
        }

        if ((result=inode_index_get_dentry_ex(thread_ctx,
                        record->rename.dest.pname.parent_inode,
                        &record->rename.dest.parent, true)) != 0)
        {
            return result;
        }
    }

    if (record->dentry_type != fdir_dentry_type_inode) {
        src_name = record->rename.dest.pname.name.str +
            record->rename.dest.pname.name.len;
        memcpy(src_name, record->rename.src.pname.name.str,
                record->rename.src.pname.name.len);
        record->rename.src.pname.name.str = src_name;
    }

    return dentry_rename(thread_ctx, record);
}

static inline int xattr_update_prepare(FDIRDataThreadContext
        *thread_ctx, FDIRBinlogRecord *record)
{
    int result;

    if (record->dentry_type == fdir_dentry_type_inode) {
        if ((result=inode_index_get_dentry(thread_ctx, record->inode,
                        &record->me.dentry)) != 0)
        {
            return result;
        }
    } else {
        if ((result=dentry_find(&record->me.fullname, &record->oper,
                        &record->me.dentry)) != 0)
        {
            return result;
        }

        record->inode = record->me.dentry->inode;
    }

    if ((record->flags & FDIR_FLAGS_FOLLOW_SYMLINK) &&
            S_ISLNK(record->me.dentry->stat.mode))
    {
        if ((result=dentry_resolve_symlink(&record->me.dentry,
                        &record->oper)) != 0)
        {
            return result;
        }
        record->inode = record->me.dentry->inode;
    }

    return 0;
}

static int batch_set_dentry_size(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    FDIRBinlogRecord **pp;
    FDIRBinlogRecord **recend;
    int64_t current_version;
    int result;

    record->parray->counts.success = record->parray->counts.updated = 0;
    recend = record->parray->records + record->parray->counts.total;
    for (pp=record->parray->records; pp<recend; pp++) {
        if ((result=inode_index_check_set_dentry_size(
                        thread_ctx, *pp)) == 0)
        {
            record->parray->counts.success++;
            if ((*pp)->options.flags != 0) {
                record->parray->counts.updated++;
            }
        } else {
            (*pp)->options.flags = 0;
        }
    }

    if (record->parray->counts.success == 0) {
        return ENOENT;
    }
    if (record->parray->counts.updated == 0) {
        return 0;
    }

    record->data_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION,
            record->parray->counts.updated);
    current_version = record->data_version - record->parray->counts.updated;
    for (pp=record->parray->records; pp<recend; pp++) {
        if ((*pp)->options.flags != 0) {
            (*pp)->data_version = ++current_version;
        }
    }

    return 0;
}

static int check_load_children(FDIRServerDentry *parent)
{
    int result;
    int target_count;
    FDIRServerDentry *child;
    id_name_pair_t pair;
    id_name_pair_t *elt;
    UniqSkiplistIterator it;

    if ((parent->db_args->loaded_flags & FDIR_DENTRY_LOADED_FLAGS_CLIST)) {
        return 0;
    }

    if (CHILDREN_CONTAINER == fdir_children_container_skiplist) {
        parent->db_args->children.sl = uniq_skiplist_new(&parent->context->
                db_args.factory, DENTRY_SKIPLIST_INIT_LEVEL_COUNT);
        if (parent->db_args->children.sl == NULL) {
            return ENOMEM;
        }

        uniq_skiplist_iterator(parent->children, &it);
        while ((child=uniq_skiplist_next(&it)) != NULL) {
            if ((elt=fast_mblock_alloc_object(&parent->context->
                            db_args.child_allocator)) == NULL)
            {
                return ENOMEM;
            }

            elt->id = child->inode;
            if ((result=dentry_strdup(parent->context,
                            &elt->name, &child->name)) != 0)
            {
                return result;
            }
            if ((result=uniq_skiplist_insert(parent->db_args->
                            children.sl, elt)) != 0)
            {
                logError("file: "__FILE__", line: %d, inode: %"PRId64", "
                        "insert child {inode: %"PRId64", name: %.*s} to "
                        "skiplist fail, errno: %d, error info: %s", __LINE__,
                        parent->inode, child->inode, child->name.len,
                        child->name.str, result, STRERROR(result));
                return result;
            }
        }
    } else {
        target_count = 0;
        uniq_skiplist_iterator(parent->children, &it);
        while ((child=uniq_skiplist_next(&it)) != NULL) {
            ++target_count;
        }

        if (target_count > 0) {
            parent->db_args->children.sa = id_name_array_allocator_alloc(
                    &ID_NAME_ARRAY_ALLOCATOR_CTX, target_count);
            if (parent->db_args->children.sa == NULL) {
                return ENOMEM;
            }

            uniq_skiplist_iterator(parent->children, &it);
            while ((child=uniq_skiplist_next(&it)) != NULL) {
                pair.id = child->inode;
                if ((result=dentry_strdup(parent->context,
                                &pair.name, &child->name)) != 0)
                {
                    return result;
                }
                if ((result=sorted_array_insert(&ID_NAME_SORTED_ARRAY_CTX,
                                parent->db_args->children.sa->elts, &parent->
                                db_args->children.sa->count, &pair)) != 0)
                {
                    return result;
                }
            }
        }
    }

    parent->db_args->loaded_flags |= FDIR_DENTRY_LOADED_FLAGS_CLIST;
    return 0;
}


#define GENERATE_ADD_TO_PARENT_MESSAGE(msg, dentry, op_type)  \
    if ((dentry)->parent != NULL) {  \
        if ((result=check_load_children((dentry)->parent)) != 0) { \
            return result; \
        }  \
        FDIR_CHANGE_NOTIFY_FILL_MESSAGE(msg, (dentry)->parent, \
                op_type, FDIR_PIECE_FIELD_INDEX_CHILDREN, 0);  \
        if ((dentry)->parent->db_args->add_to_clist) { \
            (msg)->child.id = (dentry)->inode;  \
            if ((result=dentry_strdup((dentry)->context, &(msg)-> \
                            child.name, &(dentry)->name)) != 0)   \
            {  \
                return result; \
            }  \
        } else { \
            (msg)->child.id = -1 * (dentry)->inode; \
            FC_SET_STRING_NULL((msg)->child.name);  \
        }  \
        (msg)++; \
    }

#define GENERATE_REMOVE_FROM_PARENT_MESSAGE(msg, parent, inode)  \
    if (parent != NULL) {  \
        if ((result=check_load_children(parent)) != 0) { \
            return result; \
        }  \
        FDIR_CHANGE_NOTIFY_FILL_MESSAGE(msg, parent, \
                da_binlog_op_type_remove, \
                FDIR_PIECE_FIELD_INDEX_CHILDREN, 0); \
        (msg)->child.id = (parent->db_args->add_to_clist ? inode : -1 * inode);\
        FC_SET_STRING_NULL((msg)->child.name); \
        (msg)++; \
    }

#define GENERATE_REOMVE_DENTRY_MESSAGES(msg, dentry) \
    GENERATE_REMOVE_FROM_PARENT_MESSAGE(msg,    \
            (dentry)->parent, (dentry)->inode); \
    FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(msg, dentry, \
            da_binlog_op_type_remove, FDIR_PIECE_FIELD_INDEX_FOR_REMOVE, \
            ((dentry)->stat.alloc > 0 ? -1 * (dentry)->stat.alloc : 0))

#define GENERATE_DENTRY_MESSAGES(msg, dentry, op_type, inc_alloc) \
    GENERATE_ADD_TO_PARENT_MESSAGE(msg, dentry, da_binlog_op_type_create); \
    FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(msg, dentry, op_type, \
            FDIR_PIECE_FIELD_INDEX_BASIC, inc_alloc)

#define GENERATE_MOVE_DENTRY_MESSAGES(msg, old_parent, dentry)  \
        GENERATE_REMOVE_FROM_PARENT_MESSAGE(msg, old_parent, (dentry)->inode); \
        GENERATE_DENTRY_MESSAGES(msg, dentry, da_binlog_op_type_update, 0)

#define SET_ADD_TO_CLIST_FLAG(parent)  \
    if (parent != NULL) parent->db_args->add_to_clist =  \
       (parent->db_args->children.ptr != NULL)

#define GENERATE_UPDATE_PARENT_MESSAGE(msg, parent) \
    if (parent != NULL) {  \
        FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(    \
            msg, parent, da_binlog_op_type_update,  \
            FDIR_PIECE_FIELD_INDEX_BASIC, 0); \
    }

static inline int generate_affected_messages(FDIRChangeNotifyMessage **msg,
        FDIRBinlogRecord *record)
{
    FDIRAffectedDentry *current;
    FDIRAffectedDentry *end;
    int result;

    end = record->affected.entries + record->affected.count;
    for (current=record->affected.entries; current<end; current++) {
        if (current->op_type == da_binlog_op_type_remove) {
            SET_ADD_TO_CLIST_FLAG(current->dentry->parent);
            GENERATE_REOMVE_DENTRY_MESSAGES(*msg, current->dentry);
            GENERATE_UPDATE_PARENT_MESSAGE(*msg, current->dentry->parent);
        } else {  //update
            FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(*msg,
                    current->dentry, da_binlog_op_type_update,
                    FDIR_PIECE_FIELD_INDEX_BASIC, 0);
            if (current->remove_from_parent) {
                SET_ADD_TO_CLIST_FLAG(current->dentry->parent);
                GENERATE_REMOVE_FROM_PARENT_MESSAGE(*msg, record->me.
                        dentry->parent, record->me.dentry->inode);
                GENERATE_UPDATE_PARENT_MESSAGE(*msg,
                        record->me.dentry->parent);
                record->me.dentry->parent = NULL;   //orphan inode
            }
        }
    }

    return 0;
}

static inline int generate_create_messages(FDIRChangeNotifyMessage **msg,
        FDIRBinlogRecord *record, const int64_t inc_alloc)
{
    int result;

    SET_ADD_TO_CLIST_FLAG(record->me.dentry->parent);
    if (record->affected.count > 0) {
        if ((result=generate_affected_messages(msg, record)) != 0) {
            return result;
        }
    }
    GENERATE_DENTRY_MESSAGES(*msg, record->me.dentry,
            da_binlog_op_type_create, inc_alloc);
    GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->me.dentry->parent);
    return 0;
}

static int generate_rename_messages(FDIRChangeNotifyMessage **msg,
        FDIRBinlogRecord *record)
{
    int result;

    SET_ADD_TO_CLIST_FLAG(record->rename.src.parent);
    if (record->rename.src.parent != record->rename.dest.parent) {
        SET_ADD_TO_CLIST_FLAG(record->rename.dest.parent);
    }

    if ((record->flags & RENAME_EXCHANGE)) {
        if (record->rename.src.parent == record->rename.dest.parent) {
            GENERATE_ADD_TO_PARENT_MESSAGE(*msg, record->rename.src.dentry,
                    da_binlog_op_type_update);
            GENERATE_ADD_TO_PARENT_MESSAGE(*msg, record->rename.dest.dentry,
                    da_binlog_op_type_update);

            FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(*msg, record->
                    rename.src.dentry, da_binlog_op_type_update,
                    FDIR_PIECE_FIELD_INDEX_BASIC, 0);
            FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(*msg, record->
                    rename.dest.dentry, da_binlog_op_type_update,
                    FDIR_PIECE_FIELD_INDEX_BASIC, 0);
            GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->
                    rename.src.parent);
        } else {
            GENERATE_MOVE_DENTRY_MESSAGES(*msg, record->rename.
                    src.parent, record->rename.src.dentry);
            GENERATE_MOVE_DENTRY_MESSAGES(*msg, record->rename.
                    dest.parent, record->rename.dest.dentry);
            GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->
                    rename.src.parent);
            GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->
                    rename.dest.parent);
        }
        return 0;
    }

    if (record->affected.count > 0) {
        if ((result=generate_affected_messages(msg, record)) != 0) {
            return result;
        }
    }

    if (record->rename.src.parent == record->rename.dest.parent) {
        GENERATE_ADD_TO_PARENT_MESSAGE(*msg, record->rename.src.dentry,
                da_binlog_op_type_update);
        FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(*msg, record->
                rename.src.dentry, da_binlog_op_type_update,
                FDIR_PIECE_FIELD_INDEX_BASIC, 0);
        GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->
                rename.src.parent);
    } else {  //parent changed
        GENERATE_MOVE_DENTRY_MESSAGES(*msg, record->rename.
                src.parent, record->rename.src.dentry);
        GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->
                rename.src.parent);
        GENERATE_UPDATE_PARENT_MESSAGE(*msg, record->
                rename.dest.parent);

        record->rename.src.parent->db_args->loaded_count--;
        record->rename.src.dentry->parent->db_args->loaded_count++;
    }

    return 0;
}

static inline int pack_messages(FDIRChangeNotifyEvent *event)
{
    int result;
    FDIRChangeNotifyMessage *msg;
    FDIRChangeNotifyMessage *end;

    end = event->marray.messages + event->marray.count;
    for (msg=event->marray.messages; msg<end; msg++) {
        msg->id = __sync_add_and_fetch(&g_data_thread_vars.
                event.current_id, 1);
        if (msg->op_type == da_binlog_op_type_remove ||
                msg->field_index == FDIR_PIECE_FIELD_INDEX_CHILDREN)
        {
            msg->buffer = NULL;
        } else if ((result=dentry_serializer_pack(msg->dentry,
                        msg->field_index, &msg->buffer)) != 0)
        {
            return result;
        }

        dentry_hold(msg->dentry);
    }

    return 0;
}

static int push_to_db_update_queue(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    FDIRChangeNotifyEvent *event;
    FDIRChangeNotifyMessage *msg;

    event = fast_mblock_alloc_object(&thread_ctx->event.allocator);
    if (event == NULL) {
        return ENOMEM;
    }

    event->version = record->data_version;
    msg = event->marray.messages;

    switch (record->operation) {
        case BINLOG_OP_CREATE_DENTRY_INT:
        case BINLOG_OP_DUMP_DENTRY_INT:
            if ((result=generate_create_messages(&msg, record,
                            record->stat.alloc)) != 0)
            {
                return result;
            }
            if (record->operation == BINLOG_OP_DUMP_DENTRY_INT &&
                    record->me.dentry->kv_array != NULL)
            {
                FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(msg, record->
                        me.dentry, da_binlog_op_type_create,
                        FDIR_PIECE_FIELD_INDEX_XATTR, 0);
            }
            break;
        case BINLOG_OP_UPDATE_DENTRY_INT:
            FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(msg, record->me.dentry,
                    da_binlog_op_type_update, FDIR_PIECE_FIELD_INDEX_BASIC,
                    (record->options.inc_alloc ? record->stat.alloc : 0));
            break;
        case BINLOG_OP_SET_XATTR_INT:
        case BINLOG_OP_REMOVE_XATTR_INT:
            FDIR_CHANGE_NOTIFY_FILL_MSG_AND_INC_PTR(msg, record->
                    me.dentry, da_binlog_op_type_update,
                    FDIR_PIECE_FIELD_INDEX_XATTR, 0);
            break;
        case BINLOG_OP_REMOVE_DENTRY_INT:
            if ((result=generate_affected_messages(&msg, record)) != 0) {
                return result;
            }
            break;
        case BINLOG_OP_RENAME_DENTRY_INT:
            if ((result=generate_rename_messages(&msg, record)) != 0) {
                return result;
            }
            break;
        default:
            break;
    }

    event->marray.count = msg - event->marray.messages;
    if ((result=pack_messages(event)) != 0) {
        return result;
    }

    change_notify_push_to_queue(event);
    return 0;
}

static int push_batch_set_dsize_to_db_update_queue(FDIRDataThreadContext
        *thread_ctx, FDIRBinlogRecord *record)
{
    FDIRBinlogRecord **pp;
    FDIRBinlogRecord **recend;
    int result;

    recend = record->parray->records + record->parray->counts.total;
    for (pp=record->parray->records; pp<recend; pp++) {
        if ((*pp)->data_version > 0) {
            if ((result=push_to_db_update_queue(thread_ctx, *pp)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

static inline int update_dentry_stat(FDIRServerDentry *dentry,
        FDIRBinlogRecord *record)
{
    int result;
    int keep_perms;

    if (record->options.mode) {
        if (IS_DENTRY_OWNER(record->oper.uid, dentry)) {
            /*
            POSIX: If the calling process does not have appropriate privileges,
            and if the group ID of the file does not match the effective group
            ID or one of the supplementary group IDs and if the file is
            a regular file, bit S_ISGID (set-group-ID on execution)
            in the file's mode shall be cleared. */
            if (record->oper.uid != 0 && record->oper.gid != dentry->stat.gid) {
                keep_perms = S_ISUID|S_ISVTX|S_IRWXU|S_IRWXG|S_IRWXO;
            } else {
                keep_perms = ALLPERMS;
            }
        } else {
            if ((record->stat.mode & (S_IRWXU|S_IRWXG|S_IRWXO)) ==
                    (dentry->stat.mode & (S_IRWXU|S_IRWXG|S_IRWXO)) &&
                    (result=dentry_access(dentry, &record->oper, W_OK)) == 0)
            {
                /* non-owner clears the S_ISUID and S_ISGID */
                keep_perms = S_ISVTX|S_IRWXU|S_IRWXG|S_IRWXO;
            } else {
                return EPERM;
            }
        }
        record->stat.mode = (dentry->stat.mode & S_IFMT) |
            (record->stat.mode & keep_perms);
        dentry->stat.mode = record->stat.mode;

        if (record->source == fdir_record_source_master_rpc &&
                FDIR_USE_POSIX_ACL && !record->options.ctime)
        {
            /* successful chmod updates ctime. */
            record->options.ctime = 1;
            record->stat.ctime = record->timestamp;
        }
    }

    if (record->options.uid || record->options.gid) {
        /*
        logInfo("======inode: %"PRId64", oper { uid: %d, gid: %d}, "
                "dentry {uid: %d, gid: %d}, options {uid: %d, gid: %d}, "
                "record {uid: %d, gid: %d}", dentry->inode,
                record->oper.uid, record->oper.gid,
                dentry->stat.uid, dentry->stat.gid,
                record->options.uid, record->options.gid,
                record->stat.uid, record->stat.gid);
                */
        if (!FDIR_USE_POSIX_ACL || record->oper.uid == 0) {
        } else if (record->oper.uid == dentry->stat.uid) {
            /* can't change owner to other user */

            /*
            logInfo("oper {uid: %d, gid: %d, additional count: %d, first gid: %d}, "
                    "change to {uid: %d, gid: %d}",
                    record->oper.uid, record->oper.gid,
                    record->oper.additional_gids.count,
                    record->oper.additional_gids.count > 0 ?
                    buff2int(record->oper.additional_gids.list) : -1,
                    record->stat.uid, record->stat.gid);
                    */

            if ((record->options.uid && record->stat.uid >= 0) &&
                    record->stat.uid != dentry->stat.uid)
            {
                return EPERM;
            }


            if (record->options.gid && record->stat.gid >= 0) {
                if (!((record->stat.gid == record->oper.gid) || group_contain(
                                record->stat.gid, &record->oper)))
                {
                    return EPERM;
                }
            }

            /* when non-super-user calls chown successfully,
             * S_ISUID and S_ISGID may be removed, except when
             * both uid and gid are equal to -1.
             **/
            if (!((record->options.uid && record->stat.uid == -1)
                        && (record->options.gid && record->stat.gid == -1)))
            {
                if (dentry->stat.mode & (S_ISUID|S_ISGID)) {
                    record->options.mode = 1;
                    record->stat.mode = dentry->stat.mode & ~(S_ISUID|S_ISGID);
                    dentry->stat.mode = record->stat.mode;
                }
            }
        } else {
            /* chown return 0 if user is not owner of a file, but chown
             * is called with both uid and gid equal to -1.
             **/
            if (!((record->options.uid && record->stat.uid == -1)
                        && (record->options.gid && record->stat.gid == -1)))
            {
                return EPERM;
            }
        }

        if (record->options.uid) {
            if (record->stat.uid >= 0) {
                dentry->stat.uid = record->stat.uid;
            } else {
                record->options.uid = 0;
            }
        }
        if (record->options.gid) {
            if (record->stat.gid >= 0) {
                dentry->stat.gid = record->stat.gid;
            } else {
                record->options.gid = 0;
            }
        }

        if (record->source == fdir_record_source_master_rpc &&
                (record->options.uid || record->options.gid) &&
                FDIR_USE_POSIX_ACL && !record->options.ctime)
        {
            /* successful chown updates ctime. */
            record->options.ctime = 1;
            record->stat.ctime = record->timestamp;
        }
    }

    if (record->options.atime || record->options.mtime) {
        if (!IS_DENTRY_OWNER(record->oper.uid, dentry)) {
            if ((result=dentry_access(dentry, &record->oper, W_OK)) != 0) {
                return EPERM;
            }
        }

        if (record->options.atime) {
            if (record->options.atime_now) {
                dentry->stat.atime = record->stat.atime = g_current_time;
            } else {
                dentry->stat.atime = record->stat.atime;
            }
        }
        if (record->options.mtime) {
            if (record->options.mtime_now) {
                dentry->stat.mtime = record->stat.mtime = g_current_time;
            } else {
                dentry->stat.mtime = record->stat.mtime;
            }
        }
    }

    if (record->options.size) {
        if ((result=dentry_access(dentry, &record->oper, W_OK)) != 0) {
            return result;
        }

        dentry->stat.size = record->stat.size;
    }

    if (record->options.ctime) {
        dentry->stat.ctime = record->stat.ctime;
    }

    if (record->options.space_end) {
        dentry->stat.space_end = record->stat.space_end;
    }
    if (record->options.inc_alloc) {
        dentry_set_inc_alloc_bytes(dentry, record->stat.alloc);
    }

    return 0;
}

static int deal_update_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;

    if (record->dentry_type == fdir_dentry_type_inode) {
        if ((result=inode_index_get_dentry(thread_ctx, record->inode,
                        &record->me.dentry)) != 0)
        {
            return result;
        }
    } else {
        if ((result=dentry_find(&record->me.fullname, &record->oper,
                        &record->me.dentry)) != 0)
        {
            return result;
        }
        record->inode = record->me.dentry->inode;
    }

    if ((record->flags & FDIR_FLAGS_FOLLOW_SYMLINK) &&
            S_ISLNK(record->me.dentry->stat.mode))
    {
        if ((result=dentry_resolve_symlink(&record->me.dentry,
                        &record->oper)) != 0)
        {
            return result;
        }
        record->inode = record->me.dentry->inode;
    }

    return update_dentry_stat(record->me.dentry, record);
}

static inline void deal_affected_dentries(FDIRBinlogRecord *record)
{
    FDIRAffectedDentry *current;
    FDIRAffectedDentry *end;

    end = record->affected.entries + record->affected.count;
    for (current=record->affected.entries; current<end; current++) {
        if (current->op_type == da_binlog_op_type_update &&
                current->remove_from_parent)
        {
            record->me.dentry->parent = NULL;   //orphan inode
        }
    }
}

static int deal_dump_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;

    if ((result=check_parent(thread_ctx, record)) != 0) {
        return result;
    }

    if (FDIR_IS_DENTRY_HARD_LINK(record->stat.mode)) {
        if ((result=set_hdlink_src_dentry(thread_ctx, record)) != 0) {
            return result;
        }
    }

    if ((result=dentry_create(thread_ctx, record)) != 0) {
        return result;
    }

    return inode_index_xattrs_copy(record->xattr_kvarray.elts,
            record->xattr_kvarray.count, record->me.dentry);
}

static int deal_update_record(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    struct fast_task_info *task;
    int result;
    int ignore_errno;
    bool set_data_verson;
    bool is_error;

    if (record->source == fdir_record_source_master_rpc &&
            CLUSTER_MYSELF_PTR != CLUSTER_MASTER_ATOM_PTR)
    {
        task = record->notify.args;
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "[data-thread] i am NOT master");

        result = SF_RETRIABLE_ERROR_NOT_MASTER;
        is_error = true;
        record->notify.func(record, result, is_error);
        return result;
    }

    record->affected.count = 0;
    switch (record->operation) {
        case BINLOG_OP_CREATE_DENTRY_INT:
        case BINLOG_OP_REMOVE_DENTRY_INT:
            if ((result=find_or_check_parent(thread_ctx, record)) != 0) {
                logError("file: "__FILE__", line: %d, "
                        "hash code: %u, inode: %"PRId64", get parent: %"
                        PRId64", fail", __LINE__, record->hash_code,
                        record->inode, record->me.pname.parent_inode);
                ignore_errno = 0;
                break;
            }

            if (record->me.parent != NULL && dentry_access(record->
                        me.parent, &record->oper, W_OK) != 0)
            {
                result = EACCES;
                ignore_errno = 0;
                break;
            }

            if (record->operation == BINLOG_OP_CREATE_DENTRY_INT) {
                if (FDIR_IS_DENTRY_HARD_LINK(record->stat.mode)) {
                    if ((result=set_hdlink_src_dentry(thread_ctx,
                                    record)) != 0)
                    {
                        logError("file: "__FILE__", line: %d, "
                                "hash code: %u, inode: %"PRId64", "
                                "get src inode: %"PRId64", fail", __LINE__,
                                record->hash_code, record->inode,
                                record->hdlink.src.inode);
                        ignore_errno = 0;
                        break;
                    }
                } else if (S_ISLNK(record->stat.mode) && record->dentry_type
                        == fdir_dentry_type_fullname)
                {
                    if ((result=service_set_record_link(record,
                                    (struct fast_task_info *)
                                    record->notify.args)) != 0)
                    {
                        ignore_errno = 0;
                        break;
                    }
                }

                result = dentry_create(thread_ctx, record);
                ignore_errno = EEXIST;
            } else {
                result = dentry_remove(thread_ctx, record);
                ignore_errno = ENOENT;
            }
            break;
        case BINLOG_OP_RENAME_DENTRY_INT:
            ignore_errno = 0;
            result = deal_record_rename_op(thread_ctx, record);
            break;
        case BINLOG_OP_UPDATE_DENTRY_INT:
            result = deal_update_dentry(thread_ctx, record);
            ignore_errno = 0;
            break;
        case BINLOG_OP_SET_XATTR_INT:
            ignore_errno = 0;
            if ((result=xattr_update_prepare(thread_ctx, record)) == 0) {
                result = inode_index_set_xattr(record->me.dentry, record);
            }
            break;
        case BINLOG_OP_REMOVE_XATTR_INT:
            if ((result=xattr_update_prepare(thread_ctx, record)) == 0) {
                result = inode_index_remove_xattr(record->me.dentry,
                        &record->xattr.key);
            }
            ignore_errno = ENODATA;
            break;
        case BINLOG_OP_DUMP_DENTRY_INT:
            result = deal_dump_dentry(thread_ctx, record);
            ignore_errno = EEXIST;
            break;
        case SERVICE_OP_SYS_LOCK_RELEASE_INT:
            ignore_errno = 0;
            if ((result=service_sys_lock_release((struct fast_task_info *)
                            record->notify.args, true)) != 0)
            {
                break;
            }
            record->operation = SERVICE_OP_SET_DSIZE_INT;
        case SERVICE_OP_SET_DSIZE_INT:
            ignore_errno = 0;
            if ((result=inode_index_check_set_dentry_size(
                            thread_ctx, record)) == 0)
            {
                if (record->options.flags != 0) {
                    record->data_version = __sync_add_and_fetch(
                            &DATA_CURRENT_VERSION, 1);
                }
            }
            break;
        case SERVICE_OP_BATCH_SET_DSIZE_INT:
            ignore_errno = ENOENT;
            result = batch_set_dentry_size(thread_ctx, record);
            break;
        default:
            ignore_errno = 0;
            result = 0;
            break;
    }

    if (record->operation == SERVICE_OP_BATCH_SET_DSIZE_INT ||
            record->operation == SERVICE_OP_SET_DSIZE_INT)
    {
        if (record->operation == SERVICE_OP_SET_DSIZE_INT) {
            record->operation = BINLOG_OP_UPDATE_DENTRY_INT;
        }
        set_data_verson = false;
        is_error = (result != 0);
    } else if (result == 0) {
        if (record->data_version == 0) {
            record->data_version = __sync_add_and_fetch(
                    &DATA_CURRENT_VERSION, 1);
            set_data_verson = false;
        } else {
            set_data_verson = true;
        }
        is_error = false;
    } else {
        set_data_verson = record->data_version > 0;
        is_error = !((result == ignore_errno) &&
                (g_data_thread_vars.error_mode ==
                 FDIR_DATA_ERROR_MODE_LOOSE));
    }

    if (set_data_verson && !is_error) {
        int64_t old_version;
        old_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 0);
        while (record->data_version > old_version) {
            if (__sync_bool_compare_and_swap(&DATA_CURRENT_VERSION,
                        old_version, record->data_version))
            {
                break;
            }
            old_version = __sync_add_and_fetch(&DATA_CURRENT_VERSION, 0);
        }
    }

    if (result == 0 && record->affected.count > 0 && !STORAGE_ENABLED) {
        deal_affected_dentries(record);
    }

    if (result == 0 && STORAGE_ENABLED && record->data_version > 0) {
        if (record->operation != BINLOG_OP_NO_OP_INT) {
            if (record->operation == SERVICE_OP_BATCH_SET_DSIZE_INT) {
                result = push_batch_set_dsize_to_db_update_queue(
                        thread_ctx, record);
            } else {
                result = push_to_db_update_queue(thread_ctx, record);
            }
            if (result != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "push_to_db_update_queue fail, "
                        "program exit!", __LINE__);
                sf_terminate_myself();
            }
        }

        if (record->data_version > thread_ctx->DATA_THREAD_LAST_VERSION) {
            thread_ctx->DATA_THREAD_LAST_VERSION = record->data_version;
        }
    }

    if (record->notify.func != NULL) {
        record->notify.func(record, result, is_error);
    }

    /*
    logInfo("file: "__FILE__", line: %d, record: %p, "
            "operation: %d, hash code: %u, inode: %"PRId64
             ", data_version: %"PRId64", result: %d, is_error: %d",
             __LINE__, record, record->operation, record->hash_code,
             record->inode, record->data_version, result, is_error);
             */

    return result;
}

static int deal_list_dentry(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    struct fast_task_info *task;

    task = (struct fast_task_info *)record->notify.args;
    if (record->dentry_type == fdir_dentry_type_inode) {
        if ((result=inode_index_get_dentry_ex(thread_ctx, record->inode,
                        &record->me.dentry, true)) != 0)
        {
            return result;
        }

        result = dentry_list(record->me.dentry, &record->oper,
                &DENTRY_LIST_CACHE.array);
    } else {
        result = dentry_list_by_path(&record->me.fullname, &record->oper,
                &DENTRY_LIST_CACHE.array, &record->me.dentry);
    }

    return result;
}

static int deal_flock_apply(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    struct fast_task_info *task;

    task = (struct fast_task_info *)record->notify.args;
    record->flock->ftask = inode_index_flock_apply(thread_ctx,
            record->inode, &record->flock_params,
            record->options.blocked, task, &result);
    return result;
}

static int deal_flock_unlock(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    return inode_index_flock_unlock(thread_ctx, record->inode,
            &record->flock_params);
}

static int deal_sys_lock_apply(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;
    struct fast_task_info *task;

    task = (struct fast_task_info *)record->notify.args;
    record->flock->stask = inode_index_sys_lock_apply(thread_ctx,
            record->inode, record->options.blocked, task, &result);
    return result;
}

static int deal_query_record(FDIRDataThreadContext *thread_ctx,
        FDIRBinlogRecord *record)
{
    int result;

    switch (record->operation) {
        case SERVICE_OP_STAT_DENTRY_INT:
        case SERVICE_OP_ACCESS_DENTRY_INT:
        case SERVICE_OP_READ_LINK_INT:
        case SERVICE_OP_LOOKUP_INODE_INT:
        case SERVICE_OP_GET_XATTR_INT:
        case SERVICE_OP_LIST_XATTR_INT:
        case SERVICE_OP_GET_FULLNAME_INT:
            if (record->dentry_type == fdir_dentry_type_inode) {
                result = inode_index_get_dentry(thread_ctx,
                        record->inode, &record->me.dentry);
            } else if (record->dentry_type == fdir_dentry_type_pname) {
                result = inode_index_get_dentry_by_pname(thread_ctx,
                        record->me.pname.parent_inode, &record->me.
                        pname.name, &record->oper, &record->me.dentry);
            } else {
                result = dentry_find(&record->me.fullname,
                        &record->oper, &record->me.dentry);
            }

            if (result == 0) {
                SFErrorInfo error_info;
                switch (record->operation) {
                    case SERVICE_OP_STAT_DENTRY_INT:
                    case SERVICE_OP_ACCESS_DENTRY_INT:
                    case SERVICE_OP_GET_XATTR_INT:
                    case SERVICE_OP_LIST_XATTR_INT:
                    case SERVICE_OP_GET_FULLNAME_INT:
                        if ((record->flags & FDIR_FLAGS_FOLLOW_SYMLINK) &&
                                S_ISLNK(record->me.dentry->stat.mode))
                        {
                            if ((result=dentry_resolve_symlink(&record->me.
                                            dentry, &record->oper)) != 0)
                            {
                                break;
                            }
                        }

                        switch (record->operation) {
                            case SERVICE_OP_ACCESS_DENTRY_INT:
                                result = dentry_access(record->me.dentry,
                                        &record->oper, record->mask);
                                break;
                            case SERVICE_OP_GET_XATTR_INT:
                                result = inode_index_get_xattr(record->me.dentry,
                                        &record->xattr.key, &record->xattr.value);
                                break;
                            case SERVICE_OP_LIST_XATTR_INT:
                                if (STORAGE_ENABLED) {
                                    result = dentry_check_load_xattr(thread_ctx,
                                            record->me.dentry);
                                }
                                break;
                            case SERVICE_OP_GET_FULLNAME_INT:
                                *(error_info.message) = '\0';
                                if ((result=dentry_get_full_path(record->me.
                                                dentry, &record->fullname,
                                                &error_info)) != 0)
                                {
                                    logError("file: "__FILE__", line: %d, "
                                            "get dentry path fail, error info: %s",
                                            __LINE__, error_info.message);
                                }
                                break;
                        }

                        break;
                }
            }

            break;
        case SERVICE_OP_LIST_DENTRY_INT:
            result = deal_list_dentry(thread_ctx, record);
            break;
        case SERVICE_OP_FLOCK_APPLY_INT:
            result = deal_flock_apply(thread_ctx, record);
            break;
        case SERVICE_OP_FLOCK_UNLOCK_INT:
            result = deal_flock_unlock(thread_ctx, record);
            break;
        case SERVICE_OP_SYS_LOCK_APPLY_INT:
            result = deal_sys_lock_apply(thread_ctx, record);
            break;
        case SERVICE_OP_FLOCK_GETLK_INT:
            result = inode_index_flock_getlk(thread_ctx,
                    record->inode, record->flock->ftask);
            break;
        case SERVER_OP_DUMP_DATA_INT:
            result = binlog_dump_data(thread_ctx, record->notify.args);
            break;
        default:
            result = EPROTONOSUPPORT;
            break;
    }

    record->notify.func(record, result, !(result == 0 ||
                result == ENOENT || result == EINPROGRESS));
    return result;
}

static inline void deal_delay_free_chain(FDIRDataThreadContext *thread_ctx)
{
    FDIRServerDentry *dentry;

    while (thread_ctx->delay_free_head != NULL) {
        dentry = thread_ctx->delay_free_head;
        thread_ctx->delay_free_head = thread_ctx->delay_free_head->free_next;

        dentry_free(dentry);
    }
}

static void *data_thread_func(void *arg)
{
    FDIRBinlogRecord *record;
    FDIRBinlogRecord *current;
    FDIRDataThreadContext *thread_ctx;
    int update_count;

    __sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    thread_ctx = (FDIRDataThreadContext *)arg;

#ifdef OS_LINUX
    {
        char thread_name[16];
        snprintf(thread_name, sizeof(thread_name),
                "data[%d]", thread_ctx->index);
        prctl(PR_SET_NAME, thread_name);
    }
#endif

    while (SF_G_CONTINUE_FLAG) {
        record = (FDIRBinlogRecord *)fc_queue_pop_all(&thread_ctx->queue);
        if (record != NULL) {
            update_count = 0;
            do {
                current = record;
                record = record->next;
                switch (current->record_type) {
                    case fdir_record_type_update:
                        ++update_count;
                        deal_update_record(thread_ctx, current);
                        break;
                    case fdir_record_type_query:
                        deal_query_record(thread_ctx, current);
                        break;
                    default:
                        logError("file: "__FILE__", line: %d, "
                                "invalid record type: %d",
                                __LINE__, current->record_type);
                        break;
                }
            } while (record != NULL && SF_G_CONTINUE_FLAG);

            if (STORAGE_ENABLED && update_count > 0) {
                __sync_sub_and_fetch(&thread_ctx->update_notify.
                        waiting_records, update_count);
            }
        }

        if (thread_ctx->delay_free_head != NULL) {
            deal_delay_free_chain(thread_ctx);
        }

        if (__sync_add_and_fetch(&thread_ctx->free_context.
                    immediate.waiting_count, 0) != 0)
        {
            /*
            logInfo("file: "__FILE__", line: %d, "
                    "free_context.immediate.waiting_count: %d",
                    __LINE__, __sync_add_and_fetch(&thread_ctx->
                        free_context.immediate.waiting_count, 0));
             */

            deal_immediate_free_queue(thread_ctx);
        }

        if (STORAGE_ENABLED && thread_ctx->lru_ctx.target_reclaims > 0) {
            int64_t target_reclaims;
            if ((target_reclaims=thread_ctx->lru_ctx.target_reclaims) > 0) {
                thread_ctx->lru_ctx.target_reclaims = 0;
                dentry_lru_eliminate(thread_ctx, target_reclaims);
            }
        }

#ifdef FDIR_DUMP_DATA_FOR_DEBUG
        if (thread_ctx->dump_flag) {
            thread_ctx->dump_flag = false;
            server_dump_data(thread_ctx);
        }
#endif
    }

    __sync_sub_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    return NULL;
}

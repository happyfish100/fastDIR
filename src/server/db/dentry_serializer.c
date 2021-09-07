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
#include "fastcommon/pthread_func.h"
#include "../server_global.h"
#include "../dentry.h"
#include "dentry_serializer.h"

#define DENTRY_FIELD_ID_INODE         1
#define DENTRY_FIELD_ID_PARENT        2  //parent inode
#define DENTRY_FIELD_ID_SUBNAME       3
#define DENTRY_FIELD_ID_SRC_INODE     5  //src inode for hard link
#define DENTRY_FIELD_ID_LINK          6
#define DENTRY_FIELD_ID_MODE         10
#define DENTRY_FIELD_ID_ATIME        11
#define DENTRY_FIELD_ID_BTIME        12
#define DENTRY_FIELD_ID_CTIME        13
#define DENTRY_FIELD_ID_MTIME        14
#define DENTRY_FIELD_ID_UID          15
#define DENTRY_FIELD_ID_GID          16
#define DENTRY_FIELD_ID_FILE_SIZE    17
#define DENTRY_FIELD_ID_ALLOC_SIZE   18
#define DENTRY_FIELD_ID_SPACE_END    19
#define DENTRY_FIELD_ID_NLINK        20
#define DENTRY_FIELD_ID_XATTR        30
#define DENTRY_FIELD_ID_HASH_CODE    40
#define DENTRY_FIELD_ID_CHILDREN     50

#define FIXED_INODES_ARRAY_SIZE  1024

typedef struct
{
    int64_t fixed[FIXED_INODES_ARRAY_SIZE];
    int64_t *elts;
    int count;
    int alloc;
} smart_int64_array_t;

static int realloc_array(smart_int64_array_t *array)
{
    int64_t *elts;

    array->alloc *= 2;
    if ((elts=fc_malloc(sizeof(int64_t) * array->alloc)) == NULL) {
        return ENOMEM;
    }

    memcpy(elts, array->elts, sizeof(int64_t) * array->count);
    if (array->elts != array->fixed) {
        free(array->elts);
    }
    array->elts = elts;
    return 0;
}

static int pack_children(const FDIRServerDentry *dentry, FastBuffer *buffer)
{
    smart_int64_array_t children;
    UniqSkiplistIterator it;
    FDIRServerDentry *child;
    int result;

    result = 0;
    children.elts = children.fixed;
    children.alloc = FIXED_INODES_ARRAY_SIZE;
    children.count = 0;
    uniq_skiplist_iterator(dentry->children, &it);
    while ((child=uniq_skiplist_next(&it)) != NULL) {
        if (children.count >= children.alloc) {
            if ((result=realloc_array(&children)) != 0) {
                break;
            }
        }
        children.elts[children.count++] = child->inode;
    }

    if (result == 0) {
        result = sf_serializer_pack_int64_array(buffer,
                DENTRY_FIELD_ID_CHILDREN,
                children.elts, children.count);
    }

    if (children.elts != children.fixed) {
        free(children.elts);
    }
    return result;
}

static int pack(const FDIRServerDentry *dentry, FastBuffer *buffer)
{
    int result;
    int64_t parent_inode;

    if ((result=sf_serializer_pack_int64(buffer,
                    DENTRY_FIELD_ID_INODE,
                    dentry->inode)) != 0)
    {
        return result;
    }

    parent_inode = (dentry->parent != NULL ? dentry->parent->inode : 0);
    if ((result=sf_serializer_pack_int64(buffer,
                    DENTRY_FIELD_ID_PARENT,
                    parent_inode)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_string(buffer,
                    DENTRY_FIELD_ID_SUBNAME,
                    &dentry->name)) != 0)
    {
        return result;
    }


    if (FDIR_IS_DENTRY_HARD_LINK(dentry->stat.mode)) {
        if ((result=sf_serializer_pack_int64(buffer,
                        DENTRY_FIELD_ID_SRC_INODE,
                        dentry->src_dentry->inode)) != 0)
        {
            return result;
        } 
    } else if (S_ISLNK(dentry->stat.mode)) {
        if ((result=sf_serializer_pack_string(buffer,
                        DENTRY_FIELD_ID_LINK,
                        &dentry->link)) != 0)
        {
            return result;
        }
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_MODE,
                    dentry->stat.mode)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_ATIME,
                    dentry->stat.atime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_BTIME,
                    dentry->stat.btime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_CTIME,
                    dentry->stat.ctime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_MTIME,
                    dentry->stat.mtime)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_UID,
                    dentry->stat.uid)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_GID,
                    dentry->stat.gid)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_FILE_SIZE,
                    dentry->stat.size)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_ALLOC_SIZE,
                    dentry->stat.alloc)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_SPACE_END,
                    dentry->stat.space_end)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_integer(buffer,
                    DENTRY_FIELD_ID_NLINK,
                    dentry->stat.nlink)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_int32(buffer,
                    DENTRY_FIELD_ID_HASH_CODE,
                    dentry->hash_code)) != 0)
    {
        return result;
    }

    if (dentry->kv_array != NULL) {
        if ((result=sf_serializer_pack_map(buffer,
                        DENTRY_FIELD_ID_XATTR,
                        dentry->kv_array->elts,
                        dentry->kv_array->count)) != 0)
        {
            return result;
        }
    }

    if (S_ISDIR(dentry->stat.mode)) {
        return pack_children(dentry, buffer);
    }

    return 0;
}

int dentry_serializer_pack(const FDIRServerDentry *dentry, FastBuffer *buffer)
{
    int result;

    sf_serializer_pack_begin(buffer);
    if ((result=pack(dentry, buffer)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "pack dentry fail, inode: %"PRId64,
                __LINE__, dentry->inode);
        return result;
    }

    sf_serializer_pack_end(buffer);
    return 0;
}

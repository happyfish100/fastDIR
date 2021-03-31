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

#include <limits.h>
#include "fastcommon/md5.h"
#include "fdir_func.h"

int fdir_validate_xattr(const key_value_pair_t *xattr)
{
    if (xattr->key.len <= 0) {
        logError("file: "__FILE__", line: %d, "
                "invalid xattr name, length: %d <= 0",
                __LINE__, xattr->key.len);
        return EINVAL;
    }
    if (xattr->key.len > NAME_MAX) {
        logError("file: "__FILE__", line: %d, "
                "xattr name length: %d is too long, exceeds %d",
                __LINE__, xattr->key.len, NAME_MAX);
        return ENAMETOOLONG;
    }

    if (xattr->value.len < 0) {
        logError("file: "__FILE__", line: %d, "
                "invalid xattr value, length: %d < 0",
                __LINE__, xattr->value.len);
        return EINVAL;
    }
    if (xattr->value.len > FDIR_XATTR_MAX_VALUE_SIZE) {
        logError("file: "__FILE__", line: %d, "
                "xattr value length: %d is too long, exceeds %d",
                __LINE__, xattr->value.len, FDIR_XATTR_MAX_VALUE_SIZE);
        return ENAMETOOLONG;
    }

    return 0;
}

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

//bid_journal.h

#ifndef _BID_JOURNAL_H_
#define _BID_JOURNAL_H_

#include "../storage_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int bid_journal_init();

int bid_journal_log(const FDIRInodeBinlogIdJournal *journal);

int bid_journal_fetch(FDIRInodeBidJournalArray *jarray,
        const int64_t start_version);

#ifdef __cplusplus
}
#endif

#endif

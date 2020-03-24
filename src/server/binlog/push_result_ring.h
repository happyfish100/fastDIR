//push_result_ring.h

#ifndef _PUSH_RESULT_RING_H_
#define _PUSH_RESULT_RING_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int push_result_ring_check_init(FDIRBinlogPushResultContext *ctx,
        const int alloc_size);

void push_result_ring_destroy(FDIRBinlogPushResultContext *ctx);

int push_result_ring_add(FDIRBinlogPushResultContext *ctx,
        const uint64_t data_version, struct fast_task_info *waiting_task);

int push_result_ring_remove(FDIRBinlogPushResultContext *ctx,
        const uint64_t data_version);

void push_result_ring_clear_all(FDIRBinlogPushResultContext *ctx);

void push_result_ring_clear_timeouts(FDIRBinlogPushResultContext *ctx);

#ifdef __cplusplus
}
#endif

#endif

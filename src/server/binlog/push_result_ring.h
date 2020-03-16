//push_result_ring.h

#ifndef _PUSH_RESULT_RING_H_
#define _PUSH_RESULT_RING_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif


int push_result_ring_check_init(FDIRBinlogPushResultRing *ring,
        const int alloc_size);

static inline void push_result_ring_destroy(FDIRBinlogPushResultRing *ring)
{
    if (ring->entries != NULL) {
        free(ring->entries);
        ring->start = ring->end = ring->entries = NULL;
        ring->size = 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif

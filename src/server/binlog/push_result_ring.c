#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "push_result_ring.h"

int push_result_ring_check_init(FDIRBinlogPushResultRing *ring,
        const int alloc_size)
{
    int bytes;

    if (ring->entries != NULL) {
        return 0;
    }

    bytes = sizeof(FDIRBinlogPushResultEntry) * alloc_size;
    ring->entries = (FDIRBinlogPushResultEntry *)malloc(bytes);
    if (ring->entries == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(ring->entries, 0, bytes);

    ring->start = ring->end = ring->entries;
    ring->size = alloc_size;

    return 0;
}

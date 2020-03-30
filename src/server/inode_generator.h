//inode_generator.h

#ifndef _INODE_GENERATOR_H_
#define _INODE_GENERATOR_H_

#include "server_global.h"

#define INODE_SN_MAX_QPS   (1000 * 1000)

#ifdef __cplusplus
extern "C" {
#endif

int inode_generator_init();
void inode_generator_destroy();

//skip avoid conflict
static inline void inode_generator_skip()
{
    __sync_add_and_fetch(&CURRENT_INODE_SN, INODE_SN_MAX_QPS);
}

static inline int64_t inode_generator_next()
{
    return INODE_CLUSTER_PART | __sync_add_and_fetch(&CURRENT_INODE_SN, 1);
}

#ifdef __cplusplus
}
#endif

#endif

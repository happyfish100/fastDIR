//inode_generator.h

#ifndef _INODE_GENERATOR_H_
#define _INODE_GENERATOR_H_

#include "server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

int inode_generator_init();
void inode_generator_destroy();

static inline int64_t inode_generator_next()
{
    return INODE_CLUSTER_PART | __sync_add_and_fetch(&CURRENT_INODE_SN, 1);
}

#ifdef __cplusplus
}
#endif

#endif

#ifndef _FDIR_CLIENT_OPS_H
#define _FDIR_CLIENT_OPS_H

#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "fastdir/fdir_client.h"

/* fco is Fdir Client Ops */

typedef struct fdir_fuse_dirptr {
    FDIRClientDentryArray dentry_array;
    off_t offset;
} FdirFuseDirptr;

#ifdef __cplusplus
extern "C" {
#endif
/** Fdir Client Ops init
 *
 *  @return 0 for success, -1 for failure
 */
int fco_init();

/** Fdir Client Ops destroy
 *
 *  @return 0 for success, -1 for failure
 */
int fco_destroy();

/** Fdir Client Ops stat: get file status
 *
 *  @param full_path: full path of dentry
 *  @param st: stat will be set
 *  @return 0 for success, errno for failure
 */
int fco_stat(const char* full_path, struct stat* st);

/** Fdir Client Ops set default stat
 *
 *  @param st: stat will be set
 */
void fco_set_default_stat(struct stat* st);

/** Fdir Client Ops open dir
 *
 *  @param full_path: full path of dentry
 *  @return pointer of FdirFuseDirptr for success, NULL for failure
 */
FdirFuseDirptr* fco_opendir(const char* full_path);

/** Fdir Client Ops close dir
 *
 *  @param dp: which return from fco_opendir
 */
void fco_closedir(FdirFuseDirptr* dp);

#ifdef __cplusplus
}
#endif
#endif  // _FDIR_CLIENT_OPS_H

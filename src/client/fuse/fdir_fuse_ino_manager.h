#ifndef _FDIR_FUSE_INO_MANAGER_H
#define _FDIR_FUSE_INO_MANAGER_H
#include <fuse_lowlevel.h>
#include <stdarg.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <fuse.h>
#include <assert.h>
#include <fuse_lowlevel.h>
#include <stddef.h>
#include <fcntl.h> /* Definition of AT_* constants */
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <pthread.h>
#include <hash.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef PATH_LEN_MAX
#define PATH_LEN_MAX 4080
#endif

 typedef struct fdir_inode {
    /* Full path of dir/file */
    char *name;
    /* Inode number */
    ino_t ino;
    /* Device ID */
    dev_t dev;
    /* Lookup count of this node */
    uint64_t nlookup;
} FdirInode;

extern FdirInode *fino_root;

#define FDIR_INODE(ino) (ino == FUSE_ROOT_ID ? fino_root : (FdirInode *)ino)
#define FDIR_NAME(ino) (FDIR_INODE(ino)->name)

/** Inode manager init
 *  @return 0 for success, -1 for failure
 */
int ino_manager_init();

/** Inode manager destroy
 *  @return 0 for success, -1 for failure
 */
int ino_manager_destroy();

/** Open fdir inode or create when not exist.
 *
 *  @param st: stat of full_path
 *  @param full_path: full path of name
 *  @param fino: will be set before return
 *  @return fdir inode for success, NULL for failure
 */
FdirInode *open_fdir_inode(const struct stat *st, const char *full_path);

/** Release fdir inode
 *
 *  @param fino: fdir inode which need release
 *  @return 0 for success, errno for failure
 */
int release_fdir_inode(FdirInode *fino);

inline void construct_full_path(fuse_ino_t ino, const char *name,
                                char *full_path) {
    if (ino == FUSE_ROOT_ID) {
        snprintf(full_path, PATH_LEN_MAX, "/%s", name);
    } else {
        snprintf(full_path, PATH_LEN_MAX, "%s/%s", FDIR_NAME(ino), name);
    }
}

#ifdef __cplusplus
}
#endif
#endif  // _FDIR_FUSE_INO_MANAGER_H

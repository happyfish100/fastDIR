#include "fdir_fuse_ino_manager.h"
#include "sf/sf_util.h"

FdirInode* fino_root = NULL;

static void destroy_fdir_inode(FdirInode *fino) {
    if (fino) {
        free(fino->name);
        free(fino);
    }
}

int ino_manager_init() {
    fino_root = (FdirInode*)malloc(sizeof(*fino_root));
    if (fino_root == NULL) {
        lerr("ino_manager_init failed. ENOMEM");
        return -1;
    }
    fino_root->name = "/";
    fino_root->ino = FUSE_ROOT_ID;
    fino_root->dev = 0;
    fino_root->nlookup = 2;
    return 0;
}

int ino_manager_destroy() {
    destroy_fdir_inode(fino_root);
    return 0;
}

FdirInode* open_fdir_inode(const struct stat *st, const char *full_path) {
    FdirInode* fino;
    fino = (FdirInode*)malloc(sizeof(FdirInode));
    if (fino == NULL) {
        lerr("open_fdir_inode failed. ENOMEM");
        return NULL;
    }
    memset(fino, 0, sizeof(FdirInode));
    fino->name = strdup(full_path);
    fino->ino = st->st_ino;
    fino->dev = st->st_dev;
    return fino;
}

int release_fdir_inode(FdirInode *fino) {
    destroy_fdir_inode(fino);
    return 0;
}

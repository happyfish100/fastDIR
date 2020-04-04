#include "fdir_fuse_ll.h"
#include <fuse_lowlevel.h>
#include <string.h>
#include <stdlib.h>
#include "fdir_fuse_ino_manager.h"
#include "fdir_client_ops.h"
#include "fastcommon/logger.h"
#include "fastdir/fdir_client.h"
#include "sf/sf_util.h"

#define FUSE_ATTR_TIMEOUT 1.0
#define FUSE_ENTRY_TIMEOUT 1.0
#define READDIR_BUF_SIZE 4096

#define FDIR_DIRPTR(fi) ((FdirFuseDirptr *)(fi->fh))

#define min(x, y) ((x) < (y) ? (x) : (y))

void fuse_ll_init(void *userdata, struct fuse_conn_info *conn) {
    linfo("fuse_ll_init");
}

void fuse_ll_destroy(void *userdata) { linfo("fuse_ll_destroy"); }

void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    struct fuse_entry_param e;
    int res;
    char full_path[PATH_LEN_MAX];
    FdirInode *fino;

    construct_full_path(parent, name, full_path);

    memset(&e, 0, sizeof(e));

    e.attr_timeout = FUSE_ATTR_TIMEOUT;
    e.entry_timeout = FUSE_ENTRY_TIMEOUT; /* dentry timeout */

    res = fco_stat(full_path, &e.attr);
    ldebug("fuse_ll_lookup: name=%s, full_path=%s, fco_stat_res=%d", name,
           full_path, res);
    if (res != 0) {
        fuse_reply_err(req, res);
        return;
    }

    fino = open_fdir_inode(&e.attr, full_path);
    if (fino == NULL) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    e.ino = (uint64_t)fino;
    fuse_reply_entry(req, &e);
}

void fuse_ll_getattr(fuse_req_t req, fuse_ino_t ino,
                     struct fuse_file_info *fi) {
    (void)fi;
    int res;
    struct stat st;
    const char *full_path;

    full_path = FDIR_NAME(ino);
    res = fco_stat(full_path, &st);
    ldebug("fuse_ll_getattr: ino=%ld, full_path=%s, fco_stat_res=%d", ino,
           full_path, res);
    if (res != 0) {
        fuse_reply_err(req, res);
        return;
    }

    fuse_reply_attr(req, &st, FUSE_ATTR_TIMEOUT);
}

void fuse_ll_opendir(fuse_req_t req, fuse_ino_t ino,
                     struct fuse_file_info *fi) {
    FdirFuseDirptr *dp;
    const char *full_path;

    full_path = FDIR_NAME(ino);
    dp = fco_opendir(full_path);
    ldebug("fuse_ll_opendir: full_path=%s, dp=%p", full_path, dp);

    if (dp == NULL) {
        return (void)fuse_reply_err(req, errno);
    }

    dp->offset = 0;
    fi->fh = (uint64_t)dp;

    fuse_reply_open(req, fi);
}

#define HELPER_FUSE_ADD_DIRENTRY(name)                                      \
    do {                                                                    \
        entsize = fuse_add_direntry(req, p, rem, name, &st, nextoff);       \
        ldebug("nextoff=%ld, name=%s, entsize=%ld, ino=%ld", nextoff, name, \
               entsize, st.st_ino);                                         \
        if (entsize > rem) {                                                \
            fuse_reply_buf(req, buf, buf_size - rem);                       \
            return;                                                         \
        }                                                                   \
        dp->offset += 1;                                                    \
        p += entsize;                                                       \
        rem -= entsize;                                                     \
        nextoff += entsize;                                                 \
    } while (0)

void fuse_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                     struct fuse_file_info *fi) {
    char buf[READDIR_BUF_SIZE];
    char dir_name[PATH_LEN_MAX];
    FdirFuseDirptr *dp;
    struct stat st;
    FDIRClientDentryArray *array;
    FDIRClientDentry *dentry;
    FDIRClientDentry *end;
    size_t buf_size;
    char *p;
    size_t rem;
    size_t nextoff;
    size_t entsize;

    buf_size = min(READDIR_BUF_SIZE, size);
    fco_set_default_stat(&st);
    st.st_ino = 1;  // TODO
    dp = (FdirFuseDirptr *)fi->fh;
    if (dp == NULL) {
        fuse_reply_err(req, ENOENT);
    }
    array = &(dp->dentry_array);

    ldebug("fuse_ll_readdir: off=%ld, array_off=%ld, array_count=%d, size=%ld,",
           off, dp->offset, array->count, size);

    if (dp->offset >= array->count + 2) {  // additional "." and ".."
        fuse_reply_buf(req, NULL, 0);
        return;
    }

    p = buf;
    rem = buf_size;
    nextoff = off;
    end = array->entries + array->count;
    dentry = array->entries + dp->offset;
    for (; dentry < end; dentry++) {
        /* st.st_ino = dentry->stat.inode; */
        snprintf(dir_name, PATH_LEN_MAX, "%.*s", dentry->name.len,
                 dentry->name.str);
        ldebug("dentry_name=%s, dentry_ino=%ld", dir_name, dentry->stat.inode);
        HELPER_FUSE_ADD_DIRENTRY(dir_name);
    }

    if (dp->offset == array->count) {
        HELPER_FUSE_ADD_DIRENTRY(".");
    }
    if (dp->offset == array->count + 1) {
        HELPER_FUSE_ADD_DIRENTRY("..");
    }

    ldebug("fuse_reply_buf: size=%ld", buf_size - rem);
    fuse_reply_buf(req, buf, buf_size - rem);
}

void fuse_ll_releasedir(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info *fi) {
    (void)ino;
    FdirFuseDirptr *dp;

    dp = FDIR_DIRPTR(fi);
    fco_closedir(dp);
    fuse_reply_err(req, 0);
}

void fuse_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                   mode_t mode) {}

void fuse_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {}

void fuse_ll_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                      struct fuse_file_info *fi) {}

void fuse_ll_statfs(fuse_req_t req, fuse_ino_t ino) {}

void fuse_ll_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup) {}

void fuse_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                     int to_set, struct fuse_file_info *fi) {}

void fuse_ll_readlink(fuse_req_t req, fuse_ino_t ino) {}

void fuse_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                   mode_t mode, dev_t rdev) {}

void fuse_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {}

void fuse_ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                     const char *name) {}

void fuse_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                    fuse_ino_t newparent, const char *newname) {}

void fuse_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                  const char *newname) {}

void fuse_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {}

void fuse_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                  struct fuse_file_info *fi) {}

void fuse_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
                   off_t off, struct fuse_file_info *fi) {}

void fuse_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {}

void fuse_ll_release(fuse_req_t req, fuse_ino_t ino,
                     struct fuse_file_info *fi) {}

void fuse_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                   struct fuse_file_info *fi) {}

#ifdef __APPLE__
void fuse_ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      const char *value, size_t size, int flags,
                      uint32_t position) {}
#else
void fuse_ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      const char *value, size_t size, int flags) {}
#endif

#ifdef __APPLE__
void fuse_ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      size_t size, uint32_t position) {}
#else
void fuse_ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      size_t size) {}
#endif

void fuse_ll_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {}

void fuse_ll_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name) {}

void fuse_ll_access(fuse_req_t req, fuse_ino_t ino, int mask) {}

void fuse_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                    mode_t mode, struct fuse_file_info *fi) {}

void fuse_ll_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                   struct flock *lock) {}

void fuse_ll_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                   struct flock *lock, int sleep) {}

void fuse_ll_bmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize,
                  uint64_t idx) {}

void fuse_ll_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, void *arg,
                   struct fuse_file_info *fi, unsigned flags,
                   const void *in_buf, size_t in_bufsz, size_t out_bufsz) {}

void fuse_ll_poll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                  struct fuse_pollhandle *ph) {}

void fuse_ll_write_buf(fuse_req_t req, fuse_ino_t ino, struct fuse_bufvec *bufv,
                       off_t off, struct fuse_file_info *fi) {}

void fuse_ll_retrieve_reply(fuse_req_t req, void *cookie, fuse_ino_t ino,
                            off_t offset, struct fuse_bufvec *bufv) {}

void fuse_ll_forget_multi(fuse_req_t req, size_t count,
                          struct fuse_forget_data *forgets) {}

void fuse_ll_flock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                   int op) {}

void fuse_ll_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset,
                       off_t length, struct fuse_file_info *fi) {}

#ifdef __APPLE__

void fuse_ll_reserved00(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *) {}
void fuse_ll_reserved01(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *) {}
void fuse_ll_reserved02(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *) {}
void fuse_ll_reserved03(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *) {}

void fuse_ll_setvolname(fuse_req_t req, const char *name) {}

void fuse_ll_exchange(fuse_req_t req, fuse_ino_t parent, const char *name,
                      fuse_ino_t newparent, const char *newname,
                      unsigned long options) {}

void fuse_ll_getxtimes(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info *) {}

void fuse_ll_setattr_x(fuse_req_t req, fuse_ino_t ino, struct setattr_x *attr,
                       int to_set, struct fuse_file_info *fi) {}
#endif /* __APPLE__ */

#ifndef _FDIR_FUSE_LL_H
#define _FDIR_FUSE_LL_H

#include <fuse3/fuse.h>
#include <fuse_lowlevel.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

/**
 * FUSE Low level filesystem operations
 *
 * Most of the methods (with the exception of init and destroy)
 * receive a request handle (fuse_req_t) as their first argument.
 * This handle must be passed to one of the specified reply functions.
 *
 * This may be done inside the method invocation, or after the call
 * has returned.  The request handle is valid until one of the reply
 * functions is called.
 *
 * Other pointer arguments (name, fuse_file_info, etc) are not valid
 * after the call has returned, so if they are needed later, their
 * contents have to be copied.
 *
 * The filesystem sometimes needs to handle a return value of -ENOENT
 * from the reply function, which means, that the request was
 * interrupted, and the reply discarded.  For example if
 * fuse_reply_open() return -ENOENT means, that the release method for
 * this file will not be called.
 */

/**
 * Initialize filesystem
 *
 * Called before any other filesystem method
 *
 * There's no reply to this function
 *
 * @param userdata the user data passed to fuse_lowlevel_new()
 */
void fuse_ll_init(void *userdata, struct fuse_conn_info *conn);

/**
 * Clean up filesystem
 *
 * Called on filesystem exit
 *
 * There's no reply to this function
 *
 * @param userdata the user data passed to fuse_lowlevel_new()
 */
void fuse_ll_destroy(void *userdata);

/**
 * Look up a directory entry by name and get its attributes.
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name the name to look up
 */
void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);

/**
 * Forget about an inode
 *
 * This function is called when the kernel removes an inode
 * from its internal caches.
 *
 * The inode's lookup count increases by one for every call to
 * fuse_reply_entry and fuse_reply_create. The nlookup parameter
 * indicates by how much the lookup count should be decreased.
 *
 * Inodes with a non-zero lookup count may receive request from
 * the kernel even after calls to unlink, rmdir or (when
 * overwriting an existing file) rename. Filesystems must handle
 * such requests properly and it is recommended to defer removal
 * of the inode until the lookup count reaches zero. Calls to
 * unlink, remdir or rename will be followed closely by forget
 * unless the file or directory is open, in which case the
 * kernel issues forget only after the release or releasedir
 * calls.
 *
 * Note that if a file system will be exported over NFS the
 * inodes lifetime must extend even beyond forget. See the
 * generation field in struct fuse_entry_param above.
 *
 * On unmount the lookup count for all inodes implicitly drops
 * to zero. It is not guaranteed that the file system will
 * receive corresponding forget messages for the affected
 * inodes.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 * @param ino the inode number
 * @param nlookup the number of lookups to forget
 */
void fuse_ll_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup);

/**
 * Get file attributes
 *
 * Valid replies:
 *   fuse_reply_attr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi for future use, currently always NULL
 */
void fuse_ll_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

/**
 * Set file attributes
 *
 * In the 'attr' argument only members indicated by the 'to_set'
 * bitmask contain valid values.  Other members contain undefined
 * values.
 *
 * If the setattr was invoked from the ftruncate() system call
 * under Linux kernel versions 2.6.15 or later, the fi->fh will
 * contain the value set by the open method or will be undefined
 * if the open method didn't set any value.  Otherwise (not
 * ftruncate call, or kernel version earlier than 2.6.15) the fi
 * parameter will be NULL.
 *
 * Valid replies:
 *   fuse_reply_attr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param attr the attributes
 * @param to_set bit mask of attributes which should be set
 * @param fi file information, or NULL
 *
 * Changed in version 2.5:
 *     file information filled in for ftruncate
 */
void fuse_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                     int to_set, struct fuse_file_info *fi);

/**
 * Read symbolic link
 *
 * Valid replies:
 *   fuse_reply_readlink
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 */
void fuse_ll_readlink(fuse_req_t req, fuse_ino_t ino);

/**
 * Create file node
 *
 * Create a regular file, character device, block device, fifo or
 * socket node.
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode file type and mode with which to create the new file
 * @param rdev the device number (only valid if created file is a device)
 */
void fuse_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                   mode_t mode, dev_t rdev);

/**
 * Create a directory
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode with which to create the new file
 */
void fuse_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                   mode_t mode);

/**
 * Remove a file
 *
 * If the file's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to remove
 */
void fuse_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);

/**
 * Remove a directory
 *
 * If the directory's inode's lookup count is non-zero, the
 * file system is expected to postpone any removal of the
 * inode until the lookup count reaches zero (see description
 * of the forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to remove
 */
void fuse_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);

/**
 * Create a symbolic link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param link the contents of the symbolic link
 * @param parent inode number of the parent directory
 * @param name to create
 */
void fuse_ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                     const char *name);

/** Rename a file
 *
 * If the target exists it should be atomically replaced. If
 * the target's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the old parent directory
 * @param name old name
 * @param newparent inode number of the new parent directory
 * @param newname new name
 */
void fuse_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                    fuse_ino_t newparent, const char *newname);

/**
 * Create a hard link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the old inode number
 * @param newparent inode number of the new parent directory
 * @param newname new name to create
 */
void fuse_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                  const char *newname);

/**
 * Open a file
 *
 * Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and
 * O_TRUNC) are available in fi->flags.
 *
 * Filesystem may store an arbitrary file handle (pointer, index,
 * etc) in fi->fh, and use this in other all other file operations
 * (read, write, flush, release, fsync).
 *
 * Filesystem may also implement stateless file I/O and not store
 * anything in fi->fh.
 *
 * There are also some flags (direct_io, keep_cache) which the
 * filesystem may set in fi, to change the way the file is opened.
 * See fuse_file_info structure in <fuse_common.h> for more details.
 *
 * Valid replies:
 *   fuse_reply_open
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void fuse_ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

/**
 * Read data
 *
 * Read should send exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the file
 * has been opened in 'direct_io' mode, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_iov
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size number of bytes to read
 * @param off offset to read from
 * @param fi file information
 */
void fuse_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                  struct fuse_file_info *fi);

/**
 * Write data
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the file has
 * been opened in 'direct_io' mode, in which case the return value
 * of the write system call will reflect the return value of this
 * operation.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param buf data to write
 * @param size number of bytes to write
 * @param off offset to write to
 * @param fi file information
 */
void fuse_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
                   off_t off, struct fuse_file_info *fi);

/**
 * Flush method
 *
 * This is called on each close() of the opened file.
 *
 * Since file descriptors can be duplicated (dup, dup2, fork), for
 * one open call there may be many flush calls.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * NOTE: the name of the method is misleading, since (unlike
 * fsync) the filesystem is not forced to flush pending writes.
 * One reason to flush data, is if the filesystem wants to return
 * write errors.
 *
 * If the filesystem supports file locking operations (setlk,
 * getlk) it should remove all locks belonging to 'fi->owner'.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void fuse_ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

/**
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open call there will be exactly one release call.
 *
 * The filesystem may reply with an error, but error values are
 * not returned to close() or munmap() which triggered the
 * release.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 * fi->flags will contain the same flags as for open.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void fuse_ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

/**
 * Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
void fuse_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                   struct fuse_file_info *fi);

/**
 * Open a directory
 *
 * Filesystem may store an arbitrary file handle (pointer, index,
 * etc) in fi->fh, and use this in other all other directory
 * stream operations (readdir, releasedir, fsyncdir).
 *
 * Filesystem may also implement stateless directory I/O and not
 * store anything in fi->fh, though that makes it impossible to
 * implement standard conforming directory stream operations in
 * case the contents of the directory can change between opendir
 * and releasedir.
 *
 * Valid replies:
 *   fuse_reply_open
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void fuse_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

/**
 * Read directory
 *
 * Send a buffer filled using fuse_add_direntry(), with size not
 * exceeding the requested size.  Send an empty buffer on end of
 * stream.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum number of bytes to send
 * @param off offset to continue reading the directory stream
 * @param fi file information
 */
void fuse_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                     struct fuse_file_info *fi);

/**
 * Release an open directory
 *
 * For every opendir call there will be exactly one releasedir
 * call.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void fuse_ll_releasedir(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info *fi);

/**
 * Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the directory
 * contents should be flushed, not the meta data.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
void fuse_ll_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                      struct fuse_file_info *fi);

/**
 * Get file system statistics
 *
 * Valid replies:
 *   fuse_reply_statfs
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number, zero means "undefined"
 */
void fuse_ll_statfs(fuse_req_t req, fuse_ino_t ino);

/**
 * Set an extended attribute
 *
 * Valid replies:
 *   fuse_reply_err
 */
#ifdef __APPLE__
void fuse_ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      const char *value, size_t size, int flags,
                      uint32_t position);
#else
void fuse_ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      const char *value, size_t size, int flags);
#endif

/**
 * Get an extended attribute
 *
 * If size is zero, the size of the value should be sent with
 * fuse_reply_xattr.
 *
 * If the size is non-zero, and the value fits in the buffer, the
 * value should be sent with fuse_reply_buf.
 *
 * If the size is too small for the value, the ERANGE error should
 * be sent.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_xattr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param name of the extended attribute
 * @param size maximum size of the value to send
 */
#ifdef __APPLE__
void fuse_ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      size_t size, uint32_t position);
#else
void fuse_ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                      size_t size);
#endif

/**
 * List extended attribute names
 *
 * If size is zero, the total size of the attribute list should be
 * sent with fuse_reply_xattr.
 *
 * If the size is non-zero, and the null character separated
 * attribute list fits in the buffer, the list should be sent with
 * fuse_reply_buf.
 *
 * If the size is too small for the list, the ERANGE error should
 * be sent.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_xattr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum size of the list to send
 */
void fuse_ll_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size);

/**
 * Remove an extended attribute
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param name of the extended attribute
 */
void fuse_ll_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name);

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param mask requested access mode
 */
void fuse_ll_access(fuse_req_t req, fuse_ino_t ino, int mask);

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * Open flags (with the exception of O_NOCTTY) are available in
 * fi->flags.
 *
 * Filesystem may store an arbitrary file handle (pointer, index,
 * etc) in fi->fh, and use this in other all other file operations
 * (read, write, flush, release, fsync).
 *
 * There are also some flags (direct_io, keep_cache) which the
 * filesystem may set in fi, to change the way the file is opened.
 * See fuse_file_info structure in <fuse_common.h> for more details.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 *
 * Valid replies:
 *   fuse_reply_create
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode file type and mode with which to create the new file
 * @param fi file information
 */
void fuse_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                    mode_t mode, struct fuse_file_info *fi);

/**
 * Test for a POSIX file lock
 *
 * Introduced in version 2.6
 *
 * Valid replies:
 *   fuse_reply_lock
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param lock the region/type to test
 */
void fuse_ll_getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                   struct flock *lock);

/**
 * Acquire, modify or release a POSIX file lock
 *
 * For POSIX threads (NPTL) there's a 1-1 relation between pid and
 * owner, but otherwise this is not always the case.  For checking
 * lock ownership, 'fi->owner' must be used.  The l_pid field in
 * 'struct flock' should only be used to fill in this field in
 * getlk().
 *
 * Note: if the locking methods are not implemented, the kernel
 * will still allow file locking to work locally.  Hence these are
 * only interesting for network filesystems and similar.
 *
 * Introduced in version 2.6
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param lock the region/type to set
 * @param sleep locking operation may sleep
 */
void fuse_ll_setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                   struct flock *lock, int sleep);

/**
 * Map block index within file to block index within device
 *
 * Note: This makes sense only for block device backed filesystems
 * mounted with the 'blkdev' option
 *
 * Introduced in version 2.6
 *
 * Valid replies:
 *   fuse_reply_bmap
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param blocksize unit of block index
 * @param idx block index within file
 */
void fuse_ll_bmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize,
                  uint64_t idx);

/**
 * Ioctl
 *
 * Note: For unrestricted ioctls (not allowed for FUSE
 * servers), data in and out areas can be discovered by giving
 * iovs and setting FUSE_IOCTL_RETRY in @flags.  For
 * restricted ioctls, kernel prepares in/out data area
 * according to the information encoded in cmd.
 *
 * Introduced in version 2.8
 *
 * Valid replies:
 *   fuse_reply_ioctl_retry
 *   fuse_reply_ioctl
 *   fuse_reply_ioctl_iov
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param cmd ioctl command
 * @param arg ioctl argument
 * @param fi file information
 * @param flags for FUSE_IOCTL_* flags
 * @param in_buf data fetched from the caller
 * @param in_bufsz number of fetched bytes
 * @param out_bufsz maximum size of output data
 */
void fuse_ll_ioctl(fuse_req_t req, fuse_ino_t ino, int cmd, void *arg,
                   struct fuse_file_info *fi, unsigned flags,
                   const void *in_buf, size_t in_bufsz, size_t out_bufsz);

/**
 * Poll for IO readiness
 *
 * Introduced in version 2.8
 *
 * Note: If ph is non-NULL, the client should notify
 * when IO readiness events occur by calling
 * fuse_lowelevel_notify_poll() with the specified ph.
 *
 * Regardless of the number of times poll with a non-NULL ph
 * is received, single notification is enough to clear all.
 * Notifying more times incurs overhead but doesn't harm
 * correctness.
 *
 * The callee is responsible for destroying ph with
 * fuse_pollhandle_destroy() when no longer in use.
 *
 * Valid replies:
 *   fuse_reply_poll
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param ph poll handle to be used for notification
 */
void fuse_ll_poll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                  struct fuse_pollhandle *ph);

/**
 * Write data made available in a buffer
 *
 * This is a more generic version of the ->write() method.  If
 * FUSE_CAP_SPLICE_READ is set in fuse_conn_info.want and the
 * kernel supports splicing from the fuse device, then the
 * data will be made available in pipe for supporting zero
 * copy data transfer.
 *
 * buf->count is guaranteed to be one (and thus buf->idx is
 * always zero). The write_buf handler must ensure that
 * bufv->off is correctly updated (reflecting the number of
 * bytes read from bufv->buf[0]).
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param bufv buffer containing the data
 * @param off offset to write to
 * @param fi file information
 */
void fuse_ll_write_buf(fuse_req_t req, fuse_ino_t ino, struct fuse_bufvec *bufv,
                       off_t off, struct fuse_file_info *fi);

/**
 * Callback function for the retrieve request
 *
 * Valid replies:
 *	fuse_reply_none
 *
 * @param req request handle
 * @param cookie user data supplied to fuse_lowlevel_notify_retrieve()
 * @param ino the inode number supplied to fuse_lowlevel_notify_retrieve()
 * @param offset the offset supplied to fuse_lowlevel_notify_retrieve()
 * @param bufv the buffer containing the returned data
 */
void fuse_ll_retrieve_reply(fuse_req_t req, void *cookie, fuse_ino_t ino,
                            off_t offset, struct fuse_bufvec *bufv);

/**
 * Forget about multiple inodes
 *
 * See description of the forget function for more
 * information.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 */
void fuse_ll_forget_multi(fuse_req_t req, size_t count,
                          struct fuse_forget_data *forgets);

/**
 * Acquire, modify or release a BSD file lock
 *
 * Note: if the locking methods are not implemented, the kernel
 * will still allow file locking to work locally.  Hence these are
 * only interesting for network filesystems and similar.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param op the locking operation, see flock(2)
 */
void fuse_ll_flock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
                   int op);

/**
 * Allocate requested space. If this function returns success then
 * subsequent writes to the specified range shall not fail due to the lack
 * of free space on the file system storage media.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param offset starting point for allocated region
 * @param length size of allocated region
 * @param mode determines the operation to be performed on the given range,
 *             see fallocate(2)
 */
void fuse_ll_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset,
                       off_t length, struct fuse_file_info *fi);

#ifdef __APPLE__

void fuse_ll_reserved00(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *);
void fuse_ll_reserved01(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *);
void fuse_ll_reserved02(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *);
void fuse_ll_reserved03(fuse_req_t req, fuse_ino_t ino, void *, void *, void *,
                        void *, void *, void *);

void fuse_ll_setvolname(fuse_req_t req, const char *name);

void fuse_ll_exchange(fuse_req_t req, fuse_ino_t parent, const char *name,
                      fuse_ino_t newparent, const char *newname,
                      unsigned long options);

void fuse_ll_getxtimes(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *);

void fuse_ll_setattr_x(fuse_req_t req, fuse_ino_t ino, struct setattr_x *attr,
                       int to_set, struct fuse_file_info *fi);
#endif /* __APPLE__ */

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

const static struct fuse_lowlevel_ops fuse_ll_oper = {
    .init = fuse_ll_init,
    .destroy = NULL,
    .lookup = fuse_ll_lookup,
    .forget = fuse_ll_forget,
    .getattr = fuse_ll_getattr,
    .setattr = fuse_ll_setattr,
    .readlink = fuse_ll_readlink,
    .mknod = fuse_ll_mknod,
    .mkdir = fuse_ll_mkdir,
    .unlink = fuse_ll_unlink,
    .rmdir = fuse_ll_rmdir,
    .symlink = fuse_ll_symlink,
    // .rename = fuse_ll_rename,
    .link = fuse_ll_link,
    .open = fuse_ll_open,
    .read = fuse_ll_read,
    .write = fuse_ll_write,
    .flush = fuse_ll_flush,
    .release = fuse_ll_release,
    .fsync = fuse_ll_fsync,
    .opendir = fuse_ll_opendir,
    .readdir = fuse_ll_readdir,
    .releasedir = fuse_ll_releasedir,
    .fsyncdir = fuse_ll_fsyncdir,
    .statfs = fuse_ll_statfs,
    .setxattr = fuse_ll_setxattr,
    // .getxattr = fuse_ll_getxattr,
    .listxattr = fuse_ll_listxattr,
    .removexattr = fuse_ll_removexattr,
    .access = fuse_ll_access,
    .create = fuse_ll_create,
    .getlk = fuse_ll_getlk,
    .setlk = fuse_ll_setlk,
    .bmap = NULL,
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
#ifdef FUSE_IOCTL_COMPAT
    .ioctl = fuse_ll_ioctl,
#else
    .ioctl = NULL,
#endif
    .poll = NULL,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 9)
    .write_buf = NULL,
    .retrieve_reply = NULL,
    .forget_multi = NULL,
    .flock = fuse_ll_flock,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 9)
    .fallocate = fuse_ll_fallocate
#endif
};

#endif  // _FDIR_FUSE_LL_H

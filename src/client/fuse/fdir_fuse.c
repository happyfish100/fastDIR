#include "fdir_fuse_ll.h"
#include "fdir_fuse_ino_manager.h"
#include "fdir_client_ops.h"
#include "sf/sf_util.h"

void print_help(const char *cmd_name) {
    printf("usage: %s [options] <mountpoint>\n\n", cmd_name);
    fuse_cmdline_help();
    fuse_lowlevel_help();
}

void show_version() {
    printf("FUSE library version %s\n", fuse_pkgversion());
    fuse_lowlevel_version();
}

int start_fuse_ll(int argc, char *argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_session *se;
    struct fuse_cmdline_opts opts;
    int ret = -1;

    if (fuse_parse_cmdline(&args, &opts) != 0) return 1;
    if (opts.show_help) {
        print_help(argv[0]);
        ret = 0;
        goto err_out1;
    } else if (opts.show_version) {
        show_version();
        ret = 0;
        goto err_out1;
    }

    if (opts.mountpoint == NULL) {
        print_help(argv[0]);
        ret = 1;
        goto err_out1;
    }

    se = fuse_session_new(&args, &fuse_ll_oper, sizeof(fuse_ll_oper), NULL);
    if (se == NULL) goto err_out1;

    if (fuse_set_signal_handlers(se) != 0) goto err_out2;

    if (fuse_session_mount(se, opts.mountpoint) != 0) goto err_out3;

    fuse_daemonize(opts.foreground);

    /* Block until ctrl+c or fusermount -u */
    if (opts.singlethread)
        ret = fuse_session_loop(se);
    else
        ret = fuse_session_loop_mt(se, opts.clone_fd);

    fuse_session_unmount(se);
err_out3:
    fuse_remove_signal_handlers(se);
err_out2:
    fuse_session_destroy(se);
err_out1:
    free(opts.mountpoint);
    fuse_opt_free_args(&args);

    return ret ? 1 : 0;
}

int main(int argc, char *argv[]) {
    int res;

    log_init();
    g_log_context.log_level = LOG_DEBUG;

    if (fco_init() != 0) {
        lerr("fco_init init failed");
        return 1;
    }

    res = ino_manager_init();
    if (res) {
        lerr("ino_manager_init failed.");
        return res;
    }

    res = start_fuse_ll(argc, argv);

    ino_manager_destroy();
    return res;
}

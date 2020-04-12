#include "fdir_client_ops.h"
#include <stdlib.h>
#include "sf/sf_util.h"

const static char *ns = "test";
static uid_t mount_uid;
static gid_t mount_gid;

int fco_init() {
    const char *config_filename;
    config_filename = getenv("FDIR_CLIENT_CONFIG_PATH");
    if (config_filename) {
        linfo("loading config from %s which set by FDIR_CLIENT_CONFIG_PATH",
              config_filename);
    } else {
        config_filename = "/etc/fdir/fuse.conf";
        linfo(
            "FDIR_CLIENT_CONFIG_PATH is not set in env, loading config from %s",
            config_filename);
    }
    if (fdir_client_init(config_filename) != 0) {
        linfo("fdir_client_init failed");
        return -1;
    }

    mount_uid = getuid();
    mount_gid = getgid();

    return 0;
}

int fco_destroy() { return 0; }

void fco_set_default_stat(struct stat *st) {
    memset(st, 0, sizeof(*st));
    st->st_mode = S_IFDIR | 0755;
    st->st_blocks = 1;
    st->st_uid = mount_uid;
    st->st_gid = mount_gid;
}

int fco_stat(const char *full_path, struct stat *st) {
    FdirFuseDirptr *dp;

    ldebug("fco_stat: full_path=%s", full_path);
    dp = fco_opendir(full_path);
    if (dp == NULL) {
        return ENOENT;
    }

    fco_set_default_stat(st);

    fco_closedir(dp);
    return 0;
}

FdirFuseDirptr *fco_opendir(const char *full_path) {
    int result;
    FDIRDEntryFullName entry_info;
    FDIRClientDentryArray *array;
    FdirFuseDirptr *dp;

    ldebug("fco_opendir: ng=%s, full_path=%s", ns, full_path);
    FC_SET_STRING(entry_info.ns, (char *)ns);
    FC_SET_STRING(entry_info.path, (char *)full_path);
    dp = (FdirFuseDirptr *)malloc(sizeof(*dp));
    if (dp == NULL) {
        lerr("fco_opendir failed. ENOMEM");
        return NULL;
    }
    array = &(dp->dentry_array);
    if ((result = fdir_client_dentry_array_init(array)) != 0) {
        lerr("fdir_client_dentry_array_init failed. res=%d", result);
        free(dp);
        return NULL;
    }

    result = fdir_client_list_dentry(&g_client_global_vars.client_ctx,
                                     &entry_info, array);
    if (result != 0) {
        lerr("fdir_client_list_dentry failed, errno: %d", result);
        free(dp);
        return NULL;
    }
    return dp;
}

void fco_closedir(FdirFuseDirptr *dp) {
    ldebug("fco_closedir: dp=%p", dp);
    free(dp);
}

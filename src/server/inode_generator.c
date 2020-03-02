#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <limits.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "server_global.h"
#include "inode_generator.h"

#define INODE_SN_FILENAME          ".inode.sn"
#define INODE_SN_FLUSH_FREQUENCE   10000

#define GET_INODE_SN_FILENAME(filename, size) \
    snprintf(filename, size, "%s/%s", DATA_PATH_STR, INODE_SN_FILENAME)

static int64_t inode_cluster = 0;

#define write_to_inode_sn_file()  write_to_inode_sn_file_func(NULL)

static int write_to_inode_sn_file_func(void *args)
{
    static volatile int64_t last_sn = -1;
    char filename[PATH_MAX];
    char buff[256];
    int len;

    if (CURRENT_INODE_SN == last_sn) {
        return 0;
    }

    last_sn = __sync_add_and_fetch(&CURRENT_INODE_SN, 0);
    len = sprintf(buff, "%"PRId64, last_sn);
    GET_INODE_SN_FILENAME(filename, sizeof(filename));
    return safeWriteToFile(filename, buff, len);
}

static int setup_inode_sn_flush_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 1, write_to_inode_sn_file_func, NULL);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

int inode_generator_init()
{
    char filename[PATH_MAX];
    int result;

    GET_INODE_SN_FILENAME(filename, sizeof(filename));
    if (access(filename, F_OK) == 0) {
        char content[32];
        char *endptr;
        int64_t file_size;

        file_size = sizeof(content);
        if ((result=getFileContentEx(filename, content, 0, &file_size)) != 0) {
            return result;
        }
        endptr = NULL;
        CURRENT_INODE_SN = strtoll(content, &endptr, 10);
        if (!(endptr == NULL || *endptr == '\0')) {
            logError("file: "__FILE__", line: %d, "
                    "inode sn filename: %s, invalid inode sn: %s",
                    __LINE__, filename, content);
            return EINVAL;
        }
        CURRENT_INODE_SN += INODE_SN_FLUSH_FREQUENCE;  //skip avoid conflict
    }

    inode_cluster = ((int64_t)CLUSTER_ID) << (63 - FDIR_CLUSTER_ID_BITS);
	return setup_inode_sn_flush_task();
}

void inode_generator_destroy()
{
    write_to_inode_sn_file();
}

int64_t inode_generator_next()
{
    int64_t sn;
    sn = __sync_add_and_fetch(&CURRENT_INODE_SN, 1);
    if (sn % INODE_SN_FLUSH_FREQUENCE == 0) {
        write_to_inode_sn_file();
    }

    return inode_cluster | sn;
}

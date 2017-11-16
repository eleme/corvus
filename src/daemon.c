#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include "corvus.h"
#include "logging.h"
#include "daemon.h"

int daemonize() {
    int status;
    pid_t pid, sid;
    int fd;

    pid = fork();
    switch (pid) {
        case -1:
            LOG(ERROR, "fork() failed: %s", strerror(errno));
            return CORVUS_ERR;
        case 0:
            break;
        default:
            /* parent */
            exit(0);
    }

    /* The first child is the session leader */
    sid = setsid();
    if (sid < 0) {
        LOG(ERROR, "setsid() failed: %s", strerror(errno));
        return CORVUS_ERR;
    }
    
    pid = fork();
    switch (pid) {
        case -1:
            LOG(ERROR, "fork() failed: %s", strerror(errno));
            return CORVUS_ERR;
        case 0:
            break;
        default:
            /* the first child terminates */
            exit(0);
    }

    /* the second child continues */
    umask(0);

    /* redirect stdin,stdout,stderr to /dev/null */
    fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        LOG(ERROR, "open(\"/dev/null\") failed: %s", strerror(errno));
        return CORVUS_ERR;
    }

    status = dup2(fd, STDIN_FILENO);
    if (status < 0) {
        LOG(ERROR, "dup2(%d, STDIN) failed: %s", fd, strerror(errno));
        close(fd);
        return CORVUS_ERR;
    }
    status = dup2(fd, STDOUT_FILENO);
    if (status < 0) {
        LOG(ERROR, "dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
        close(fd);
        return CORVUS_ERR;
    }
    status = dup2(fd, STDERR_FILENO);
    if (status < 0) {
        LOG(ERROR, "dup2(%d, STDERR) failed: %s", fd, strerror(errno));
        close(fd);
        return CORVUS_ERR;
    }
    if (fd > STDERR_FILENO) {
        status = close(fd);
        if (status < 0) {
            LOG(ERROR, "close(%d) failed: %s", fd, strerror(errno));
            close(fd);
            return CORVUS_ERR;
        }
    }
    return CORVUS_OK;
}

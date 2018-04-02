#include <stdio.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <time.h>
#include "corvus.h"
#include "logging.h"

static const char *LEVEL_MAP[] = {"DEBUG", "INFO", "WARN", "ERROR", "CRIT"};
static const int SYSLOG_LEVEL_MAP[] = {LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERR, LOG_CRIT};

void logger(const char *file, int line, int level, const char *fmt, ...)
{
    va_list ap;
    char msg[MAX_LOG_LEN];
    char timestamp[64];
    struct timeval now;

    if (level < ATOMIC_GET(config.loglevel)) return;

    pid_t thread_id = (pid_t)syscall(SYS_gettid);
    pid_t process_id = getpid();

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    if (config.syslog) {
        syslog(SYSLOG_LEVEL_MAP[level], "[%s %s:%d %d %d] %s", config.cluster,
            config.bind, (int)config.port, (int)process_id, (int)thread_id, msg);
    } else {
        gettimeofday(&now, NULL);
        int n = strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S,",
                        localtime(&now.tv_sec));
        snprintf(timestamp + n, sizeof(timestamp) - n, "%03d", (int)now.tv_usec/1000);
        fprintf(stderr, "%s %s [%s %s:%d %d %d]: %s (%s:%d)\n", timestamp, LEVEL_MAP[level],
            config.cluster, config.bind, (int)config.port, (int)process_id, (int)thread_id, msg, file, line);
    }
}

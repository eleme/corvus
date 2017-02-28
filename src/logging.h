#ifndef LOGGING_H
#define LOGGING_H

#include <stdbool.h>
#include <syslog.h>

#define MAX_LOG_LEN 1024

#define LOG(...) logger(__FILE__, __LINE__, __VA_ARGS__)

enum {
    DEBUG,
    INFO,
    WARN,
    ERROR,
    CRIT,
};

struct context;

void log_init(struct context *ctx);
void logger(const char *file, int line, int level, const char *fmt, ...);

#define LOG_LEVEL_STR(loglevel) (  \
    loglevel == DEBUG ? "debug" :  \
    loglevel == INFO  ? "info"  :  \
    loglevel == WARN  ? "warn"  :  \
    loglevel == ERROR ? "error" :  \
    loglevel == CRIT  ? "crit"  :  \
                        "invalid_level") \

#endif /* end of include guard: LOGGING_H */

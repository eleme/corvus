#ifndef __LOGGING_H
#define __LOGGING_H

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

#endif /* end of include guard: __LOGGING_H */

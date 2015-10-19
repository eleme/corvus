#ifndef __LOGGING_H
#define __LOGGING_H

#include <stdbool.h>
#include <syslog.h>

#define MAX_LOG_LEN 1024

enum {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

struct context;

void log_init(struct context *ctx);
void logger(int level, const char *fmt, ...);
void log_destroy();

#endif /* end of include guard: __LOGGING_H */

#ifndef __CONNECTION_H
#define __CONNECTION_H

#include "mbuf.h"

struct event_loop;
struct context;

struct connection {
    struct context *ctx;
    int fd;
    struct mhdr buf_queue;
    void (*ready)(struct connection *self, struct event_loop *loop, uint32_t mask);
};

#endif /* end of include guard: __CONNECTION_H */

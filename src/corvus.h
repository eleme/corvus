#ifndef __MAIN_H
#define __MAIN_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "mbuf.h"
#include "command.h"

#include "hashmap/hash.h"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4

struct node_conf {
    char **nodes;
    size_t len;
};

struct context {
    /* buffer related */
    uint32_t nfree_mbufq;
    struct mhdr free_mbufq;
    size_t mbuf_chunk_size;
    size_t mbuf_offset;

    /* logging */
    bool syslog;
    int log_level;

    /* connection pool */
    hash_t *server_table;

    /* event */
    struct event_loop *loop;
};

void context_init(struct context *ctx, bool syslog, int log_level);

struct node_conf initial_nodes;

#endif /* end of include guard: __MAIN_H */

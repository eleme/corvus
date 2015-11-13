#ifndef __MAIN_H
#define __MAIN_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "mbuf.h"
#include "command.h"
#include "connection.h"
#include "stats.h"

#include "hashmap/hash.h"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4

#define THREAD_STACK_SIZE (1024*1024*4)

#define MIN(a, b) ((a) > (b) ? (b) : (a))

#define VERSION "0.1"

enum thread_role {
    THREAD_UNKNOWN,
    THREAD_MAIN_WORKER,
    THREAD_SLOT_UPDATER,
};

struct node_conf {
    char **nodes;
    int len;
};

struct context {
    /* buffer related */
    uint32_t nfree_mbufq;
    struct mhdr free_mbufq;
    size_t mbuf_chunk_size;
    size_t mbuf_offset;

    uint32_t nfree_cmdq;
    struct cmd_tqh free_cmdq;

    uint32_t nfree_connq;
    struct conn_tqh free_connq;

    struct connection *proxy;

    /* logging */
    bool syslog;
    int log_level;

    /* connection pool */
    hash_t *server_table;
    struct node_conf *node_conf;

    /* event */
    struct event_loop *loop;

    /* thread control */
    int quit;
    pthread_t thread;
    bool started;
    enum thread_role role;
    struct connection *notifier;
};

#endif /* end of include guard: __MAIN_H */

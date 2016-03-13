#ifndef __CORVUS_H
#define __CORVUS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "mbuf.h"
#include "command.h"
#include "connection.h"
#include "stats.h"
#include "dict.h"
#include "event.h"

#define VERSION "0.1.1"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4
#define CORVUS_ASKING -5

#define THREAD_STACK_SIZE (1024*1024*4)
#define MIN(a, b) ((a) > (b) ? (b) : (a))

#define CLUSTER_NAME_SIZE 127

enum thread_role {
    THREAD_UNKNOWN,
    THREAD_MAIN_WORKER,
    THREAD_SLOT_UPDATER,
};

enum {
    CTX_UNKNOWN,
    CTX_QUIT,
    CTX_BEFORE_QUIT,
    CTX_QUITTING,
};

struct node_conf {
    struct address *addr;
    int len;
};

struct context {
    /* buffer related */
    uint32_t nfree_mbufq;
    struct mhdr free_mbufq;
    size_t mbuf_offset;

    uint32_t nfree_cmdq;
    struct cmd_tqh free_cmdq;

    struct connection proxy;
    struct connection timer;

    /* connection pool */
    struct dict server_table;
    struct conn_tqh conns;
    uint32_t nfree_connq;
    struct conn_info_tqh free_conn_infoq;
    uint32_t nfree_conn_infoq;

    struct conn_tqh servers;

    /* event */
    struct event_loop loop;

    /* thread control */
    int state;
    pthread_t thread;
    bool started;
    enum thread_role role;

    /* stats */
    struct basic_stats stats;
    double last_command_latency;
};

struct {
    char cluster[CLUSTER_NAME_SIZE + 1];
    uint16_t bind;
    struct node_conf node;
    int thread;
    int loglevel;
    bool syslog;
    char statsd_addr[DSN_LEN + 1];
    int metric_interval;
    int stats;
    int64_t client_timeout;
    int64_t server_timeout;
    int bufsize;
} config;

int64_t get_time();
struct context *get_contexts();

#endif /* end of include guard: __CORVUS_H */

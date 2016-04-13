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

#define VERSION "0.2.1"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4
#define CORVUS_ASKING -5

#define THREAD_STACK_SIZE (1024*1024*4)
#define MIN(a, b) ((a) > (b) ? (b) : (a))

#define CLUSTER_NAME_SIZE 127

#define ATOMIC_GET(data) \
    __atomic_load_n(&(data), __ATOMIC_SEQ_CST)

#define ATOMIC_IGET(data, value) \
    __atomic_exchange_n(&(data), value, __ATOMIC_SEQ_CST)

#define ATOMIC_SET(data, value) \
    __atomic_store_n(&(data), value, __ATOMIC_SEQ_CST)

#define ATOMIC_INC(data, value) \
    __atomic_add_fetch(&(data), value, __ATOMIC_SEQ_CST)

#define ATOMIC_DEC(data, value) \
    __atomic_sub_fetch(&(data), value, __ATOMIC_SEQ_CST)

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
    size_t mbuf_offset;

    struct mhdr free_mbufq;
    struct cmd_tqh free_cmdq;
    struct conn_info_tqh free_conn_infoq;
    struct buf_time_tqh free_buf_timeq;

    struct connection proxy;
    struct connection timer;

    /* connection pool */
    struct dict server_table;
    struct conn_tqh conns;

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
    struct memory_stats mstats;
    long long last_command_latency;
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

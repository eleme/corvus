#ifndef CORVUS_H
#define CORVUS_H

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
#include "slowlog.h"
#include "config.h"

#define VERSION "0.2.7"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4
#define CORVUS_ASKING -5
#define CORVUS_READONLY -6

#define RET_NOT_OK(expr)               \
    do {                               \
        int r = expr;                  \
        if (r != CORVUS_OK) return r;  \
    } while (0)


#define THREAD_STACK_SIZE (1024*1024*4)
#define MIN(a, b) ((a) > (b) ? (b) : (a))

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

enum {
    CTX_UNKNOWN,
    CTX_QUIT,
    CTX_BEFORE_QUIT,
    CTX_QUITTING,
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

    unsigned int seed;

    struct conn_tqh servers;

    /* event */
    struct event_loop loop;

    /* thread control */
    int state;
    pthread_t thread;

    /* stats */
    struct basic_stats stats;
    struct memory_stats mstats;
    long long last_command_latency;

    /* slowlog */
    struct slowlog_queue slowlog;
};

int64_t get_time();
struct context *get_contexts();
int thread_spawn(struct context *ctx, void *(*start_routine) (void *));

#endif /* end of include guard: CORVUS_H */

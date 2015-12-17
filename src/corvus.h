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

#define VERSION "0.0.1"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4
#define CORVUS_ASKING -5

#define THREAD_STACK_SIZE (1024*1024*4)
#define MIN(a, b) ((a) > (b) ? (b) : (a))

#define ARRAY_CHUNK_SIZE 1024
#define RECYCLE_SIZE 64

#define NAME_LEN 127

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
    struct dict *server_table;

    struct conn_tqh servers;
    struct node_conf *node_conf;

    /* event */
    struct event_loop *loop;

    /* thread control */
    int quit;
    pthread_t thread;
    bool started;
    enum thread_role role;
    struct connection *notifier;

    /* stats */
    struct basic_stats stats;
    double last_command_latency;
};

struct {
    char cluster_name[NAME_LEN + 1];
    uint16_t bind;
    struct node_conf node;
    int thread;
    int loglevel;
    int syslog;
    char statsd_addr[DSN_MAX + 1];
    int metric_interval;
    int stats;
} config;

char *cluster_name;

double get_time();
struct context *get_contexts();

#endif /* end of include guard: __CORVUS_H */

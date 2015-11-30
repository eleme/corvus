#ifndef __CORVUS_H
#define __CORVUS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "mbuf.h"
#include "command.h"
#include "connection.h"

#include "hashmap/hash.h"

#define VERSION "0.0.1"

#define CORVUS_OK 0
#define CORVUS_ERR -1
#define CORVUS_AGAIN -2
#define CORVUS_EOF -3
#define CORVUS_INPROGRESS -4

#define THREAD_STACK_SIZE (1024*1024*4)
#define MIN(a, b) ((a) > (b) ? (b) : (a))

#define ARRAY_CHUNK_SIZE 1024

enum thread_role {
    THREAD_UNKNOWN,
    THREAD_MAIN_WORKER,
    THREAD_SLOT_UPDATER,
};

struct node_conf {
    char **nodes;
    int len;
};

struct basic_stats {
    long long connected_clients;
    long long completed_commands;
    long long recv_bytes;
    long long send_bytes;

    double remote_latency;
    double total_latency;

    long long buffers;
};

struct stats {
    pid_t pid;
    int threads;

    double used_cpu_sys;
    double used_cpu_user;

    double *last_command_latency;
    char *remote_nodes;

    struct basic_stats basic;

    long long free_buffers;
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

    uint32_t nfree_redis_dataq;
    struct redis_data_tqh free_redis_dataq;

    struct connection *proxy;

    /* logging */
    bool syslog;
    int log_level;

    /* connection pool */
    hash_t *server_table;

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

double get_time();
int get_thread_num();
struct context *get_contexts();
int get_bind();

#endif /* end of include guard: __CORVUS_H */

#ifndef __STATS_H
#define __STATS_H

#include <sys/types.h>
#include "socket.h"

#define ADDR_MAX 1024
#define ADDR_LIST_MAX (ADDR_MAX * DSN_LEN)

struct basic_stats {
    long long connected_clients;
    long long completed_commands;
    long long recv_bytes;
    long long send_bytes;

    long long remote_latency;
    long long total_latency;

    long long buffers;
    long long cmds;
    long long conns;
    long long conn_info;
};

struct stats {
    pid_t pid;
    int threads;

    double used_cpu_sys;
    double used_cpu_user;

    long long last_command_latency[ADDR_MAX];
    char remote_nodes[ADDR_LIST_MAX];

    struct basic_stats basic;

    long long free_buffers;
    long long free_cmds;
    long long free_conns;
    long long free_conn_info;
};

int stats_init(int interval);
void stats_kill();
int stats_resolve_addr(char *addr);
void stats_get(struct stats *stats);

#endif /* end of include guard: __STATS_H */

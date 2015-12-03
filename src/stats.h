#ifndef __STATS_H
#define __STATS_H

#include <sys/types.h>

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

int stats_init(int interval);
void stats_kill();
int stats_resolve_addr(char *addr);
void stats_get(struct stats *stats);

#endif /* end of include guard: __STATS_H */

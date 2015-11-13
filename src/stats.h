#ifndef STATS_H
#define STATS_H

struct {
    int pid;
    int recv_bytes;
    int send_bytes;
    int total_commands;
    int thread;
} stats;

void stats_init();

#endif

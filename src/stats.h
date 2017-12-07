#ifndef STATS_H
#define STATS_H

#include <sys/types.h>
#include "socket.h"
#include "slot.h"

struct memory_stats {
    long long buffers;          // 表示正在使用的缓冲区的数量
    long long cmds;             // 表示正在使用的command对象的数量
    long long conns;
    long long conn_info;
    long long buf_times;        // 表示正在使用的buf_time的数量

    long long free_buffers;     // 表示空闲的缓冲区数量, 大小与context的free_mbufq队列长度保持一致
    long long free_cmds;        // 表示空闲的command对象的数量, 大小与context的free_cmdq队列长度保持一致
    long long free_conns;
    long long free_conn_info;
    long long free_buf_times;   // 表示空闲的buf_time对象的数量, 大小与context的free_buf_timeq队列长度保持一致
};

struct basic_stats {
    long long connected_clients;
    long long completed_commands;
    long long slot_update_jobs;
    long long recv_bytes;       // corvus从客户端接收请求的字节数
    long long send_bytes;

    long long remote_latency;
    long long total_latency;

    long long ask_recv;
    long long moved_recv;
};

struct stats {
    double used_cpu_sys;
    double used_cpu_user;

    long long last_command_latency[MAX_NODE_LIST];
    char remote_nodes[MAX_NODE_LIST * ADDRESS_LEN];

    struct basic_stats basic;
};

int stats_init();
void stats_kill();
int stats_resolve_addr(char *addr);
void stats_get(struct stats *stats);
void stats_get_memory(struct memory_stats *stats);

void incr_slot_update_counter();

#endif /* end of include guard: STATS_H */

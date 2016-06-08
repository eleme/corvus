#include <string.h>
#include <sys/resource.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include "stats.h"
#include "corvus.h"
#include "socket.h"
#include "logging.h"
#include "slot.h"
#include "alloc.h"

#define HOST_LEN 255

struct bytes {
    char key[ADDRESS_LEN + 1];
    long long recv;
    long long send;
    long long completed;
};

static int statsd_fd = -1;
static struct sockaddr_in dest;
static struct dict bytes_map;
static char hostname[HOST_LEN + 1];

// context for stats thread, no need to init,
// only used `stats_ctx.thread` currently
static struct context stats_ctx;

static struct stats cumulation;
static struct {
    double sys;
    double user;
} used_cpu;

static struct dict slow_counts; // node dsn => slow cmd counts


static void stats_free_slow_counts()
{
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slow_counts, &iter) {
        cv_free(iter.value);
    }

    dict_free(&slow_counts);
}

static inline void stats_get_cpu_usage(struct stats *stats)
{
    struct rusage ru;
    memset(&ru, 0, sizeof(ru));
    getrusage(RUSAGE_SELF, &ru);

    stats->used_cpu_sys = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1000000.0;
    stats->used_cpu_user = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1000000.0;
}

static inline void stats_cumulate(struct stats *stats)
{
    cumulation.basic.completed_commands += stats->basic.completed_commands;
    cumulation.basic.remote_latency += stats->basic.remote_latency;
    cumulation.basic.total_latency += stats->basic.total_latency;
    cumulation.basic.recv_bytes += stats->basic.recv_bytes;
    cumulation.basic.send_bytes += stats->basic.send_bytes;
}

static void stats_send(char *metric, double value)
{
    if (statsd_fd == -1) {
        statsd_fd = socket_create_udp_client();
    }

    int n;
    const char *fmt = "corvus.%s.%s-%d.%s:%f|g";
    n = snprintf(NULL, 0, fmt, config.cluster, hostname, config.bind, metric, value);
    char buf[n + 1];
    snprintf(buf, sizeof(buf), fmt, config.cluster, hostname, config.bind, metric, value);
    if (sendto(statsd_fd, buf, n, 0, (struct sockaddr*)&dest, sizeof(dest)) == -1) {
        LOG(WARN, "fail to send metrics data: %s", strerror(errno));
    }
}

void stats_get_memory(struct memory_stats *stats)
{
    struct context *contexts = get_contexts();

    for (int i = 0; i < config.thread; i++) {
        stats->buffers        += contexts[i].mstats.buffers;
        stats->conns          += contexts[i].mstats.conns;
        stats->cmds           += contexts[i].mstats.cmds;
        stats->conn_info      += contexts[i].mstats.conn_info;
        stats->buf_times      += contexts[i].mstats.buf_times;
        stats->free_buffers   += contexts[i].mstats.free_buffers;
        stats->free_cmds      += contexts[i].mstats.free_cmds;
        stats->free_conns     += contexts[i].mstats.free_conns;
        stats->free_conn_info += contexts[i].mstats.free_conn_info;
        stats->free_buf_times += contexts[i].mstats.free_buf_times;
    }
}

void stats_get_simple(struct stats *stats, bool reset)
{
    if (!reset) {
        // TODO use lock to protect `cumulation`?
        memcpy(stats, &cumulation, sizeof(cumulation));
    }

    stats_get_cpu_usage(stats);
    if (reset) {
        double temp_sys = stats->used_cpu_sys;
        double temp_user = stats->used_cpu_user;

        stats->used_cpu_sys -= used_cpu.sys;
        stats->used_cpu_user -= used_cpu.user;

        used_cpu.sys = temp_sys;
        used_cpu.user = temp_user;
    }

    struct context *contexts = get_contexts();

#define STATS_ASSIGN(field) \
    stats->basic.field += reset ? \
        ATOMIC_IGET(contexts[i].stats.field, 0) : \
        ATOMIC_GET(contexts[i].stats.field)

    for (int i = 0; i < config.thread; i++) {
        STATS_ASSIGN(completed_commands);
        STATS_ASSIGN(remote_latency);
        STATS_ASSIGN(total_latency);
        STATS_ASSIGN(recv_bytes);
        STATS_ASSIGN(send_bytes);
        stats->basic.connected_clients += ATOMIC_GET(contexts[i].stats.connected_clients);
    }

    if (reset) {
        stats_cumulate(stats);
    }
}

void stats_node_info_agg(struct bytes *bytes)
{
    struct bytes *b = NULL;
    struct connection *server;
    struct context *contexts = get_contexts();
    int j, n, m = 0;

    for (int i = 0; i < config.thread; i++) {
        TAILQ_FOREACH(server, &contexts[i].servers, next) {
            n = strlen(server->info->addr.ip);
            if (n <= 0) continue;

            char ip[n + 8];
            for (j = 0; j < n; j++) {
                ip[j] = server->info->addr.ip[j];
                if (ip[j] == '.') ip[j] = '-';
            }
            sprintf(ip + j, "-%d", server->info->addr.port);

            b = dict_get(&bytes_map, ip);
            if (b == NULL) {
                b = &bytes[m++];
                strncpy(b->key, ip, sizeof(b->key));
                b->send = 0;
                b->recv = 0;
                b->completed = 0;
                dict_set(&bytes_map, b->key, (void*)b);
            }
            b->send += ATOMIC_IGET(server->info->send_bytes, 0);
            b->recv += ATOMIC_IGET(server->info->recv_bytes, 0);
            b->completed += ATOMIC_IGET(server->info->completed_commands, 0);
        }
    }
}

void stats_send_simple()
{
    struct stats stats;
    memset(&stats, 0, sizeof(stats));
    stats_get_simple(&stats, true);
    stats_send("connected_clients", stats.basic.connected_clients);
    stats_send("completed_commands", stats.basic.completed_commands);
    stats_send("used_cpu_sys", stats.used_cpu_sys);
    stats_send("used_cpu_user", stats.used_cpu_user);
    stats_send("latency", stats.basic.total_latency / 1000000.0);
}

void stats_send_node_info()
{
    struct bytes *value;

    /* redis-node.127-0-0-1-8000.bytes.{send,recv} */
    int len = HOST_LEN + 64;
    char name[len];

    struct bytes bytes[REDIS_CLUSTER_SLOTS];
    stats_node_info_agg(bytes);

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&bytes_map, &iter) {
        value = (struct bytes*)iter.value;
        snprintf(name, len, "redis-node.%s.bytes.send", iter.key);
        stats_send(name, value->send);
        snprintf(name, len, "redis-node.%s.bytes.recv", iter.key);
        stats_send(name, value->recv);
        snprintf(name, len, "redis-node.%s.commands.completed", iter.key);
        stats_send(name, value->completed);
        value->send = 0;
        value->recv = 0;
        value->completed = 0;
    }
    dict_clear(&bytes_map);
}

void stats_get(struct stats *stats)
{
    stats_get_simple(stats, false);

    memset(stats->remote_nodes, 0, sizeof(stats->remote_nodes));
    node_list_get(stats->remote_nodes);

    struct context *contexts = get_contexts();

    memset(stats->last_command_latency, 0, sizeof(stats->last_command_latency));
    for (int i = 0; i < config.thread; i++) {
        if (i >= MAX_NODE_LIST) break;
        stats->last_command_latency[i] = ATOMIC_GET(contexts[i].last_command_latency);
    }
}

static void stats_send_slow_log()
{
    if (config.slow_threshold < 0)
        return;

    const char *fmt = "redis-node.%s.slow_query.%s";
    const char *sum_fmt = "slow_query.%s";
    extern struct cmd_item cmds[];
    extern const size_t CMD_NUM;

    struct connection *server;
    struct context *contexts = get_contexts();

    {
        struct dict_iter iter = DICT_ITER_INITIALIZER;
        DICT_FOREACH(&slow_counts, &iter) {
            memset(iter.value, 0, CMD_NUM * sizeof(uint32_t));
        }
    }

    uint32_t counts_sum[CMD_NUM];
    memset(counts_sum, 0, sizeof(counts_sum));

    for (size_t i = 0; i < config.thread; i++) {
        TAILQ_FOREACH(server, &contexts[i].servers, next) {
            const char *dsn = server->info->dsn;
            uint32_t *node_counts = NULL;
            for (size_t j = 0; j < CMD_NUM; j++) {
                uint32_t count = ATOMIC_IGET(server->info->slow_cmd_counts[j], 0);
                if (count == 0) continue;

                if (!node_counts) {
                    node_counts = (uint32_t*)dict_get(&slow_counts, dsn);
                    if (!node_counts) {
                        node_counts = cv_calloc(CMD_NUM, sizeof(uint32_t));
                        dict_set(&slow_counts, dsn, node_counts);
                    }
                }
                node_counts[j] += count;
                counts_sum[j] += count;
            }
        }
    }

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slow_counts, &iter) {
        const char *dsn = iter.key;
        uint32_t *counts = (uint32_t*)iter.value;

        char addr[ADDRESS_LEN] = {0};
        strncpy(addr, dsn, ADDRESS_LEN);
        for (size_t i = 0; i < ADDRESS_LEN; i++) {
            if (addr[i] == '.' || addr[i] == ':')
                addr[i] = '-';
        }

        for (size_t i = 0; i < CMD_NUM; i++) {
            if(counts[i] == 0) continue;

            const char *cmd = cmds[i].cmd;
            int n = snprintf(NULL, 0, fmt, addr, cmd);
            char buf[n + 1];
            snprintf(buf, sizeof(buf), fmt, addr, cmd);
            stats_send(buf, counts[i]);
        }
    }

    for (size_t i = 0; i < CMD_NUM; i++) {
        uint32_t sum = counts_sum[i];
        if (sum) {
            const char *cmd = cmds[i].cmd;
            int n = snprintf(NULL, 0, sum_fmt, cmd);
            char buf[n + 1];
            snprintf(buf, sizeof(buf), sum_fmt, cmd);
            stats_send(buf, sum);
        }
    }
}

void *stats_daemon(void *data)
{
    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    while (1) {
        sleep(config.metric_interval);
        stats_send_simple();
        stats_send_node_info();
        stats_send_slow_log();
        LOG(DEBUG, "sending metrics");
    }
    return NULL;
}

int stats_init()
{
    int len;
    dict_init(&bytes_map);

    memset(&cumulation, 0, sizeof(cumulation));
    memset(&used_cpu, 0, sizeof(used_cpu));

    gethostname(hostname, HOST_LEN + 1);
    len = strlen(hostname);
    if (len > HOST_LEN) {
        hostname[HOST_LEN] = '\0';
        len--;
    }

    for (int i = 0; i < len; i++) {
        if (hostname[i] == '.') hostname[i] = '-';
    }

    dict_init(&slow_counts);

    LOG(INFO, "starting stats thread");
    return thread_spawn(&stats_ctx, stats_daemon);
}

void stats_kill()
{
    int err;

    dict_free(&bytes_map);
    stats_free_slow_counts();

    if (pthread_cancel(stats_ctx.thread) == 0) {
        if ((err = pthread_join(stats_ctx.thread, NULL)) != 0) {
            LOG(WARN, "fail to kill stats thread: %s", strerror(err));
        }
    }
    LOG(DEBUG, "killing stats thread");
}

int stats_resolve_addr(char *addr)
{
    struct address a;

    memset(&dest, 0, sizeof(struct sockaddr_in));
    if (socket_parse_ip(addr, &a) == CORVUS_ERR) {
        LOG(ERROR, "stats_resolve_addr: fail to parse addr %s", addr);
        return CORVUS_ERR;
    }
    return socket_get_sockaddr(a.ip, a.port, &dest, SOCK_DGRAM);
}

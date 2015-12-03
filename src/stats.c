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
#include "dict.h"

struct bytes {
    char key[HOST_NAME_MAX + 8];
    long long recv;
    long long send;
    long long completed;
};

static int statsd_fd = -1;
static struct sockaddr_in dest;
static pthread_t stats_thread;
static int metric_interval = 10;
static struct dict *bytes_map;
static char hostname[HOST_NAME_MAX + 1];
static uint16_t port;


static void stats_send(char *metric, double value)
{
    if (statsd_fd == -1) {
        statsd_fd = socket_create_udp_client();
    }

    int n;
    const char *fmt = "corvus.%s-%d.%s:%f|g";
    n = snprintf(NULL, 0, fmt, hostname, port, metric, value);
    char buf[n + 1];
    snprintf(buf, sizeof(buf), fmt, hostname, port, metric, value);
    if (sendto(statsd_fd, buf, n, 0, (struct sockaddr*)&dest, sizeof(dest)) == -1) {
        LOG(WARN, "fail to send metrics data: %s", strerror(errno));
    }
}

void stats_get_simple(struct stats *stats)
{
    struct rusage ru;
    memset(&ru, 0, sizeof(ru));
    getrusage(RUSAGE_SELF, &ru);

    int threads = get_thread_num();
    struct context *contexts = get_contexts();

    stats->pid = getpid();
    stats->threads = threads;

    stats->used_cpu_sys = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1000000.0;
    stats->used_cpu_user = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1000000.0;

    int i;
    for (i = 0; i < threads; i++) {
        stats->basic.connected_clients += contexts[i].stats.connected_clients;
        stats->basic.completed_commands += contexts[i].stats.completed_commands;
        stats->basic.remote_latency += contexts[i].stats.remote_latency;
        stats->basic.total_latency += contexts[i].stats.total_latency;
        stats->basic.recv_bytes += contexts[i].stats.recv_bytes;
        stats->basic.send_bytes += contexts[i].stats.send_bytes;
        stats->basic.buffers += contexts[i].stats.buffers;
        stats->free_buffers += contexts[i].nfree_mbufq;
    }
}

void stats_node_info_agg(struct bytes *bytes)
{
    struct bytes *b;
    struct connection *server;
    struct context *contexts = get_contexts();
    int j, n, m = 0, threads = get_thread_num();

    for (int i = 0; i < threads; i++) {
        STAILQ_FOREACH(server, &contexts[i].servers, next) {
            n = strlen(server->addr.host);
            if (n <= 0) continue;

            char host[n + 8];
            for (j = 0; j < n; j++) {
                host[j] = server->addr.host[j];
                if (host[j] == '.') host[j] = '-';
            }
            sprintf(host + j, "-%d", server->addr.port);

            b = dict_get(bytes_map, host);
            if (b == NULL) {
                b = &bytes[m++];
                strncpy(b->key, host, sizeof(b->key));
                b->send = 0;
                b->recv = 0;
                b->completed = 0;
                dict_set(bytes_map, b->key, (void*)b);
            }
            b->send += server->send_bytes;
            b->recv += server->recv_bytes;
            b->completed += server->completed_commands;
        }
    }
}

void stats_send_simple()
{
    struct stats stats;
    memset(&stats, 0, sizeof(stats));
    stats_get_simple(&stats);
    stats_send("connected_clients", stats.basic.connected_clients);
    stats_send("completed_commands", stats.basic.completed_commands);
    stats_send("used_cpu_sys", stats.used_cpu_sys);
    stats_send("used_cpu_user", stats.used_cpu_user);
    stats_send("latency", stats.basic.total_latency);
}

void stats_send_node_info()
{
    struct bytes *value;

    /* redis-node.127-0-0-1:8000.bytes.{send,recv} */
    int len = HOST_NAME_MAX + 64;
    char name[len];

    struct bytes bytes[REDIS_CLUSTER_SLOTS];
    stats_node_info_agg(bytes);

    struct dict_iter iter = DICT_ITER_INITIALIZER(bytes_map);
    dict_each(&iter) {
        value = (struct bytes*)iter.val;
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
    dict_clear(bytes_map);
}

void stats_get(struct stats *stats)
{
    stats_get_simple(stats);

    stats->last_command_latency = calloc(stats->threads, sizeof(double));
    slot_get_addr_list(&stats->remote_nodes);

    struct context *contexts = get_contexts();

    for (int i = 0; i < stats->threads; i++) {
        stats->last_command_latency[i] = contexts[i].last_command_latency;
    }
}

void *stats_daemon(void *data)
{
    while (1) {
        sleep(metric_interval);
        stats_send_simple();
        stats_send_node_info();
        LOG(DEBUG, "sending metrics");
    }
    return NULL;
}

int stats_init(int interval)
{
    size_t stacksize;
    pthread_attr_t attr;
    int len;
    bytes_map = dict();

    gethostname(hostname, HOST_NAME_MAX + 1);
    len = strlen(hostname);
    if (len > HOST_NAME_MAX) {
        hostname[HOST_NAME_MAX] = '\0';
        len--;
    }

    for (int i = 0; i < len; i++) {
        if (hostname[i] == '.') hostname[i] = '-';
    }

    port = get_bind();

    metric_interval = interval;

    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    /* Set the stack size as by default it may be small in some system */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    if (pthread_create(&stats_thread, &attr, stats_daemon, NULL) != 0) {
        LOG(ERROR, "can't initialize stats thread");
        return -1;
    }
    LOG(INFO, "starting stats thread");

    return 0;
}

void stats_kill()
{
    int err;

    dict_free(bytes_map);

    if (pthread_cancel(stats_thread) == 0) {
        if ((err = pthread_join(stats_thread, NULL)) != 0) {
            LOG(WARN, "fail to kill stats thread: %s", strerror(err));
        }
    }
}

int stats_resolve_addr(char *addr)
{
    struct address a;

    memset(&dest, 0, sizeof(struct sockaddr_in));
    if (socket_parse_addr(addr, &a) == -1) return -1;
    if (socket_get_sockaddr(a.host, a.port, &dest, SOCK_DGRAM) == -1) return -1;
    return 0;
}

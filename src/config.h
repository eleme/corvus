#ifndef CONFIG_H
#define CONFIG_H

#include "socket.h"

#define CLUSTER_NAME_SIZE 127

struct node_conf {
    struct address *addr;
    int len;
    int refcount;
};

struct {
    char cluster[CLUSTER_NAME_SIZE + 1];
    uint16_t bind;
    struct node_conf *node;
    int thread;
    int loglevel;
    bool syslog;
    char statsd_addr[ADDRESS_LEN + 1];
    int metric_interval;
    bool stats;
    bool readslave;
    bool readmasterslave;
    char *requirepass;
    int64_t client_timeout;
    int64_t server_timeout;
    int bufsize;
    int slowlog_log_slower_than;
    int slowlog_max_len;
    int slowlog_statsd_enabled;
} config;

void config_init();
void config_free();
int read_conf(const char *filename);

struct node_conf *conf_get_node();
void conf_set_node(struct node_conf *node);
void conf_node_dec_ref(struct node_conf *node);
int config_add(char *name, char *value);

#endif /* end of include guard: CONFIG_H */
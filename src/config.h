#ifndef CONFIG_H
#define CONFIG_H

#include "socket.h"

#define CLUSTER_NAME_SIZE 127
#define CONFIG_FILE_PATH_SIZE 256
#define CONFIG_BINDADDR_MAX 16

struct node_conf {
    struct address *addr;
    int len;
    int refcount;
};

struct corvus_config {
    char config_file_path[CONFIG_FILE_PATH_SIZE + 1];
    char cluster[CLUSTER_NAME_SIZE + 1];
    char bind[CONFIG_BINDADDR_MAX];
    uint16_t port;
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
    bool slowlog_statsd_enabled;
} config;

void config_init();
void config_free();
int config_read(const char *filename);
int config_add(char *name, char *value);
int config_get(const char *name, char *value, size_t max_len);
int config_rewrite();

struct node_conf *config_get_node();
void config_set_node(struct node_conf *node);
void config_node_dec_ref(struct node_conf *node);
int config_add(char *name, char *value);
bool config_option_changable(const char *option);

#endif /* end of include guard: CONFIG_H */

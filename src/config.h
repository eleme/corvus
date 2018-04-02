#ifndef CONFIG_H
#define CONFIG_H

#include "socket.h"

#define CLUSTER_NAME_SIZE 127
#define CONFIG_FILE_PATH_SIZE 256

struct node_conf {
    struct address *addr;
    int len;
    int refcount;
};

struct corvus_config {
    char config_file_path[CONFIG_FILE_PATH_SIZE + 1];
    char cluster[CLUSTER_NAME_SIZE + 1];
    uint16_t bind;
    struct node_conf *node;
	struct node_conf *preferred_node; /* List of nodes that should be set a higher priority */
    int thread;
    int loglevel;
    bool syslog;
    char statsd_addr[ADDRESS_LEN + 1];
    int metric_interval;
    bool stats;
    bool readslave;
    bool readmasterslave;
    bool readpreferred;
    uint16_t polling_interval; /* Intervall to use when polling for cluster configuration */
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
struct node_conf *config_get_preferred_node();
bool config_is_preferred_node(struct address *node);

#endif /* end of include guard: CONFIG_H */
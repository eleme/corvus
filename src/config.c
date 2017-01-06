#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include "corvus.h"
#include "alloc.h"
#include "logging.h"

#define DEFAULT_BUFSIZE 16384
#define MIN_BUFSIZE 64

static pthread_mutex_t lock_conf_node = PTHREAD_MUTEX_INITIALIZER;

void config_init()
{
    memset(config.cluster, 0, CLUSTER_NAME_SIZE + 1);
    strncpy(config.cluster, "default", CLUSTER_NAME_SIZE);

    config.bind = 12345;
    config.node = cv_calloc(1, sizeof(struct node_conf));
    config.node->refcount = 1;
    config.thread = 4;
    config.loglevel = INFO;
    config.syslog = 0;
    config.stats = false;
    config.client_timeout = 0;
    config.server_timeout = 0;
    config.bufsize = DEFAULT_BUFSIZE;
    config.requirepass = NULL;
    config.readslave = config.readmasterslave = false;
    config.slowlog_max_len = -1;
    config.slowlog_log_slower_than = -1;
    config.slowlog_statsd_enabled = 0;

    memset(config.statsd_addr, 0, sizeof(config.statsd_addr));
    config.metric_interval = 10;
}

void config_free()
{
    conf_node_dec_ref(config.node);
    pthread_mutex_destroy(&lock_conf_node);
}

void config_boolean(bool *item, char *value)
{
    if (strcasecmp(value, "false") == 0) {
        *item = false;
    } else if (strcasecmp(value, "true") == 0) {
        *item = true;
    } else {
        if (atoi(value) == 0) {
            *item = false;
        } else {
            *item = true;
        }
    }
}

int config_add(char *name, char *value)
{
    int val;
    if (strcmp(name, "cluster") == 0) {
        if (strlen(value) <= 0) return CORVUS_OK;
        strncpy(config.cluster, value, CLUSTER_NAME_SIZE);
    } else if (strcmp(name, "bind") == 0) {
        if (socket_parse_port(value, &config.bind) == CORVUS_ERR) {
            return CORVUS_ERR;
        }
    } else if (strcmp(name, "syslog") == 0) {
        config_boolean(&config.syslog, value);
    } else if (strcmp(name, "read-slave") == 0) {
        LOG(WARN, "Config `read-slave` is obsolete, use `read-strategy` instead");
        config_boolean(&config.readslave, value);
    } else if (strcmp(name, "read-master-slave") == 0) {
        LOG(WARN, "Config `read-master-slave` is obsolete, use `read-strategy` instead");
        config_boolean(&config.readmasterslave, value);
        if (config.readmasterslave) {
            config.readslave = true;
        }
    } else if (strcmp(name, "read-strategy") == 0) {
        if (strcmp(value, "read-slave-only") == 0) {
            config.readmasterslave = false;
            config.readslave = true;
        } else if (strcmp(value, "both") == 0) {
            config.readmasterslave = config.readslave = true;
        } else {
            config.readmasterslave = config.readslave = false;
        }
    } else if (strcmp(name, "thread") == 0) {
        config.thread = atoi(value);
        if (config.thread <= 0) config.thread = 4;
    } else if (strcmp(name, "bufsize") == 0) {
        val = atoi(value);
        if (val <= 0) {
            config.bufsize = DEFAULT_BUFSIZE;
        } else if (val < MIN_BUFSIZE) {
            config.bufsize = MIN_BUFSIZE;
        } else {
            config.bufsize = val;
        }
    } else if (strcmp(name, "client_timeout") == 0) {
        val = atoi(value);
        config.client_timeout = val < 0 ? 0 : val;
    } else if (strcmp(name, "server_timeout") == 0) {
        val = atoi(value);
        config.server_timeout = val < 0 ? 0 : val;
    } else if (strcmp(name, "statsd") == 0) {
        strncpy(config.statsd_addr, value, ADDRESS_LEN);
    } else if (strcmp(name, "metric_interval") == 0) {
        config.metric_interval = atoi(value);
        if (config.metric_interval <= 0) config.metric_interval = 10;
    } else if (strcmp(name, "loglevel") == 0) {
        if (strcmp(value, "debug") == 0) {
            config.loglevel = DEBUG;
        } else if (strcmp(value, "warn") == 0) {
            config.loglevel = WARN;
        } else if (strcmp(value, "error") == 0) {
            config.loglevel = ERROR;
        } else {
            config.loglevel = INFO;
        }
    } else if (strcmp(name, "requirepass") == 0) {
        // Last config overwrites previous ones.
        cv_free(config.requirepass);
        config.requirepass = NULL;

        if (strlen(value) > 0) {
            config.requirepass = cv_calloc(strlen(value) + 1, sizeof(char));
            memcpy(config.requirepass, value, strlen(value));
        }
    } else if (strcmp(name, "node") == 0) {
    	struct address *addr = NULL;
    	int addr_cnt = 0;
        char *p = strtok(value, ",");
        while (p) {
            addr = cv_realloc(addr, sizeof(struct address) * (addr_cnt + 1));
            if (socket_parse_ip(p, &addr[addr_cnt]) == -1) {
                cv_free(addr);
                return CORVUS_ERR;
            }
            addr_cnt++;
            p = strtok(NULL, ",");
        }
        if (addr_cnt == 0) {
            LOG(WARN, "received empty node value in config set");
            return CORVUS_ERR;
        }
        {
            struct node_conf *newnode = cv_calloc(1, sizeof(struct node_conf));
            newnode->addr = addr;
            newnode->len = addr_cnt;
            newnode->refcount = 1;
            conf_set_node(newnode);
        }
    } else if (strcmp(name, "slowlog-log-slower-than") == 0) {
        config.slowlog_log_slower_than = atoi(value);
    } else if (strcmp(name, "slowlog-max-len") == 0) {
        config.slowlog_max_len = atoi(value);
    } else if (strcmp(name, "slowlog-statsd-enabled") == 0) {
        config.slowlog_statsd_enabled = atoi(value);
    }
    return CORVUS_OK;
}

struct node_conf *conf_get_node()
{
    pthread_mutex_lock(&lock_conf_node);
    struct node_conf *node = config.node;
    int refcount = ATOMIC_INC(node->refcount, 1);
    pthread_mutex_unlock(&lock_conf_node);
    assert(refcount >= 1);
    return node;
}

void conf_set_node(struct node_conf *node)
{
    pthread_mutex_lock(&lock_conf_node);
    struct node_conf *oldnode = config.node;
    config.node = node;
    pthread_mutex_unlock(&lock_conf_node);
    conf_node_dec_ref(oldnode);
}

void conf_node_dec_ref(struct node_conf *node)
{
    int refcount = ATOMIC_DEC(node->refcount, 1);
    assert(refcount >= 0);
    if (refcount == 0) {
        cv_free(node->addr);
        cv_free(node);
    }
}

int read_line(char **line, size_t *bytes, FILE *fp)
{
    size_t len, index = 0;
    char buf[1024];
    bool should_realloc = false;

    while (fgets(buf, 1024, fp) != NULL) {
        len = strlen(buf);
        while (*bytes - index <= len) {
            should_realloc = true;
            *bytes = (*bytes == 0) ? 1024 : (*bytes << 1);
        }
        if (should_realloc) {
            *line = cv_realloc(*line, (*bytes) * sizeof(char));
            should_realloc = false;
        }
        memcpy(*line + index, buf, len);
        index += len;
        if ((*line)[index - 1] == '\n') {
            (*line)[index] = '\0';
            return index;
        }
    }
    return -1;
}

int read_conf(const char *filename)
{
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "config file: %s\n", strerror(errno));
        return -1;
    }
    int i, len = 0;
    size_t bytes = 0;
    char *line = NULL;
    while ((len = read_line(&line, &bytes, fp)) != -1) {
        char name[len + 1], value[len + 1];
        memset(name, 0, sizeof(name));
        memset(value, 0, sizeof(value));

        for (i = 0; i < len && (line[i] == ' ' || line[i] == '\r'
                    || line[i] == '\t' || line[i] == '\n'); i++);

        if (i == len || line[i] == '#') {
            continue;
        }

        sscanf(line, "%s%s", name, value);
        if (config_add(name, value) == -1) {
            cv_free(line);
            fclose(fp);
            return CORVUS_ERR;
        }
    }
    cv_free(line);
    fclose(fp);
    return CORVUS_OK;
}
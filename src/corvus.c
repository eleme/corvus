#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <execinfo.h>
#include "corvus.h"
#include "mbuf.h"
#include "slot.h"
#include "logging.h"
#include "event.h"
#include "proxy.h"
#include "stats.h"
#include "dict.h"
#include "timer.h"
#include "alloc.h"

#define DEFAULT_BUFSIZE 16384
#define MIN_BUFSIZE 64

static struct context *contexts;

void config_init()
{
    memset(config.cluster, 0, CLUSTER_NAME_SIZE + 1);
    strncpy(config.cluster, "default", CLUSTER_NAME_SIZE);

    config.bind = 12345;
    memset(&config.node, 0, sizeof(struct node_conf));
    config.thread = 4;
    config.loglevel = INFO;
    config.syslog = 0;
    config.stats = false;
    config.client_timeout = 0;
    config.server_timeout = 0;
    config.bufsize = DEFAULT_BUFSIZE;
    config.requirepass = NULL;
    config.readslave = config.readmasterslave = false;
    config.slow_threshold = -1;

    memset(config.statsd_addr, 0, sizeof(config.statsd_addr));
    config.metric_interval = 10;
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
        if (strlen(value) <= 0) return 0;
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
        cv_free(config.node.addr);
        memset(&config.node, 0, sizeof(config.node));

        char *p = strtok(value, ",");
        while (p) {
            config.node.addr = cv_realloc(config.node.addr,
                    sizeof(struct address) * (config.node.len + 1));

            if (socket_parse_ip(p, &config.node.addr[config.node.len]) == -1) {
                cv_free(config.node.addr);
                return -1;
            }

            config.node.len++;
            p = strtok(NULL, ",");
        }
    } else if (strcmp(name, "slow_threshold") == 0) {
        config.slow_threshold = atoi(value);
    }
    return 0;
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
        fprintf(stderr, "config file: %s", strerror(errno));
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

void sig_handler(int sig)
{
    int i;
    struct sigaction act;
    void *trace[100];

    switch (sig) {
        case SIGINT:
        case SIGTERM:
            for (i = 0; i < config.thread; i++) {
                contexts[i].state = CTX_BEFORE_QUIT;
            }
            break;
        case SIGSEGV:
            i = backtrace(trace, 100);
            backtrace_symbols_fd(trace, i, STDOUT_FILENO);

            // restore default signal handlers
            sigemptyset(&act.sa_mask);
            act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
            act.sa_handler = SIG_DFL;
            sigaction(sig, &act, NULL);
            kill(getpid(), sig);
            break;
    }
}

void setup_signal()
{
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sig_handler;
    sigaction(SIGINT, &act, NULL);
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGSEGV, &act, NULL);
}

int64_t get_time()
{
    int64_t ns;
    struct timespec spec;
    clock_gettime(CLOCK_MONOTONIC, &spec);
    ns = spec.tv_sec * 1000000000;
    ns += spec.tv_nsec;
    return ns;
}

int thread_spawn(struct context *ctx, void *(*start_routine) (void *))
{
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize = 0;
    int err;

    /* Set the stack size as by default it may be small in some system */
    if ((err = pthread_attr_init(&attr)) != 0) {
        LOG(ERROR, "pthread_attr_init: %s", strerror(err));
        return CORVUS_ERR;
    }
    if ((err = pthread_attr_getstacksize(&attr, &stacksize)) != 0) {
        LOG(ERROR, "pthread_attr_getstacksize: %s", strerror(err));
        pthread_attr_destroy(&attr);
        return CORVUS_ERR;
    }
    if (stacksize <= 0) {
        stacksize = 1;
    }
    while (stacksize < THREAD_STACK_SIZE) {
        stacksize <<= 1;
    }
    if ((err = pthread_attr_setstacksize(&attr, stacksize)) != 0) {
        LOG(ERROR, "pthread_attr_setstacksize: %s", strerror(err));
        pthread_attr_destroy(&attr);
        return CORVUS_ERR;
    }

    if ((err = pthread_create(&thread, &attr, start_routine, (void*)ctx)) != 0) {
        LOG(ERROR, "pthread_create: %s", strerror(err));
        pthread_attr_destroy(&attr);
        return CORVUS_ERR;
    }
    ctx->thread = thread;
    pthread_attr_destroy(&attr);
    return CORVUS_OK;
}

struct context *get_contexts()
{
    return contexts;
}

void context_init(struct context *ctx)
{
    memset(ctx, 0, sizeof(struct context));

    dict_init(&ctx->server_table);
    ctx->state = CTX_UNKNOWN;
    mbuf_init(ctx);
    ctx->seed = time(NULL);

    STAILQ_INIT(&ctx->free_cmdq);
    STAILQ_INIT(&ctx->free_conn_infoq);
    TAILQ_INIT(&ctx->servers);
    TAILQ_INIT(&ctx->conns);
}

void build_contexts()
{
    contexts = cv_malloc(sizeof(struct context) * (config.thread + 1));
    for (int i = 0; i <= config.thread; i++) {
        context_init(&contexts[i]);
    }
}

void destroy_contexts()
{
    cv_free(contexts);
}

void context_free(struct context *ctx)
{
    /* server pool */
    struct connection *conn;
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&ctx->server_table, &iter) {
        conn = (struct connection*)(iter.value);
        cmd_iov_free(&conn->info->iov);
        conn_free(conn);
        conn_buf_free(conn);
        cv_free(conn->info->slow_cmd_counts);
        cv_free(conn->info);
        cv_free(conn);
    }
    dict_free(&ctx->server_table);

    /* mbuf queue */
    mbuf_destroy(ctx);

    /* cmd queue */
    struct command *cmd;
    while (!STAILQ_EMPTY(&ctx->free_cmdq)) {
        cmd = STAILQ_FIRST(&ctx->free_cmdq);
        STAILQ_REMOVE_HEAD(&ctx->free_cmdq, cmd_next);
        cv_free(cmd);
        ctx->mstats.free_cmds--;
    }

    /* connection queue */
    while (!TAILQ_EMPTY(&ctx->conns)) {
        conn = TAILQ_FIRST(&ctx->conns);
        TAILQ_REMOVE(&ctx->conns, conn, next);
        if (conn->fd != -1) {
            if (conn->ev != NULL) {
                conn_free(conn->ev);
                cv_free(conn->ev);
                conn->ev = NULL;
            }
            conn_free(conn);
            conn_buf_free(conn);
        }
        cv_free(conn);
    }

    /* connection info queu */
    struct conn_info *info;
    while (!STAILQ_EMPTY(&ctx->free_conn_infoq)) {
        info = STAILQ_FIRST(&ctx->free_conn_infoq);
        STAILQ_REMOVE_HEAD(&ctx->free_conn_infoq, next);
        cmd_iov_free(&info->iov);
        cv_free(info);
        ctx->mstats.free_buffers--;
    }

    /* buf_time queue */
    struct buf_time *t;
    while (!STAILQ_EMPTY(&ctx->free_buf_timeq)) {
        t = STAILQ_FIRST(&ctx->free_buf_timeq);
        STAILQ_REMOVE_HEAD(&ctx->free_buf_timeq, next);
        cv_free(t);
    }

    /* event loop */
    event_free(&ctx->loop);
}

void *main_loop(void *data)
{
    struct context *ctx = data;

    if (event_init(&ctx->loop, 1024) == -1) {
        LOG(ERROR, "Fatal: fail to create event loop.");
        exit(EXIT_FAILURE);
    }

    if (proxy_init(&ctx->proxy, ctx, "0.0.0.0", config.bind) == -1) {
        LOG(ERROR, "Fatal: fail to create proxy.");
        exit(EXIT_FAILURE);
    }
    if (event_register(&ctx->loop, &ctx->proxy, E_READABLE) == -1) {
        LOG(ERROR, "Fatal: fail to register proxy.");
        exit(EXIT_FAILURE);
    }

    if (timer_init(&ctx->timer, ctx) == -1) {
        LOG(ERROR, "Fatal: fail to init timer.");
        exit(EXIT_FAILURE);
    }
    if (event_register(&ctx->loop, &ctx->timer, E_READABLE) == -1) {
        LOG(ERROR, "Fatal: fail to register timer.");
        exit(EXIT_FAILURE);
    }
    if (timer_start(&ctx->timer) == -1) {
        LOG(ERROR, "Fatal: fail to start timer.");
        exit(EXIT_FAILURE);
    }

    while (ctx->state != CTX_QUIT) {
        event_wait(&ctx->loop, -1);
    }
    LOG(DEBUG, "main loop quiting");
    return NULL;
}

#ifndef CORVUS_TEST
int main(int argc, const char *argv[])
{
    int i, err;
    if (argc != 2) {
        fprintf(stderr, "Usage: %s corvus.conf\n", argv[0]);
        return EXIT_FAILURE;
    }

    config_init();
    if (read_conf(argv[1]) == -1) {
        fprintf(stderr, "Error: invalid config.\n");
        return EXIT_FAILURE;
    }
    if (config.node.len <= 0) {
        fprintf(stderr, "Error: invalid config, `node` should be set.\n");
        return EXIT_FAILURE;
    }

    if (config.syslog) {
        openlog(NULL, LOG_NDELAY | LOG_NOWAIT, LOG_USER);
    }

    // allocate memory for `contexts`
    build_contexts();

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setup_signal();

    cmd_map_init();

    // start slot management thread
    if (slot_start_manager(&contexts[config.thread]) == CORVUS_ERR) {
        LOG(ERROR, "fail to start slot manager thread");
        return EXIT_FAILURE;
    }
    // create first slot updating job
    slot_create_job(SLOT_UPDATE);

    // start worker threads
    for (i = 0; i < config.thread; i++) {
        if (thread_spawn(&contexts[i], main_loop) == CORVUS_ERR) {
            LOG(ERROR, "fail to start worker thread: %d", i);
            return EXIT_FAILURE;
        }
    }

    if (strlen(config.statsd_addr) > 0) {
        if (stats_resolve_addr(config.statsd_addr) == CORVUS_ERR) {
            LOG(WARN, "fail to resolve statsd address");
        } else {
            config.stats = true;
        }
    }

    // start stats thread
    if (config.stats) {
        stats_init();
    }

    LOG(INFO, "serve at 0.0.0.0:%d", config.bind);

    for (i = 0; i < config.thread; i++) {
        if ((err = pthread_join(contexts[i].thread, NULL)) != 0) {
            LOG(WARN, "pthread_join: %s", strerror(err));
        }
    }

    // stop stats thread
    if (config.stats) {
        stats_kill();
    }
    // stop slot updater thread
    slot_create_job(SLOT_UPDATER_QUIT);
    if ((err = pthread_join(contexts[config.thread].thread, NULL)) != 0) {
        LOG(WARN, "pthread_join: %s", strerror(err));
    }

    for (i = 0; i <= config.thread; i++) {
        context_free(&contexts[i]);
    }

    // free `contexts`
    destroy_contexts();
    cmd_map_destroy();
    cv_free(config.requirepass);
    cv_free(config.node.addr);
    if (config.syslog) closelog();
    return EXIT_SUCCESS;
}
#endif

#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
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

static struct context *contexts;

static void config_init()
{
    memset(config.cluster, 0, CLUSTER_NAME_SIZE + 1);
    strncpy(config.cluster, "default", CLUSTER_NAME_SIZE);

    config.bind = 12345;
    memset(&config.node, 0, sizeof(struct node_conf));
    config.thread = 4;
    config.loglevel = INFO;
    config.syslog = 0;
    config.stats = 0;
    config.client_timeout = 0;
    config.server_timeout = 0;

    memset(config.statsd_addr, 0, sizeof(config.statsd_addr));
    config.metric_interval = 10;
}

static int config_add(char *name, char *value)
{
    int timeout;
    if (strcmp(name, "cluster") == 0) {
        if (strlen(value) <= 0) return 0;
        strncpy(config.cluster, value, CLUSTER_NAME_SIZE);
    } else if (strcmp(name, "bind") == 0) {
        config.bind = atoi(value);
        if (config.bind > 0xFFFF) return -1;
    } else if (strcmp(name, "syslog") == 0) {
        config.syslog = atoi(value);
    } else if (strcmp(name, "thread") == 0) {
        config.thread = atoi(value);
        if (config.thread <= 0) config.thread = 4;
    } else if (strcmp(name, "client_timeout") == 0) {
        timeout = atoi(value);
        config.client_timeout = timeout < 0 ? 0 : timeout;
    } else if (strcmp(name, "server_timeout") == 0) {
        timeout = atoi(value);
        config.server_timeout = timeout < 0 ? 0 : timeout;
    } else if (strcmp(name, "statsd") == 0) {
        strncpy(config.statsd_addr, value, DSN_MAX);
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
    } else if (strcmp(name, "node") == 0) {
        int i;
        if (config.node.nodes != NULL) {
            for (i = 0; i < config.node.len; i++) {
                free(config.node.nodes[i]);
            }
            free(config.node.nodes);
            config.node.nodes = NULL;
            config.node.len = 0;
        }
        int len = 0, size;
        char *p = strtok(value, ",");
        while (p) {
            size = strlen(p);
            config.node.nodes = realloc(config.node.nodes, sizeof(char*) * (++len));
            config.node.nodes[len - 1] = malloc(sizeof(char) * (size + 1));
            memcpy(config.node.nodes[len - 1], p, size);
            config.node.nodes[len - 1][size] = '\0';
            config.node.len++;
            p = strtok(NULL, ",");
        }
    }
    return 0;
}

static int read_conf(const char *filename)
{
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "config file: %s", strerror(errno));
        return -1;
    }
    size_t len = 0;
    int r, i;
    char *line = NULL;
    while ((r = getline(&line, &len, fp)) != -1) {
        char name[r], value[r];
        memset(name, 0, sizeof(name));
        memset(value, 0, sizeof(value));

        for (i = 0; i < r && (line[i] == ' ' || line[i] == '\r'
                    || line[i] == '\t' || line[i] == '\n'); i++);
        if (i == r || line[i] == '#') continue;

        sscanf(line, "%s%s", name, value);
        if (config_add(name, value) == -1) {
            free(line);
            fclose(fp);
            return -1;
        }
    }
    free(line);
    fclose(fp);
    return 0;
}

static void quit()
{
    int status, i;

    if (config.stats) stats_kill();

    uint64_t data = 1;
    for (i = 0; i <= config.thread; i++) {
        if (!contexts[i].started) continue;
        switch (contexts[i].role) {
            case THREAD_SLOT_UPDATER:
                slot_create_job(SLOT_UPDATER_QUIT);
                break;
            case THREAD_MAIN_WORKER:
                status = write(contexts[i].notifier.fd, &data, sizeof(data));
                if (status == -1) {
                    LOG(ERROR, "quitting signal fail to send, force quit...");
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                break;
        }
    }
}

static void log_traceback()
{
    void *stack[64];
    char **symbols;
    int size, i, j;

    size = backtrace(stack, 64);
    symbols = backtrace_symbols(stack, size);
    if (symbols == NULL || size <= 5) {
        LOG(ERROR, "segmentation fault");
        exit(EXIT_FAILURE);
    }

    const int msg_len = 2048;
    char msg[(size - 5) * msg_len];
    int n, len = 0;

    for (i = 3, j = 0; i < size - 2; i++, j++) {
        n = snprintf(msg + len, msg_len, "  [%d] %s", j, symbols[i]);
        len += n;
        if (i < size - 3) {
            msg[len++] = '\n';
        }
    }
    free(symbols);

    LOG(ERROR, "segmentation fault: \n%s", msg);
    exit(EXIT_FAILURE);
}

static void sig_handler(int sig)
{
    switch (sig) {
        case SIGINT:
        case SIGTERM:
            quit();
            break;
        case SIGSEGV:
            log_traceback();
            break;
    }
}

static void setup_signal()
{
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sig_handler;
    sigaction(SIGINT, &act, NULL);
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGSEGV, &act, NULL);
}

static void notify_ready(struct connection *conn, uint32_t mask)
{
    if (mask & E_READABLE) {
        conn->ctx->quit = 1;
        LOG(DEBUG, "existing signal received");
    }
}

static int setup_notifier(struct context *ctx)
{
    conn_init(&ctx->notifier, ctx);
    ctx->notifier.fd = eventfd(0, 0);
    if (ctx->notifier.fd == -1) return CORVUS_ERR;
    ctx->notifier.ready = notify_ready;

    if (event_register(&ctx->loop, &ctx->notifier) == -1) {
        return CORVUS_ERR;
    }
    return CORVUS_OK;
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

struct context *get_contexts()
{
    return contexts;
}

void context_init(struct context *ctx, bool syslog, int log_level)
{
    memset(ctx, 0, sizeof(struct context));

    ctx->syslog = syslog;
    ctx->log_level = log_level;
    dict_init(&ctx->server_table);
    ctx->started = false;
    ctx->role = THREAD_UNKNOWN;
    mbuf_init(ctx);
    log_init(ctx);

    STAILQ_INIT(&ctx->free_cmdq);
    TAILQ_INIT(&ctx->servers);
    TAILQ_INIT(&ctx->conns);
}

void context_free(struct context *ctx)
{
    /* server pool */
    struct connection *conn;
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&ctx->server_table, &iter) {
        conn = (struct connection*)(iter.value);
        conn_free(conn);
        conn_buf_free(conn);
        free(conn);
    }
    dict_free(&ctx->server_table);

    /* mbuf queue */
    mbuf_destroy(ctx);

    /* cmd queue */
    struct command *cmd;
    while (!STAILQ_EMPTY(&ctx->free_cmdq)) {
        cmd = STAILQ_FIRST(&ctx->free_cmdq);
        STAILQ_REMOVE_HEAD(&ctx->free_cmdq, cmd_next);
        free(cmd);
        ctx->nfree_cmdq--;
    }

    /* connection queue */
    while (!TAILQ_EMPTY(&ctx->conns)) {
        conn = TAILQ_FIRST(&ctx->conns);
        if (conn->fd != -1) {
            conn_free(conn);
            conn_buf_free(conn);
        }
        TAILQ_REMOVE(&ctx->conns, conn, next);
        free(conn);
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
    if (event_register(&ctx->loop, &ctx->proxy) == -1) {
        LOG(ERROR, "Fatal: fail to register proxy.");
        exit(EXIT_FAILURE);
    }

    if (setup_notifier(ctx) == CORVUS_ERR) {
        LOG(ERROR, "Fatal: fail to setup notifier.");
        exit(EXIT_FAILURE);
    }

    if (config.client_timeout > 0 || config.server_timeout > 0) {
        if (timer_init(&ctx->timer, ctx) == -1) {
            LOG(ERROR, "Fatal: fail to init timer.");
            exit(EXIT_FAILURE);
        }
        if (event_register(&ctx->loop, &ctx->timer) == -1) {
            LOG(ERROR, "Fatal: fail to register timer.");
            exit(EXIT_FAILURE);
        }
        if (timer_start(&ctx->timer) == -1) {
            LOG(ERROR, "Fatal: fail to start timer.");
            exit(EXIT_FAILURE);
        }
    }

    while (!ctx->quit) {
        event_wait(&ctx->loop, -1);
    }
    context_free(ctx);

    LOG(DEBUG, "main loop quiting");
    return NULL;
}

void start_worker(int i)
{
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;

    struct context *ctx = &contexts[i];

    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    /* Set the stack size as by default it may be small in some system */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    if (pthread_create(&thread, &attr, main_loop, (void*)ctx) != 0) {
        LOG(ERROR, "can't initialize slot updating thread");
        exit(EXIT_FAILURE);
    }
    ctx->thread = thread;
    ctx->started = true;
    ctx->role = THREAD_MAIN_WORKER;
}

#ifndef CORVUS_TEST
int main(int argc, const char *argv[])
{
    int i;
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

    contexts = malloc(sizeof(struct context) * (config.thread + 1));
    for (i = 0; i <= config.thread; i++) {
        context_init(&contexts[i], config.syslog, config.loglevel);
        contexts[i].node_conf = &config.node;
        contexts[i].role = THREAD_UNKNOWN;
    }

    cmd_map_init();
    slot_init_updater(&contexts[config.thread]);
    slot_create_job(SLOT_UPDATE);

    for (i = 0; i < config.thread; i++) {
        start_worker(i);
    }

    setup_signal();

    LOG(INFO, "serve at 0.0.0.0:%d", config.bind);

    if (strlen(config.statsd_addr) > 0) {
        if (stats_resolve_addr(config.statsd_addr) == -1) {
            LOG(WARN, "fail to resolve statsd address");
        } else {
            config.stats = 1;
        }
    }

    if (config.stats) stats_init(config.metric_interval);

    for (i = 0; i <= config.thread; i++) {
        if (!contexts[i].started) continue;
        pthread_join(contexts[i].thread, NULL);
    }

    free(contexts);
    cmd_map_destroy();

    for (i = 0; i < config.node.len; i++) {
        free(config.node.nodes[i]);
    }
    free(config.node.nodes);
    if (config.syslog) closelog();
    return EXIT_SUCCESS;
}
#endif

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

#define DEFAULT_BUFSIZE 16384
#define MIN_BUFSIZE 64

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
    config.bufsize = DEFAULT_BUFSIZE;

    memset(config.statsd_addr, 0, sizeof(config.statsd_addr));
    config.metric_interval = 10;
}

static int config_add(char *name, char *value)
{
    int val;
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
        if (config.node.addr != NULL) {
            free(config.node.addr);
            memset(&config.node, 0, sizeof(config.node));
        }
        char *p = strtok(value, ",");
        while (p) {
            config.node.addr = realloc(config.node.addr,
                    sizeof(struct address) * (config.node.len + 1));

            if (socket_parse_addr(p, &config.node.addr[config.node.len]) == -1) {
                free(config.node.addr);
                return -1;
            }

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
    if (config.stats) stats_kill();

    for (int i = 0; i <= config.thread; i++) {
        if (!contexts[i].started) continue;
        switch (contexts[i].role) {
            case THREAD_SLOT_UPDATER:
                slot_create_job(SLOT_UPDATER_QUIT);
                break;
            case THREAD_MAIN_WORKER:
                contexts[i].state = CTX_BEFORE_QUIT;
                break;
            case THREAD_UNKNOWN: break;
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
    ctx->state = CTX_UNKNOWN;
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

    while (ctx->state != CTX_QUIT) {
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
    free(config.node.addr);
    if (config.syslog) closelog();
    return EXIT_SUCCESS;
}
#endif

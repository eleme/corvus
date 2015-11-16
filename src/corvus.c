#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include "corvus.h"
#include "mbuf.h"
#include "slot.h"
#include "logging.h"
#include "event.h"
#include "proxy.h"

static struct {
    uint16_t bind;
    struct node_conf node;
    int thread;
    int loglevel;
    int syslog;
} config;

static struct context *contexts;

static void config_init()
{
    config.bind = 12345;
    memset(&config.node, 0, sizeof(struct node_conf));
    config.thread = 4;
    config.loglevel = INFO;
    config.syslog = 0;
}

static int config_add(char *name, char *value)
{
    int name_len = strlen(name),
        value_len = strlen(value);
    if (name_len <= 0 || value_len <= 0) return -1;

    char *end;
    if (strncmp(name, "bind", MIN(4, name_len)) == 0) {
        config.bind = strtoul(value, &end, 0);
        if (config.bind > 0xFFFF) return -1;
    } else if (strncmp(name, "syslog", MIN(6, name_len)) == 0) {
        config.syslog = strtoul(value, &end, 0);
    } else if (strncmp(name, "thread", MIN(6, name_len)) == 0) {
        config.thread = strtoul(value, &end, 0);
        if (config.thread < 0) config.thread = 4;
    } else if (strncmp(name, "loglevel", MIN(8, name_len)) == 0) {
        if (strncmp(value, "debug", MIN(5, value_len)) == 0) {
            config.loglevel = DEBUG;
        } else if (strncmp(value, "warn", MIN(4, value_len)) == 0) {
            config.loglevel = WARN;
        } else if (strncmp(value, "error", MIN(4, value_len)) == 0) {
            config.loglevel = ERROR;
        } else {
            config.loglevel = INFO;
        }
    } else if (strncmp(name, "node", MIN(4, name_len)) == 0) {
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

static void do_quit()
{
    int status, i;
    uint64_t data = 1;
    for (i = 0; i <= config.thread; i++) {
        if (!contexts[i].started) continue;
        switch (contexts[i].role) {
            case THREAD_SLOT_UPDATER:
                slot_create_job(SLOT_UPDATER_QUIT, NULL);
                break;
            case THREAD_MAIN_WORKER:
                status = write(contexts[i].notifier->fd, &data, sizeof(data));
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

static void sig_handler(int sig)
{
    if (sig != SIGINT && sig != SIGTERM) return;
    do_quit();
}

static void setup_signal()
{
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sig_handler;
    sigaction(SIGINT, &act, NULL);
    sigaction(SIGTERM, &act, NULL);
}

static void notify_ready(struct connection *conn, struct event_loop *loop, uint32_t mask)
{
    if (mask & E_READABLE) {
        conn->ctx->quit = 1;
        LOG(DEBUG, "existing signal received");
    }
}

static int setup_notifier(struct context *ctx)
{
    struct connection *notifier = conn_create(ctx);
    notifier->fd = eventfd(0, 0);
    notifier->ready = notify_ready;
    if (notifier->fd == -1) {
        LOG(ERROR, "thread quit, can not create event fd");
        return CORVUS_ERR;
    }
    ctx->notifier = notifier;
    event_register(ctx->loop, notifier);
    return CORVUS_OK;
}

void context_init(struct context *ctx, bool syslog, int log_level)
{
    ctx->syslog = syslog;
    ctx->log_level = log_level;
    ctx->server_table = hash_new();
    ctx->started = false;
    ctx->quit = 0;
    ctx->notifier = NULL;
    ctx->proxy = NULL;
    ctx->loop = NULL;
    ctx->node_conf = NULL;
    ctx->role = THREAD_UNKNOWN;
    mbuf_init(ctx);
    log_init(ctx);

    STAILQ_INIT(&ctx->free_cmdq);
    ctx->nfree_cmdq = 0;

    STAILQ_INIT(&ctx->free_connq);
    ctx->nfree_connq = 0;
}

void context_free(struct context *ctx)
{
    /* server pool */
    struct connection *conn;
    hash_each(ctx->server_table, {
        free((void*)key);
        conn = (struct connection*)val;
        conn_free(conn);
        free(conn);
    })
    hash_clear(ctx->server_table);
    hash_free(ctx->server_table);

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
    while (!STAILQ_EMPTY(&ctx->free_connq)) {
        conn = STAILQ_FIRST(&ctx->free_connq);
        STAILQ_REMOVE_HEAD(&ctx->free_connq, next);
        free(conn);
        ctx->nfree_connq--;
    }

    /* event loop */
    event_destory(ctx->loop);

    /* notifier */
    conn_free(ctx->notifier);
    if (ctx->notifier != NULL) free(ctx->notifier);

    /* proxy */
    conn_free(ctx->proxy);
    if (ctx->proxy != NULL) free(ctx->proxy);
}

void *main_loop(void *data)
{
    struct context *ctx = data;
    struct event_loop *loop = event_create(1024);
    ctx->loop = loop;

    struct connection *proxy = proxy_create(ctx, "0.0.0.0", config.bind);
    if (proxy == NULL) {
        LOG(ERROR, "Fatal: fail to create proxy.");
        exit(EXIT_FAILURE);
    }
    event_register(loop, proxy);

    if (setup_notifier(ctx) == CORVUS_ERR) {
        LOG(ERROR, "Fatal: fail to setup notifier.");
        exit(EXIT_FAILURE);
    }

    while (!ctx->quit) {
        event_wait(loop, -1);
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

int main(int argc, const char *argv[])
{
    int i;
    if (argc != 2) {
        fprintf(stderr, "Usage: corvus corvus.conf\n");
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
        openlog(NULL, LOG_PID | LOG_NDELAY | LOG_NOWAIT, LOG_USER);
    }

    contexts = malloc(sizeof(struct context) * (config.thread + 1));
    for (i = 0; i <= config.thread; i++) {
        context_init(&contexts[i], config.syslog, config.loglevel);
        contexts[i].node_conf = &config.node;
        contexts[i].role = THREAD_UNKNOWN;
    }

    cmd_map_init();
    slot_init_updater(&contexts[config.thread]);
    slot_create_job(SLOT_UPDATE_INIT, NULL);

    for (i = 0; i < config.thread; i++) {
        start_worker(i);
    }

    setup_signal();

    LOG(INFO, "serve at 0.0.0.0:%d", config.bind);

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

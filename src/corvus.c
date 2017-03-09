#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <execinfo.h>
#include <getopt.h>
#include <errno.h>
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

static struct context *contexts;

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

int ignore_signal(int sig)
{
    if (signal(sig, SIG_IGN) == SIG_ERR) {
        LOG(CRIT, "Failed to ignore signal[%s]: %s",
            strsignal(sig), strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int register_signal(int sig, struct sigaction *act)
{
    LOG(DEBUG, "Registering signal[%s]", strsignal(sig));
    if (sigaction(sig, act, NULL) != 0) {
        LOG(CRIT, "Failed to register signal[%s]: %s",
            strsignal(sig), strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int create_signal_action(struct sigaction *act)
{
    if (sigemptyset(&(act->sa_mask)) != 0) {
        LOG(CRIT, "Failed to initialize signal set: %s", strerror(errno));
        return CORVUS_ERR;
    }
    act->sa_flags = 0;
    act->sa_handler = sig_handler;
    return CORVUS_OK;
}

int setup_signals()
{
    struct sigaction act;
    RET_NOT_OK(create_signal_action(&act));
    RET_NOT_OK(register_signal(SIGINT, &act));
    RET_NOT_OK(register_signal(SIGTERM, &act));
    RET_NOT_OK(register_signal(SIGSEGV, &act));
    RET_NOT_OK(ignore_signal(SIGHUP));
    RET_NOT_OK(ignore_signal(SIGPIPE));
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

    ctx->slowlog.capacity = 0;  // for non worker threads
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

    /* slowlog */
    if (ctx->slowlog.capacity > 0)
        slowlog_free(&ctx->slowlog);

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

    // Still initialize it no matter whether slowlog_log_slower_than
    // is not negative so that we can turn on slowlog dynamically by
    // changing slowlog_log_slower_than at runtime.
    if (config.slowlog_max_len > 0) {
        LOG(DEBUG, "slowlog enabled");
        if (slowlog_init(&ctx->slowlog) == CORVUS_ERR) {
            LOG(ERROR, "Fatal: fail to init slowlog.");
            exit(EXIT_FAILURE);
        }
    }

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

static const struct option opts[] = {
    {"nodes", required_argument, NULL, 'n' },
    {"threads", required_argument, NULL, 't'},
    {"cluster", required_argument, NULL, 'c'},
    {"bind", required_argument, NULL, 'b'},
    {"syslog", required_argument, NULL, 'l'},
    {"read-strategy", required_argument, NULL, 's'},
    {"bufsize", required_argument, NULL, 'B'},
    {"client-timeout", required_argument, NULL, 'C'},
    {"server-timeout", required_argument, NULL, 'S'},
    {"statsd", required_argument, NULL, 'A'},
    {"metric-interval", required_argument, NULL, 'm'},
    {"loglevel", required_argument, NULL, 'L'},
    {"requirepass", required_argument, NULL, 'P'},
    {"slowlog-time", required_argument, NULL, 'g'},
    {"slowlog-max", required_argument, NULL, 'G'},
    {"slowlog-statsd", required_argument, NULL, 'E'},
    { 0, 0, 0, 0 },
};

static const char *opts_desc[] = {
    "nodes of targeting cluster. eg: 192.168.123.2:6391,192.168.123.2:6392",
    "thread num",
    "cluster name",
    "port to listen",
    "whether writing log to syslog. 0 or 1",
    "whether sending all cmd to both masters and slaves. read-slave-only|both or \"\" by default to masters",
    "buffer size allocated each time avoiding fregments",
    "for clients",
    "for upstreams",
    "statsd upstream, eg 10.10.99.11:8125",
    "interval to capture metrics, in seconds",
    "level including debug, info, warn, error",
    "password needed to auth",
    "slowlog time in microseconds",
    "slowlog max len",
    "slowlog whether writing to statsd",
};

static void usage(const char *cmd)
{
    int i = 0;
    fprintf(stderr, "Usage: %s [options] corvus.conf\n", cmd);
    fprintf(stderr, "Note: Options specified in command line will override ones in config file.\n");
    fprintf(stderr, "Options:\n");
    while(opts[i].name > 0) {
        fprintf(stderr, "    -%c, --%-16s%s\n", opts[i].val, opts[i].name, opts_desc[i]);
        i ++;
    }
    fprintf(stderr, "\n");
}

static int parameter_init(int argc, const char *argv[]) {
    int ch;
    opterr = optind = 0;
    do {
        ch = getopt_long(argc, (char * const *)argv, ":n:t:c:b:l:s:B:C:S:A:m:L:P:g:G:E:", opts, NULL);
        if (ch < 0) {
            break;
        }
        switch (ch) {
            case 't':
                config_add("thread", optarg);
                break;
            case 'n':
                config_add("node", optarg);
                break;
            case 'c':
                config_add("cluster", optarg);
                break;
            case 'b':
                config_add("bind", optarg);
                break;
            case 'l':
                config_add("syslog", optarg);
                break;
            case 's':
                config_add("read-strategy", optarg);
                break;
            case 'B':
                config_add("bufsize", optarg);
                break;
            case 'C':
                config_add("client_timeout", optarg);
                break;
            case 'S':
                config_add("server_timeout", optarg);
                break;
            case 'A':
                config_add("statsd", optarg);
                break;
            case 'm':
                config_add("metric_interval", optarg);
                break;
            case 'L':
                config_add("loglevel", optarg);
                break;
            case 'P':
                config_add("requirepass", optarg);
                break;
            case 'g':
                config_add("slowlog-log-slower-than", optarg);
                break;
            case 'G':
                config_add("slowlog-max-len", optarg);
                break;
            case 'E':
                config_add("slowlog-statsd-enabled", optarg);
                break;
            default:
                fprintf(stderr, "unknow option: %c\n", ch);
                return CORVUS_ERR;
        }
    } while(1);
    return CORVUS_OK;
}

int main(int argc, const char *argv[])
{
    int i, err;
    if (argc < 2) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    config_init();
    if (config_read(argv[argc - 1]) == CORVUS_ERR) {
        fprintf(stderr, "Error: invalid config.\n");
        return EXIT_FAILURE;
    }
    if (parameter_init(argc, argv) != CORVUS_OK) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    if (config.node->len <= 0) {
        fprintf(stderr, "Error: invalid upstream list, `node` should be set to a valid nodes list.\n");
        return EXIT_FAILURE;
    }

    if (config.syslog) {
        openlog(NULL, LOG_NDELAY | LOG_NOWAIT, LOG_USER);
    }

    // allocate memory for `contexts`
    build_contexts();

    if (setup_signals() == CORVUS_ERR) {
        fprintf(stderr, "Error: failed to setup signals.\n");
        return EXIT_FAILURE;
    }

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
    config_free();
    if (config.syslog) closelog();
    return EXIT_SUCCESS;
}
#endif

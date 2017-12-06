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

static pthread_spinlock_t signal_lock;
static struct context *contexts;    // 全局对象, context列表

// 发生段错误时释放的信号执行的函数
void sigsegv_handler(int sig)
{
    if (pthread_spin_trylock(&signal_lock) == EBUSY) {
        sigset_t mask;
        sigemptyset(&mask);
        pselect(0, NULL, NULL, NULL, NULL, &mask);
    }

    struct sigaction act;
    void *trace[100];
    int size = backtrace(trace, 100);
    backtrace_symbols_fd(trace, size, STDOUT_FILENO);

    // restore default signal handlers
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction(sig, &act, NULL);
    kill(getpid(), sig);
}

// 信号处理函数
void sig_handler(int sig)
{
    int i;
    switch (sig) {
        case SIGINT:
        case SIGTERM:
            // 对于上面两种信号, 修改context的state
            for (i = 0; i < config.thread; i++) {
                contexts[i].state = CTX_BEFORE_QUIT;
            }
            break;
        case SIGSEGV:
            sigsegv_handler(sig);
            break;
    }
}

// 忽略部分信号
int ignore_signal(int sig)
{
    if (signal(sig, SIG_IGN) == SIG_ERR) {
        LOG(CRIT, "Failed to ignore signal[%s]: %s",
            strsignal(sig), strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// 对于不同类型的信号, 注册到信号集
int register_signal(int sig, struct sigaction *act)
{
    LOG(DEBUG, "Registering signal[%s]", strsignal(sig));
    // 指定对不同信号进行不同的处理动作
    if (sigaction(sig, act, NULL) != 0) {
        LOG(CRIT, "Failed to register signal[%s]: %s",
            strsignal(sig), strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

/** 初始化信号, 结构体sigaction结构如下
 * struct sigaction
 * {
 *      void (*sa_handler) (int);       表示信号处理函数, 与signal函数中的信号处理函数相似
 *      void (*sa_sigaction)(int, siginfo_t *, void *); 信号处理函数, 可以获取信号的详细信息
 *      sigset_t sa_mask;               用来设置在处理该信号时暂时将sa_mask指定的信号搁置
 *      int sa_flags;                   用来设置信号处理的其他相关操作
 *      void (*sa_restorer) (void);     已废弃
 * }
 */
int create_signal_action(struct sigaction *act)
{
    // 初始化信号集为空, 失败返回-1
    if (sigemptyset(&(act->sa_mask)) != 0) {
        LOG(CRIT, "Failed to initialize signal set: %s", strerror(errno));
        return CORVUS_ERR;
    }
    act->sa_flags = 0;
    act->sa_handler = sig_handler;  // 指定信号处理函数
    return CORVUS_OK;
}

// 初始化信号, 并把部分信号注册上去, 同时忽略部分信号
int setup_signals()
{
    struct sigaction act;
    RET_NOT_OK(create_signal_action(&act));
    RET_NOT_OK(register_signal(SIGINT, &act));      // interrupt的信号
    RET_NOT_OK(register_signal(SIGTERM, &act));     // 通过kill指令来获取终止执行本程序的信号
    RET_NOT_OK(register_signal(SIGSEGV, &act));     // 当一个进程执行了一个无效的内存引用，或发生段错误时发送给它的信号
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
    pthread_attr_t attr;    // 线程属性
    pthread_t thread;
    size_t stacksize = 0;
    int err;

    /* Set the stack size as by default it may be small in some system */
    // 初始化线程属性, 线程属性中有stacksize(线程栈的大小), 下面需要更改stacksize这个attr
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
    // 调整stacksize, 最少需要4M
    while (stacksize < THREAD_STACK_SIZE) {
        stacksize <<= 1;
    }
    // 设置线程属性的stacksize
    if ((err = pthread_attr_setstacksize(&attr, stacksize)) != 0) {
        LOG(ERROR, "pthread_attr_setstacksize: %s", strerror(err));
        pthread_attr_destroy(&attr);
        return CORVUS_ERR;
    }

    // 创建线程, pthread_create函数参数依次为(指向线程的指针, 线程属性, 线程执行的函数的指针, 执行函数需要的参数)
    if ((err = pthread_create(&thread, &attr, start_routine, (void*)ctx)) != 0) {
        LOG(ERROR, "pthread_create: %s", strerror(err));
        pthread_attr_destroy(&attr);
        return CORVUS_ERR;
    }
    ctx->thread = thread;
    pthread_attr_destroy(&attr);    // 去除线程属性初始化
    return CORVUS_OK;
}

struct context *get_contexts()
{
    return contexts;
}

// 初始化context
void context_init(struct context *ctx)
{
    memset(ctx, 0, sizeof(struct context));

    dict_init(&ctx->server_table);
    // 设置context的state状态
    ctx->state = CTX_UNKNOWN;
    // 初始化缓冲区
    mbuf_init(ctx);
    ctx->seed = time(NULL);     // 把seed设置为当前时间戳

    // STAILQ是内核对单向队列的一个抽象
    STAILQ_INIT(&ctx->free_cmdq);
    STAILQ_INIT(&ctx->free_conn_infoq);
    // TAILQ是linux内核对双向队列的一个抽象
    TAILQ_INIT(&ctx->servers);
    TAILQ_INIT(&ctx->conns);

    ctx->slowlog.capacity = 0;  // for non worker threads
}

// 初始化context
void build_contexts()
{
    // 申请(线程数+1)个context, 并初始化
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

// 处理请求的逻辑函数
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

    // 初始化当前context的事件循环
    if (event_init(&ctx->loop, 1024) == -1) {
        LOG(ERROR, "Fatal: fail to create event loop.");
        exit(EXIT_FAILURE);
    }

    // 初始化当前context的proxy对象, 主要作用:
    // 1. 创建proxy这个链接来监听corvus的TCP server接口
    // 2. 把proxy和当前context绑定
    // 3. 为proxy设置处理请求的函数
    if (proxy_init(&ctx->proxy, ctx, "0.0.0.0", config.bind) == -1) {
        LOG(ERROR, "Fatal: fail to create proxy.");
        exit(EXIT_FAILURE);
    }
    // 把proxy的文件描述符注册到epoll事件循环中, 监听事件类型为可读
    // 实际上就是epoll监听corvus的tcp server接收的请求
    if (event_register(&ctx->loop, &ctx->proxy, E_READABLE) == -1) {
        LOG(ERROR, "Fatal: fail to register proxy.");
        exit(EXIT_FAILURE);
    }

    // 初始化定时器
    if (timer_init(&ctx->timer, ctx) == -1) {
        LOG(ERROR, "Fatal: fail to init timer.");
        exit(EXIT_FAILURE);
    }
    // 把定时器的fd注册到epoll事件循环中, 监听事件类型为可读
    // 实际上就是epoll监听超时事件
    if (event_register(&ctx->loop, &ctx->timer, E_READABLE) == -1) {
        LOG(ERROR, "Fatal: fail to register timer.");
        exit(EXIT_FAILURE);
    }
    // 启动定时器
    if (timer_start(&ctx->timer) == -1) {
        LOG(ERROR, "Fatal: fail to start timer.");
        exit(EXIT_FAILURE);
    }

    // 循环等待, 监听epoll注册的相关事件
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

// 主函数入口
int main(int argc, const char *argv[])
{
    int i, err;
    if (argc < 2) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    /* Load libgcc early to safely call backtrace in signal handler.
     *
     * See:
     * https://www.scribd.com/doc/3726406/Crash-N-Burn-Writing-Linux-application-fault-handlers
     * https://github.com/gby/libcrash/blob/master/crash.c#L279
     */
    void *dummy_trace[1];
    backtrace(dummy_trace, 1);
    backtrace_symbols_fd(dummy_trace, 0, -1);
    if ((err = pthread_spin_init(&signal_lock, 0)) != 0) {
        LOG(ERROR, "Fail to init spin lock: %s", strerror(err));
        return EXIT_FAILURE;
    }

    // 初始化corvus配置
    config_init();
    // 读取配置文件, 并把配置更新到上一步生成的配置对象中
    if (config_read(argv[argc - 1]) == CORVUS_ERR) {
        fprintf(stderr, "Error: invalid config.\n");
        return EXIT_FAILURE;
    }
    // 通过命令行进行配置, 可以覆盖上一步的配置文件中对应的配置
    if (parameter_init(argc, argv) != CORVUS_OK) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    // 用户没有配置redis节点, 报错退出
    if (config.node->len <= 0) {
        fprintf(stderr, "Error: invalid upstream list, `node` should be set to a valid nodes list.\n");
        return EXIT_FAILURE;
    }

    // 用户启用syslog(把corvus的log通过syslog打出来)
    if (config.syslog) {
        openlog(NULL, LOG_NDELAY | LOG_NOWAIT, LOG_USER);
    }

    // allocate memory for `contexts`
    // 初始化context列表
    build_contexts();

    // 初始化信号, 并注册部分信号, 制定触发的函数
    if (setup_signals() == CORVUS_ERR) {
        fprintf(stderr, "Error: failed to setup signals.\n");
        return EXIT_FAILURE;
    }

    // 初始化redis操作dict
    cmd_map_init();

    // start slot management thread
    // 创建slot manager线程, 用来管理slot相关逻辑
    if (slot_start_manager(&contexts[config.thread]) == CORVUS_ERR) {
        LOG(ERROR, "fail to start slot manager thread");
        return EXIT_FAILURE;
    }
    // create first slot updating job
    // 更新slot_job变量, 进而可以触发slot manager线程操作
    slot_create_job(SLOT_UPDATE);

    // start worker threads
    // 创建真正的corvus处理请求的线程
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

    // join所有worker线程, block在这里
    for (i = 0; i < config.thread; i++) {
        if ((err = pthread_join(contexts[i].thread, NULL)) != 0) {
            LOG(WARN, "pthread_join: %s", strerror(err));
        }
    }

    // corvus停止执行, 下面都是需要做的清理工作, 防止内存泄露
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

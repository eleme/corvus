#include <sys/timerfd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include "corvus.h"
#include "connection.h"
#include "logging.h"
#include "event.h"
#include "client.h"
#include "server.h"
#include "timer.h"

/************************************
 * 定时器逻辑:
 * corvus妙地运用了定时器把时间转换成文件描述符, 可以使用epoll来进行监听这个特性, 定时触发超时事件. 在corvus中, 每隔0.5s会触发一次超时
 * 在超时事件发生后,
 *      1. 如果用户设置了client_timeout, corvus会检查客户端到corvus是否有超时连接;
 *      2. 如果用户设置了server_timeout, corvus会检查corvus到redis实例是否有超时连接
 *      3. corvus会检查context的status属性是否发生改变
 ************************************
 */

bool conn_active(struct context *ctx)
{
    struct connection *c;
    TAILQ_FOREACH_REVERSE(c, &ctx->conns, conn_tqh, next) {
        if (c->fd != -1) return true;
    }
    return false;
}

// 检查context的status
void check_context(struct context *ctx)
{
    switch (ctx->state) {
        case CTX_BEFORE_QUIT:
            config.client_timeout = 5;
            event_deregister(&ctx->loop, &ctx->proxy);
            conn_free(&ctx->proxy);
            ctx->state = CTX_QUITTING;
        case CTX_QUITTING:
            LOG(DEBUG, "do quit");
            if (!conn_active(ctx)) ctx->state = CTX_QUIT;
            break;
    }
}

// 检查连接
void check_connections(struct context *ctx)
{
    int64_t now = time(NULL);

    struct connection *c, *n;
    // 用户配置了client_timeout
    if (config.client_timeout > 0) {
        c = TAILQ_LAST(&ctx->conns, conn_tqh);
        // 遍历从客户端到corvus的连接, 如果有连接在client_timeout秒之后仍旧没有获得response, 则断开链接
        while (c != NULL) {
            n = TAILQ_PREV(c, conn_tqh, next);
            if (c->fd == -1) break;
            // When a client connection is holding some unfinished cmds
            // and encounters a client_eof (caused by receiving an illegal redis packet for example),
            // the client connection is turned into an intermediate state.
            // In this state the connection object has not called conn_free so the c->fd is not -1 here.
            // But it should not call client_eof again.
            if (c->eof) {
                LOG(WARN, "zombie client");
                c = n;
                continue;
            }

            if (c->info->last_active > 0
                    && now - c->info->last_active > config.client_timeout)
            {
                if (ctx->state != CTX_QUITTING) {
                    LOG(WARN, "client '%s:%d' timed out",
                            c->info->addr.ip, c->info->addr.port);
                }
                client_eof(c);
            }
            c = n;
        }
    }

    // 用户配置了server_timeout
    if (config.server_timeout > 0) {
        // 遍历从corvus到redis实例的连接, 如果有连接在server_timeout秒之后仍旧没有获得response, 则断开链接
        TAILQ_FOREACH(c, &ctx->servers, next) {
            if (c->fd == -1) continue;
            if (c->info->last_active > 0
                    && now - c->info->last_active > config.server_timeout)
            {
                LOG(WARN, "server '%s:%d' timed out",
                        c->info->addr.ip, c->info->addr.port);
                server_eof(c, rep_timeout_err);
            }
        }
    }
}

// 触发定时器执行的函数
void timer_ready(struct connection *self, uint32_t mask)
{
    uint64_t num;
    if (mask & E_READABLE) {    // 判断事件类型是否为可读
        if (read(self->fd, &num, sizeof(num)) == -1) {
            if (errno != EAGAIN) {
                LOG(WARN, "timer read: %s", strerror(errno));
            }
            return;
        }
        if (config.client_timeout > 0 || config.server_timeout > 0) {
            // 如果用户配置了client_timeout或者server_timeout, 则进行连接检查
            check_connections(self->ctx);
        }
        // 检查context的status是否发生变化
        check_context(self->ctx);
    }
}

// 启动定时器
int timer_start(struct connection *timer)
{
    // struct timespec {
    //      time_t tv_sec;      // 秒
    //      long tv_nsec;       // 纳秒
    // }
    // 定时器设置的超时结构体
    // struct itimerspec {
    //      struct timespec it_interval;    表示每隔多长时间超时, 0表示定时器只超时一次, 否则每隔固定时间就超时
    //      struct timespec it_value;       表示定时器第一次超时时间
    // }
    struct itimerspec spec;
    struct timespec now;

    // 获取当前系统时间
    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        LOG(ERROR, "timer: fail to get current time");
        return CORVUS_ERR;
    }

    // 设置定时器第一次超时时间
    spec.it_value.tv_sec = now.tv_sec;
    spec.it_value.tv_nsec = now.tv_nsec;
    // 设置定时器超时间隔
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 500000000;

    // timerfd_settime函数用来启动或关闭有fd指定的定时器. 参数分别为:
    // 1. 定时器fd
    // 2. 0或1, 1表示设置的是绝对时间, 0是相对时间
    // 3. 接受类型为itimerspec的指针
    // 4. 忽略..
    if (timerfd_settime(timer->fd, TFD_TIMER_ABSTIME, &spec, NULL) == -1) {
        LOG(ERROR, "timer fail to settime: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// 初始化定时器
int timer_init(struct connection *timer, struct context *ctx)
{
    conn_init(timer, ctx);

    // 创建定时器描述符, 两个参数分别为:
    // 1. 指定时间类型, CLOCK_REALTIME表示系统范围内的实时时钟, CLOCK_MONOTONIC表示以固定的速率运行，从不进行调整和复位 ,它不受任何系统time-of-day时钟修改的影响
    // 2. 可以是0或者O_CLOEXEC/O_NONBLOCK
    // timerfd_create函数把时间变成一个fd, 该fd在定时器超时的时候变成可读
    int fd = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd == -1) {
        LOG(ERROR, "fail to create timerfd: %s", strerror(errno));
        return CORVUS_ERR;
    }
    timer->fd = fd;     // 定时器fd
    timer->ready = timer_ready;     // 超时触发执行的函数
    return CORVUS_OK;
}

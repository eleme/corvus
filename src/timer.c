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

bool conn_active(struct context *ctx)
{
    struct connection *c;
    TAILQ_FOREACH_REVERSE(c, &ctx->conns, conn_tqh, next) {
        if (c->fd != -1) return true;
    }
    return false;
}

void check_context(struct context *ctx)
{
    switch (ctx->state) {
        case CTX_BEFORE_QUIT:
            config.client_timeout = 1;
            event_deregister(&ctx->loop, &ctx->proxy);
            conn_free(&ctx->proxy);
            ctx->state = CTX_QUITTING;
        case CTX_QUITTING:
            LOG(DEBUG, "do quit");
            if (!conn_active(ctx)) ctx->state = CTX_QUIT;
            break;
    }
}

void check_connections(struct context *ctx)
{
    int64_t now = time(NULL);

    struct connection *c, *n;
    if (config.client_timeout > 0) {
        c = TAILQ_LAST(&ctx->conns, conn_tqh);
        while (c != NULL) {
            n = TAILQ_PREV(c, conn_tqh, next);
            if (c->fd == -1) break;
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

    if (config.server_timeout > 0) {
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

void timer_ready(struct connection *self, uint32_t mask)
{
    uint64_t num;
    if (mask & E_READABLE) {
        if (read(self->fd, &num, sizeof(num)) == -1) {
            if (errno != EAGAIN) {
                LOG(WARN, "timer read: %s", strerror(errno));
            }
            return;
        }
        if (config.client_timeout > 0 || config.server_timeout > 0) {
            check_connections(self->ctx);
        }
        check_context(self->ctx);
    }
}

int timer_start(struct connection *timer)
{
    struct itimerspec spec;
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        LOG(ERROR, "timer: fail to get current time");
        return CORVUS_ERR;
    }

    spec.it_value.tv_sec = now.tv_sec;
    spec.it_value.tv_nsec = now.tv_nsec;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 100000000;

    if (timerfd_settime(timer->fd, TFD_TIMER_ABSTIME, &spec, NULL) == -1) {
        LOG(ERROR, "timer fail to settime: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int timer_init(struct connection *timer, struct context *ctx)
{
    conn_init(timer, ctx);

    int fd = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd == -1) {
        LOG(ERROR, "fail to create timerfd: %s", strerror(errno));
        return CORVUS_ERR;
    }
    timer->fd = fd;
    timer->ready = timer_ready;
    return CORVUS_OK;
}

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

void check_connections(struct connection *self)
{
    int64_t now = time(NULL);
    struct context *ctx = self->ctx;

    struct connection *c;
    TAILQ_FOREACH_REVERSE(c, &ctx->conns, conn_tqh, next) {
        if (c->fd == -1) break;
        if (c->last_active > 0 && now - c->last_active > config.client_timeout) {
            client_eof(c);
        }
    }

    TAILQ_FOREACH(c, &ctx->servers, next) {
        if (c->fd == -1) continue;
        if (c->last_active > 0 && now - c->last_active > config.server_timeout) {
            LOG(WARN, "server '%s:%d' timed out", c->addr.host, c->addr.port);
            server_eof(c, rep_timeout_err);
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
        check_connections(self);
    }
}

int timer_start(struct connection *timer)
{
    struct itimerspec spec;
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        LOG(ERROR, "timer: fail to get current time");
        return -1;
    }

    spec.it_value.tv_sec = now.tv_sec;
    spec.it_value.tv_nsec = now.tv_nsec;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 100000000;

    if (timerfd_settime(timer->fd, TFD_TIMER_ABSTIME, &spec, NULL) == -1) {
        LOG(ERROR, "timer fail to settime: %s", strerror(errno));
        return -1;
    }
    return 0;
}

int timer_init(struct connection *timer, struct context *ctx)
{
    conn_init(timer, ctx);

    int fd = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd == -1) {
        LOG(ERROR, "fail to create timerfd: %s", strerror(errno));
        return -1;
    }
    timer->fd = fd;
    timer->ready = timer_ready;
    return 0;
}

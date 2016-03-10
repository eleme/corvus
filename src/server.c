#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "server.h"
#include "corvus.h"
#include "event.h"
#include "logging.h"
#include "socket.h"
#include "slot.h"

#define SERVER_RETRY_TIMES 3
#define SERVER_NULL -1
#define SERVER_REGISTER_ERROR -2

#define CHECK_REDIRECTED(c, info_addr, msg)                                         \
do {                                                                                \
    if (c->redirected >= SERVER_RETRY_TIMES) {                                      \
        if (msg == NULL) {                                                          \
            LOG(WARN, "mark cmd done after retrying %d times", SERVER_RETRY_TIMES); \
            cmd_mark_done(c);                                                       \
        } else {                                                                    \
            LOG(WARN, "redirect error after retring %d times: (%d)%s:%d -> %s",     \
                    SERVER_RETRY_TIMES, c->slot, c->server->addr.host,              \
                    c->server->addr.port, info_addr);                               \
            server_data_clear(c);                                                   \
            cmd_mark_fail(c, msg);                                                  \
        }                                                                           \
        return CORVUS_OK;                                                           \
    }                                                                               \
    c->redirected++;                                                                \
} while (0)

static const char *req_ask = "*1\r\n$6\r\nASKING\r\n";

void server_data_clear(struct command *cmd)
{
    struct mbuf *n, *b = cmd->rep_buf[0].buf;
    while (b != NULL) {
        n = TAILQ_NEXT(b, next);
        b->refcount--;
        if (b->refcount <= 0 && b->pos >= b->last) {
            TAILQ_REMOVE(b->queue, b, next);
            mbuf_recycle(cmd->ctx, b);
        }
        if (b == cmd->rep_buf[1].buf) break;
        b = n;
    }
    memset(cmd->rep_buf, 0, sizeof(cmd->rep_buf));
}

void server_make_iov(struct connection *server)
{
    struct command *cmd;
    int64_t t = get_time();

    while (!STAILQ_EMPTY(&server->ready_queue)) {
        cmd = STAILQ_FIRST(&server->ready_queue);
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
        STAILQ_NEXT(cmd, ready_next) = NULL;

        if (cmd->stale) {
            cmd_free(cmd);
            continue;
        }

        if (cmd->asking) {
            cmd_iov_add(&server->iov, (void*)req_ask, strlen(req_ask), NULL);
        }
        cmd->rep_time[0] = t;

        if (cmd->prefix != NULL) {
            cmd_iov_add(&server->iov, (void*)cmd->prefix, strlen(cmd->prefix), NULL);
        }
        cmd_create_iovec(cmd->req_buf, &server->iov);
        STAILQ_INSERT_TAIL(&server->waiting_queue, cmd, waiting_next);
    }
}

int server_write(struct connection *server)
{
    if (!STAILQ_EMPTY(&server->ready_queue)) {
        server_make_iov(server);
    }
    if (server->iov.len <= 0) {
        cmd_iov_reset(&server->iov);
        return CORVUS_OK;
    }

    int status = conn_write(server, 0);

    if (status == CORVUS_ERR) {
        LOG(ERROR, "server_write: server %d fail to write iov", server->fd);
        return CORVUS_ERR;
    }
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    server->send_bytes += status;

    if (server->iov.cursor >= server->iov.len) {
        cmd_iov_reset(&server->iov);
    } else if (conn_register(server) == CORVUS_ERR) {
        LOG(ERROR, "server_write: fail to reregister server %d", server->fd);
        return CORVUS_ERR;
    }
    server->last_active = time(NULL);

    return CORVUS_OK;
}

int _server_retry(struct connection *server, struct command *cmd)
{
    if (server == NULL) {
        server_data_clear(cmd);
        cmd_mark_fail(cmd, rep_server_err);
        return SERVER_NULL;
    }
    if (conn_register(server) == CORVUS_ERR) {
        return SERVER_REGISTER_ERROR;
    }
    server->last_active = time(NULL);
    server_data_clear(cmd);
    cmd->server = server;
    STAILQ_INSERT_TAIL(&server->ready_queue, cmd, ready_next);
    return CORVUS_OK;
}

int server_retry(struct command *cmd)
{
    struct connection *server = conn_get_server(cmd->ctx, cmd->slot);

    switch (_server_retry(server, cmd)) {
        case SERVER_NULL:
            LOG(WARN, "server_retry: slot %d fail to get server", cmd->slot);
            return CORVUS_OK;
        case SERVER_REGISTER_ERROR:
            LOG(ERROR, "server_retry: fail to reregister connection %d", server->fd);
            return CORVUS_ERR;
        default:
            return CORVUS_OK;
    }
}

int server_redirect(struct command *cmd, struct redirect_info *info)
{
    int port;
    struct address addr;

    port = socket_parse_addr(info->addr, &addr);
    if (port == CORVUS_ERR) {
        LOG(WARN, "server_redirect: fail to parse addr %s", info->addr);
        server_data_clear(cmd);
        cmd_mark_fail(cmd, rep_addr_err);
        return CORVUS_OK;
    }

    struct connection *server = conn_get_server_from_pool(cmd->ctx, &addr);

    switch (_server_retry(server, cmd)) {
        case SERVER_NULL:
            LOG(WARN, "server_redirect: fail to get server %s", info->addr);
            return CORVUS_OK;
        case SERVER_REGISTER_ERROR:
            LOG(ERROR, "server_redirect: fail to reregister connection %d", server->fd);
            return CORVUS_ERR;
        default:
            return CORVUS_OK;
    }
}

int server_read_reply(struct connection *server, struct command *cmd)
{
    int status = cmd_read(cmd, server, MODE_REP);
    if (status != CORVUS_OK) return status;

    server->completed_commands++;
    if (cmd->asking) return CORVUS_ASKING;

    if (cmd->stale) {
        server_data_clear(cmd);
        return CORVUS_OK;
    }

    if (cmd->reply_type != REP_ERROR) {
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }

    struct redirect_info info = {.type = CMD_ERR, .slot = -1};
    memset(info.addr, 0, sizeof(info.addr));

    if (cmd_parse_redirect(cmd, &info) == CORVUS_ERR) {
        server_data_clear(cmd);
        cmd_mark_fail(cmd, rep_redirect_err);
        return CORVUS_OK;
    }
    switch (info.type) {
        case CMD_ERR_MOVED:
            slot_create_job(SLOT_UPDATE);
            CHECK_REDIRECTED(cmd, info.addr, rep_redirect_err);
            return server_redirect(cmd, &info);
        case CMD_ERR_ASK:
            CHECK_REDIRECTED(cmd, info.addr, rep_redirect_err);
            cmd->asking = 1;
            return server_redirect(cmd, &info);
        case CMD_ERR_CLUSTERDOWN:
            slot_create_job(SLOT_UPDATE);
            CHECK_REDIRECTED(cmd, NULL, NULL);
            return server_retry(cmd);
        default:
            cmd_mark_done(cmd);
            break;
    }
    return CORVUS_OK;
}

int server_read(struct connection *server)
{
    int status = CORVUS_OK;
    struct command *cmd;
    int64_t now = get_time();

    while (!STAILQ_EMPTY(&server->waiting_queue)) {
        cmd = STAILQ_FIRST(&server->waiting_queue);
        status = server_read_reply(server, cmd);

        cmd->rep_time[1] = now;

        switch (status) {
            case CORVUS_ASKING:
                LOG(DEBUG, "recv asking");
                server_data_clear(cmd);
                cmd->asking = 0;
                continue;
            case CORVUS_OK:
                STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
                STAILQ_NEXT(cmd, waiting_next) = NULL;
                if (cmd->stale) cmd_free(cmd);
                continue;
        }
        break;
    }
    server->last_active = -1;
    return status;
}

void server_ready(struct connection *self, uint32_t mask)
{
    if (mask & E_ERROR) {
        LOG(ERROR, "server error: %s:%d %d", self->addr.host, self->addr.port, self->fd);
        server_eof(self, rep_err);
        return;
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "server writable");
        if (self->status == CONNECTING) self->status = CONNECTED;
        if (self->status == CONNECTED) {
            if (server_write(self) == CORVUS_ERR) {
                server_eof(self, rep_err);
                return;
            }
        } else {
            LOG(ERROR, "server not connected");
            server_eof(self, rep_err);
            return;
        }
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "server readable");

        if (!STAILQ_EMPTY(&self->waiting_queue)) {
            switch (server_read(self)) {
                case CORVUS_ERR:
                case CORVUS_EOF:
                    server_eof(self, rep_err);
                    return;
            }
        } else {
            LOG(WARN, "server is readable but waiting_queue is empty");
            server_eof(self, rep_err);
            return;
        }
    }
}

struct connection *server_create(struct context *ctx, int fd)
{
    struct connection *server = conn_create(ctx);
    server->fd = fd;
    server->ready = server_ready;
    return server;
}

void server_eof(struct connection *server, const char *reason)
{
    LOG(WARN, "server eof");

    struct command *c;
    while (!STAILQ_EMPTY(&server->ready_queue)) {
        c = STAILQ_FIRST(&server->ready_queue);
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
        STAILQ_NEXT(c, ready_next) = NULL;
        if (c->stale) {
            cmd_free(c);
        } else {
            cmd_mark_fail(c, reason);
        }
    }

    while (!STAILQ_EMPTY(&server->waiting_queue)) {
        c = STAILQ_FIRST(&server->waiting_queue);
        STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
        STAILQ_NEXT(c, waiting_next) = NULL;
        mbuf_range_clear(server->ctx, c->rep_buf);
        if (c->stale) {
            cmd_free(c);
        } else {
            cmd_mark_fail(c, reason);
        }
    }

    event_deregister(&server->ctx->loop, server);

    // drop all unsent requests
    cmd_iov_reset(&server->iov);
    conn_free(server);
    slot_create_job(SLOT_UPDATE);
}

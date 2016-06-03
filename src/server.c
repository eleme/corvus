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

#define CHECK_REDIRECTED(c, info_addr, msg)                               \
do {                                                                      \
    if (c->redirected >= SERVER_RETRY_TIMES) {                            \
        if (msg == NULL) {                                                \
            LOG(WARN, "mark cmd done after retrying %d times",            \
                    SERVER_RETRY_TIMES);                                  \
            cmd_mark_done(c);                                             \
        } else {                                                          \
            LOG(WARN,                                                     \
                "redirect error after retring %d times: (%d)%s:%d -> %s", \
                SERVER_RETRY_TIMES, c->slot,                              \
                c->server->info->addr.ip,                                 \
                c->server->info->addr.port, info_addr);                   \
            mbuf_range_clear(c->ctx, c->rep_buf);                         \
            cmd_mark_fail(c, msg);                                        \
        }                                                                 \
        return CORVUS_OK;                                                 \
    }                                                                     \
    c->redirected++;                                                      \
} while (0)

static const char *req_ask = "*1\r\n$6\r\nASKING\r\n";
static const char *req_readonly = "*1\r\n$8\r\nREADONLY\r\n";

void server_make_iov(struct conn_info *info)
{
    struct command *cmd;
    int64_t t = get_time();

    while (!STAILQ_EMPTY(&info->ready_queue)) {
        if (info->iov.len - info->iov.cursor > CORVUS_IOV_MAX) {
            break;
        }
        cmd = STAILQ_FIRST(&info->ready_queue);
        STAILQ_REMOVE_HEAD(&info->ready_queue, ready_next);
        STAILQ_NEXT(cmd, ready_next) = NULL;

        if (cmd->stale) {
            cmd_free(cmd);
            continue;
        }

        if (info->readonly) {
            cmd_iov_add(&info->iov, (void*)req_readonly, strlen(req_readonly), NULL);
            info->readonly = false;
            info->readonly_sent = true;
        }

        if (cmd->asking) {
            cmd_iov_add(&info->iov, (void*)req_ask, strlen(req_ask), NULL);
        }
        cmd->rep_time[0] = t;

        if (cmd->prefix != NULL) {
            cmd_iov_add(&info->iov, (void*)cmd->prefix, strlen(cmd->prefix), NULL);
        }
        cmd_create_iovec(cmd->req_buf, &info->iov);
        STAILQ_INSERT_TAIL(&info->waiting_queue, cmd, waiting_next);
    }
}

int server_write(struct connection *server)
{
    struct conn_info *info = server->info;
    if (!STAILQ_EMPTY(&info->ready_queue)) {
        server_make_iov(info);
    }
    if (info->iov.len <= 0) {
        cmd_iov_reset(&info->iov);
        return CORVUS_OK;
    }

    int status = conn_write(server, 0);

    if (status == CORVUS_ERR) {
        LOG(ERROR, "server_write: server %d fail to write iov", server->fd);
        return CORVUS_ERR;
    }
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    ATOMIC_INC(info->send_bytes, status);

    if (info->iov.cursor >= info->iov.len) {
        cmd_iov_free(&info->iov);
    }

    if (!STAILQ_EMPTY(&info->ready_queue) || info->iov.cursor < info->iov.len) {
        if (conn_register(server) == CORVUS_ERR) {
            LOG(ERROR, "server_write: fail to reregister server %d", server->fd);
            return CORVUS_ERR;
        }
    }

    info->last_active = time(NULL);

    return CORVUS_OK;
}

int _server_retry(struct connection *server, struct command *cmd)
{
    if (server == NULL) {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        cmd_mark_fail(cmd, rep_server_err);
        return SERVER_NULL;
    }
    if (conn_register(server) == CORVUS_ERR) {
        return SERVER_REGISTER_ERROR;
    }
    server->info->last_active = time(NULL);
    mbuf_range_clear(cmd->ctx, cmd->rep_buf);
    cmd->server = server;
    STAILQ_INSERT_TAIL(&server->info->ready_queue, cmd, ready_next);
    return CORVUS_OK;
}

int server_retry(struct command *cmd)
{
    struct connection *server = conn_get_server(cmd->ctx, cmd->slot, cmd->cmd_access);

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
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        cmd_mark_fail(cmd, rep_addr_err);
        return CORVUS_OK;
    }

    // redirection always points to master
    struct connection *server = conn_get_server_from_pool(cmd->ctx, &addr, false);

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
    int status = cmd_read_rep(cmd, server);
    if (status != CORVUS_OK) return status;

    ATOMIC_INC(server->info->completed_commands, 1);

    if (server->info->readonly_sent) {
        return CORVUS_READONLY;
    }

    if (cmd->asking) return CORVUS_ASKING;

    if (cmd->stale) {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        return CORVUS_OK;
    }

    if (cmd->reply_type != REP_ERROR) {
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }

    struct redirect_info info = {.type = CMD_ERR, .slot = -1};
    memset(info.addr, 0, sizeof(info.addr));

    if (cmd_parse_redirect(cmd, &info) == CORVUS_ERR) {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
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
    struct conn_info *info = server->info;
    int64_t now = get_time();

    while (!STAILQ_EMPTY(&info->waiting_queue)) {
        cmd = STAILQ_FIRST(&info->waiting_queue);
        status = server_read_reply(server, cmd);

        cmd->rep_time[1] = now;

        switch (status) {
            case CORVUS_ASKING:
                LOG(DEBUG, "recv asking");
                mbuf_range_clear(cmd->ctx, cmd->rep_buf);
                cmd->asking = 0;
                continue;
            case CORVUS_READONLY:
                LOG(DEBUG, "recv readonly");
                mbuf_range_clear(cmd->ctx, cmd->rep_buf);
                server->info->readonly_sent = false;
                continue;
            case CORVUS_OK:
                STAILQ_REMOVE_HEAD(&info->waiting_queue, waiting_next);
                STAILQ_NEXT(cmd, waiting_next) = NULL;
                stats_log_slow_cmd(cmd);
                if (cmd->stale) cmd_free(cmd);
                continue;
        }
        break;
    }
    info->last_active = -1;
    return status;
}

void server_ready(struct connection *self, uint32_t mask)
{
    struct conn_info *info = self->info;

    if (mask & E_ERROR) {
        LOG(ERROR, "server error: %s:%d %d", info->addr.ip, info->addr.port, self->fd);
        server_eof(self, rep_err);
        return;
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "server writable");
        if (info->status == CONNECTING) info->status = CONNECTED;
        if (info->status == CONNECTED) {
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

        if (!STAILQ_EMPTY(&info->waiting_queue)) {
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
    server->info = conn_info_create(ctx);
    server->fd = fd;
    server->ready = server_ready;
    return server;
}

void server_eof(struct connection *server, const char *reason)
{
    LOG(WARN, "server eof");

    struct command *c;
    while (!STAILQ_EMPTY(&server->info->ready_queue)) {
        c = STAILQ_FIRST(&server->info->ready_queue);
        STAILQ_REMOVE_HEAD(&server->info->ready_queue, ready_next);
        STAILQ_NEXT(c, ready_next) = NULL;
        if (c->stale) {
            cmd_free(c);
        } else {
            cmd_mark_fail(c, reason);
        }
    }

    // remove unprocessed data
    struct mbuf *b = TAILQ_LAST(&server->info->data, mhdr);
    if (b != NULL && b->pos < b->last) {
        b->pos = b->last;
    }

    while (!STAILQ_EMPTY(&server->info->waiting_queue)) {
        c = STAILQ_FIRST(&server->info->waiting_queue);
        STAILQ_REMOVE_HEAD(&server->info->waiting_queue, waiting_next);
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
    cmd_iov_free(&server->info->iov);
    conn_free(server);
    slot_create_job(SLOT_UPDATE);
}

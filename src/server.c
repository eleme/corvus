#include <stdlib.h>
#include "server.h"
#include "corvus.h"
#include "event.h"
#include "logging.h"
#include "socket.h"
#include "slot.h"

static const char *req_ask = "*1\r\n$6\r\nASKING\r\n";

void server_free_buf(struct command *cmd)
{
    struct mbuf *b, *buf;
    b = cmd->rep_buf[0].buf;
    while (b != NULL) {
        if (b == cmd->rep_buf[0].buf && b == cmd->rep_buf[1].buf) {
            mbuf_dec_ref_by(b, 2);
        } else if (b == cmd->rep_buf[0].buf || b == cmd->rep_buf[1].buf) {
            mbuf_dec_ref(b);
        }

        buf = STAILQ_NEXT(b, next);
        if (b->refcount <= 0 && (b != cmd->rep_buf[1].buf
                    || cmd->rep_buf[1].pos >= b->last))
        {
            STAILQ_REMOVE(&cmd->server->data, b, mbuf, next);
            STAILQ_NEXT(b, next) = NULL;
            mbuf_recycle(cmd->ctx, b);
        }
        if (b == cmd->rep_buf[1].buf) break;
        b = buf;
    }
    memset(&cmd->rep_buf, 0, sizeof(cmd->rep_buf));
}

void server_make_iov(struct connection *server)
{
    struct command *cmd;
    double t = get_time();

    while (!STAILQ_EMPTY(&server->ready_queue)) {
        cmd = STAILQ_FIRST(&server->ready_queue);
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
        STAILQ_NEXT(cmd, ready_next) = NULL;

        if (cmd->stale) {
            cmd_free(cmd);
            continue;
        }

        if (cmd->asking) {
            cmd_iov_add(&server->iov, (void*)req_ask, strlen(req_ask));
        }
        cmd->rep_time[0] = t;

        cmd_create_iovec(&cmd->req_buf[0], &cmd->req_buf[1], &server->iov);

        STAILQ_INSERT_TAIL(&server->waiting_queue, cmd, waiting_next);
    }
}

int server_write(struct connection *server)
{
    if (!STAILQ_EMPTY(&server->ready_queue)) {
        server_make_iov(server);
    }
    if (server->iov.len <= 0) {
        cmd_iov_free(&server->iov);
        return CORVUS_OK;
    }

    int status = cmd_iov_write(server->ctx, &server->iov, server->fd);

    if (status == CORVUS_ERR) return CORVUS_ERR;
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    server->send_bytes += status;

    if (server->iov.cursor >= server->iov.len) {
        cmd_iov_free(&server->iov);
    } else {
        if (conn_register(server) == CORVUS_ERR) {
            LOG(ERROR, "fail to reregister server %d", server->fd);
            return CORVUS_ERR;
        }
    }

    return CORVUS_OK;
}

int server_redirect(struct command *cmd, struct redirect_info *info)
{
    int port;
    struct address addr;

    if (cmd->redirected) {
        LOG(WARN, "multiple redirect error");
        cmd_mark_fail(cmd);
        return CORVUS_OK;
    } else {
        cmd->redirected = 1;
    }

    server_free_buf(cmd);
    redis_data_free(cmd->ctx, cmd->rep_data);
    cmd->rep_data = NULL;

    port = socket_parse_addr(info->addr, &addr);
    if (port == CORVUS_ERR) {
        LOG(WARN, "server_redirect: fail to parse addr %s", info->addr);
        cmd_mark_fail(cmd);
        return CORVUS_OK;
    }

    struct connection *server = conn_get_server_from_pool(cmd->ctx, &addr);
    if (server == NULL) {
        LOG(WARN, "server_redirect: fail to get server %s", info->addr);
        cmd_mark_fail(cmd);
        return CORVUS_OK;
    }

    cmd->server = server;
    STAILQ_INSERT_TAIL(&server->ready_queue, cmd, ready_next);

    switch (conn_register(server)) {
        case CORVUS_ERR: return CORVUS_ERR;
        case CORVUS_OK: break;
    }
    return CORVUS_OK;
}

int read_one_reply(struct connection *server)
{
    if (STAILQ_EMPTY(&server->waiting_queue)) return CORVUS_AGAIN;

    struct command *cmd = STAILQ_FIRST(&server->waiting_queue);

    int status = cmd_read_reply(cmd, server);
    /* after read */
    cmd->rep_time[1] = get_time();
    if (status == CORVUS_OK) {
        server->completed_commands++;
        if (!cmd->asking) {
            STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
            STAILQ_NEXT(cmd, waiting_next) = NULL;
        } else {
            LOG(DEBUG, "recv asking");
            server_free_buf(cmd);
            redis_data_free(cmd->ctx, cmd->rep_data);
            cmd->rep_data = NULL;
            cmd->asking = 0;
            return CORVUS_OK;
        }
    }

    switch (status) {
        case CORVUS_AGAIN: return CORVUS_AGAIN;
        case CORVUS_EOF: return CORVUS_EOF;
        case CORVUS_ERR: return CORVUS_ERR;
    }

    if (cmd->stale) {
        server_free_buf(cmd);
        cmd_free(cmd);
        return CORVUS_OK;
    }

    if (cmd->rep_data->type != REP_ERROR) {
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;
    memset(info.addr, 0, sizeof(info.addr));

    cmd_parse_redirect(cmd, &info);
    switch (info.type) {
        case CMD_ERR_MOVED:
            if (server_redirect(cmd, &info) == CORVUS_ERR) return CORVUS_ERR;
            slot_create_job(SLOT_UPDATE, NULL);
            break;
        case CMD_ERR_ASK:
            if (server_redirect(cmd, &info) == CORVUS_ERR) return CORVUS_ERR;
            cmd->asking = 1;
            break;
        default:
            cmd_mark_done(cmd);
            break;
    }
    return CORVUS_OK;
}

int server_read(struct connection *server)
{
    int status;
    do {
        status = read_one_reply(server);
    } while (status == CORVUS_OK);
    return status;
}

void server_ready(struct connection *self, uint32_t mask)
{
    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        server_eof(self);
        return;
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "server writable");
        if (self->status == CONNECTING) self->status = CONNECTED;
        if (self->status == CONNECTED) {
            if (server_write(self) == CORVUS_ERR) {
                server_eof(self);
                return;
            }
        } else {
            LOG(ERROR, "server not connected");
            server_eof(self);
            return;
        }
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "server readable");

        if (!STAILQ_EMPTY(&self->waiting_queue)) {
            switch (server_read(self)) {
                case CORVUS_ERR:
                case CORVUS_EOF:
                    server_eof(self);
                    return;
            }
        } else {
            LOG(WARN, "server is readable but waiting_queue is empty");
            server_eof(self);
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

void server_eof(struct connection *server)
{
    LOG(WARN, "server eof");

    struct command *c;
    while (!STAILQ_EMPTY(&server->ready_queue)) {
        c = STAILQ_FIRST(&server->ready_queue);
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
        STAILQ_NEXT(c, ready_next) = NULL;
        cmd_mark_fail(c);
    }

    while (!STAILQ_EMPTY(&server->waiting_queue)) {
        c = STAILQ_FIRST(&server->waiting_queue);
        STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
        STAILQ_NEXT(c, waiting_next) = NULL;
        cmd_mark_fail(c);
    }

    event_deregister(server->ctx->loop, server);
    /* not free connection buffer */
    conn_free(server);
    slot_create_job(SLOT_UPDATE, NULL);
}

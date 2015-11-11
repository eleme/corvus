#include <sys/queue.h>
#include <stdlib.h>
#include "server.h"
#include "connection.h"
#include "event.h"
#include "logging.h"
#include "socket.h"
#include "corvus.h"
#include "slot.h"
#include "command.h"

static inline void remove_queue_head(struct connection *server, int retry)
{
    if (retry) {
        STAILQ_REMOVE_HEAD(&server->retry_queue, retry_next);
    } else {
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
    }
}

static void server_eof(struct connection *server)
{
    LOG(WARN, "server eof");

    struct command *c;
    conn_close(server);
    STAILQ_FOREACH(c, &server->ready_queue, ready_next) {
        cmd_mark_fail(c);
        STAILQ_REMOVE_HEAD(&server->ready_queue, ready_next);
    }

    STAILQ_FOREACH(c, &server->waiting_queue, waiting_next) {
        cmd_mark_fail(c);
        STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);
    }

    STAILQ_FOREACH(c, &server->retry_queue, retry_next) {
        cmd_mark_fail(c);
        STAILQ_REMOVE_HEAD(&server->retry_queue, retry_next);
    }
    slot_create_job(SLOT_UPDATE, NULL);
}

static int on_write(struct connection *server, int retry)
{
    int status;
    struct command *cmd;

    cmd = retry ? STAILQ_FIRST(&server->retry_queue) : STAILQ_FIRST(&server->ready_queue);

    struct iov_data iov;
    memset(&iov, 0, sizeof(struct iov_data));

    if (cmd->iov.head == NULL) {
        cmd_create_iovec(NULL, &cmd->req_buf[0], &cmd->req_buf[1], &cmd->iov);
        cmd->iov.head = cmd->iov.data;
        cmd->iov.size = cmd->iov.len;
    }

    if (cmd->iov.len <= 0) {
        LOG(WARN, "no data to write");
        remove_queue_head(server, retry);
        return CORVUS_ERR;
    }

    status = cmd_write_iov(cmd, server->fd);

    if (status == CORVUS_AGAIN) return CORVUS_OK;
    if (cmd->iov.len <= 0) {
        cmd_free_iov(&cmd->iov);
        remove_queue_head(server, retry);
        STAILQ_INSERT_TAIL(&server->waiting_queue, cmd, waiting_next);
    } else {
        event_reregister(cmd->ctx->loop, server, E_WRITABLE | E_READABLE);
    }

    if (status == CORVUS_ERR) return CORVUS_ERR;
    return CORVUS_OK;
}

static void do_moved(struct command *cmd, struct redirect_info *info)
{
    int port;
    char *hostname;
    struct sockaddr sockaddr;

    port = socket_parse_addr(info->addr, &hostname);
    socket_get_addr(hostname, port, &sockaddr);

    struct connection *server = conn_get_server_from_pool(cmd->ctx, &sockaddr);
    if (server == NULL) {
        /* cmd_mark_fail(cmd); */
    }
    STAILQ_INSERT_TAIL(&server->retry_queue, cmd, retry_next);

    switch (server->registered) {
        case 1: event_reregister(cmd->ctx->loop, server, E_WRITABLE | E_READABLE); break;
        case 0: event_register(cmd->ctx->loop, server); break;
    }
    slot_create_job(SLOT_UPDATE, NULL);
}

static int read_one_reply(struct connection *server)
{
    if (STAILQ_EMPTY(&server->waiting_queue)) return CORVUS_AGAIN;

    struct command *cmd = STAILQ_FIRST(&server->waiting_queue);

    int status = cmd_read_reply(cmd, server);
    if (status == CORVUS_AGAIN) return CORVUS_AGAIN;

    STAILQ_REMOVE_HEAD(&server->waiting_queue, waiting_next);

    if (status == CORVUS_EOF) {
        cmd_mark_fail(cmd);
        return CORVUS_EOF;
    }

    if (status == CORVUS_ERR) {
        return CORVUS_ERR;
    }

    struct redirect_info info = {.addr = NULL, .type = CMD_ERR, .slot = -1};
    int moved = 0;
    switch (cmd->rep_data->type) {
        case REP_ERROR:
            cmd_parse_redirect(cmd, &info);
            LOG(DEBUG, "redirect -> %s", info.addr);
            switch (info.type) {
                case CMD_ERR_MOVED:
                    do_moved(cmd, &info);
                    moved = 1;
                    break;
            }
            if (moved) break;
        default:
            LOG(DEBUG, "mark done");
            cmd_mark_done(cmd);
            event_reregister(server->ctx->loop, cmd->client, E_WRITABLE | E_READABLE);
            break;
    }
    if (info.addr != NULL) free(info.addr);
    return CORVUS_OK;
}

static int on_read(struct connection *server)
{
    int status;
    do {
        status = read_one_reply(server);
    } while (status == CORVUS_OK);
    return status;
}

static void server_ready(struct connection *self, struct event_loop *loop, uint32_t mask)
{
    int retry = -1;

    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        event_deregister(loop, self);
        server_eof(self);
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "server writable");
        if (self->status == CONNECTING) self->status = CONNECTED;
        if (self->status == CONNECTED) {
            if (!STAILQ_EMPTY(&self->retry_queue)) {
                retry = 1;
            } else if (!STAILQ_EMPTY(&self->ready_queue)) {
                retry = 0;
            }

            if (retry != -1) {
                switch (on_write(self, retry)) {
                    case CORVUS_ERR:
                        event_deregister(loop, self);
                        server_eof(self);
                        break;
                }
            }

        } else {
            LOG(ERROR, "server not connected");
            event_deregister(loop, self);
            server_eof(self);
        }
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "server readable");

        if (!STAILQ_EMPTY(&self->waiting_queue)) {
            switch (on_read(self)) {
                case CORVUS_ERR:
                case CORVUS_EOF:
                    event_deregister(loop, self);
                    server_eof(self);
                    break;
            }
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

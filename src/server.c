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
}

static int on_write(struct connection *server, struct cmd_tqh *queue)
{
    int status;
    struct command *cmd;

    cmd = STAILQ_FIRST(queue);

    struct iov_data iov;
    memset(&iov, 0, sizeof(struct iov_data));
    cmd_create_iovec(&cmd->req_buf[0], &cmd->req_buf[1], &iov);
    if (iov.len <= 0) {
        LOG(WARN, "no data to write");
        return -1;
    }

    status = socket_write(server->fd, iov.data, iov.len);
    free(iov.data);
    if (status != CORVUS_AGAIN) {
        STAILQ_REMOVE_HEAD(queue, ready_next);
        if (status != CORVUS_ERR) {
            STAILQ_INSERT_TAIL(&server->waiting_queue, cmd, waiting_next);
        }
    }
    return 0;
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
    switch (cmd->rep_data->type) {
        case REP_ERROR:
            LOG(DEBUG, "error");
            cmd_parse_redirect(cmd, &info);
            LOG(DEBUG, "redirect -> %s", info.addr);
            switch (info.type) {
                case CMD_ERR_MOVED:
                    do_moved(cmd, &info);
                    break;
            }
            break;
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

static void ready(struct connection *self, struct event_loop *loop, uint32_t mask)
{
    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        event_deregister(self->ctx->loop, self);
        server_eof(self);
        slot_create_job(SLOT_UPDATE, NULL);
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "server writable");
        if (self->status == CONNECTING) self->status = CONNECTED;
        if (self->status == CONNECTED) {
            if (!STAILQ_EMPTY(&self->retry_queue)) {
                if (on_write(self, &self->retry_queue) == -1) {}
            } else if (!STAILQ_EMPTY(&self->ready_queue)) {
                if (on_write(self, &self->ready_queue) == -1) {}
            }
        } else {
            LOG(ERROR, "server not connected");
        }
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "server readable");

        if (!STAILQ_EMPTY(&self->waiting_queue)) {
            switch (on_read(self)) {
                case CORVUS_ERR:
                case CORVUS_EOF:
                    event_deregister(self->ctx->loop, self);
                    server_eof(self);
                    slot_create_job(SLOT_UPDATE, NULL);
                    break;
            }

        }
    }
}

struct connection *server_create(struct context *ctx, int fd)
{
    struct connection *server = conn_create(ctx);
    server->fd = fd;
    server->ready = ready;
    return server;
}

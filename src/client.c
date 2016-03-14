#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "corvus.h"
#include "client.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"

int client_trigger_event(struct connection *client, struct mbuf *buf)
{
    if (buf->pos < buf->last && !client->event_triggered) {
        if (socket_trigger_event(client->ev->fd) == CORVUS_ERR) {
            LOG(ERROR, "%s: fail to trigger readable event", __func__);
            return CORVUS_ERR;
        }
        client->event_triggered = true;
    }
    return CORVUS_OK;
}

int client_read(struct connection *client, int read_socket)
{
    struct command *cmd;
    struct mbuf *buf;
    int status = CORVUS_OK, limit = 16;

    if (!STAILQ_EMPTY(&client->info->cmd_queue)
            && STAILQ_FIRST(&client->info->cmd_queue)->parse_done)
    {
        event_reregister(&client->ctx->loop, client, E_WRITABLE);
        return CORVUS_OK;
    }

    do {
        buf = conn_get_buf(client);
        if (mbuf_read_size(buf) <= 0) {
            if (!read_socket) break;
            buf = conn_get_buf(client);
            status = conn_read(client, buf);
            if (status != CORVUS_OK) return status;
        }

        cmd = conn_get_cmd(client);
        cmd->client = client;

        status = cmd_parse_req(cmd, buf);
        if (cmd->parse_done && (--limit) < 0) {
            return client_trigger_event(client, buf);
        }
    } while (status == CORVUS_OK);

    return status;
}

void client_make_iov(struct conn_info *info)
{
    struct command *cmd;
    int64_t t = get_time();

    while (!STAILQ_EMPTY(&info->cmd_queue)) {
        cmd = STAILQ_FIRST(&info->cmd_queue);
        LOG(DEBUG, "client make iov %d %d", cmd->cmd_count, cmd->cmd_done_count);
        if (cmd->cmd_count != cmd->cmd_done_count) {
            break;
        }
        STAILQ_REMOVE_HEAD(&info->cmd_queue, cmd_next);
        STAILQ_NEXT(cmd, cmd_next) = NULL;

        /* before write */
        cmd->req_time[1] = t;

        cmd_make_iovec(cmd, &info->iov);

        cmd_stats(cmd);
        cmd_free(cmd);
    }
    LOG(DEBUG, "client make iov %d", info->iov.len);
}

int client_write(struct connection *client)
{
    struct context *ctx = client->ctx;
    struct conn_info *info = client->info;

    if (!STAILQ_EMPTY(&info->cmd_queue)) {
        client_make_iov(info);
    }

    if (info->iov.len <= 0) {
        cmd_iov_reset(&info->iov);
        return CORVUS_OK;
    }

    int status = conn_write(client, 1);

    if (status == CORVUS_ERR) {
        LOG(ERROR, "client_write: client %d fail to write iov", client->fd);
        return CORVUS_ERR;
    }
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    if (info->iov.cursor >= info->iov.len) {
        cmd_iov_reset(&info->iov);
        if (event_reregister(&ctx->loop, client, E_READABLE) == CORVUS_ERR) {
            LOG(ERROR, "client_write: fail to reregister client %d", client->fd);
            return CORVUS_ERR;
        }
        if (client_trigger_event(client, conn_get_buf(client)) == CORVUS_ERR) {
            LOG(ERROR, "client_write: fail to trigger event %d %d",
                    client->fd, client->ev->fd);
            return CORVUS_ERR;
        }
    } else if (event_reregister(&ctx->loop, client, E_WRITABLE) == CORVUS_ERR) {
        LOG(ERROR, "client_write: fail to reregister client %d", client->fd);
        return CORVUS_ERR;
    }

    if (client->ctx->state == CTX_BEFORE_QUIT
            || client->ctx->state == CTX_QUITTING)
    {
        return CORVUS_ERR;
    }

    LOG(DEBUG, "client write ok");
    return CORVUS_OK;
}

void client_ready(struct connection *self, uint32_t mask)
{
    if (self->eof) {
        if (self->info->refcount <= 0) {
            conn_free(self);
            conn_recycle(self->ctx, self);
        }
        return;
    }

    self->info->last_active = time(NULL);

    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        client_eof(self);
        return;
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "client readable");

        int status = client_read(self, 1);
        if (status == CORVUS_ERR || status == CORVUS_EOF) {
            client_eof(self);
            return;
        }
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "client writable");
        if (client_write(self) == CORVUS_ERR) {
            client_eof(self);
            return;
        }
    }
}

void client_event_ready(struct connection *self, uint32_t mask)
{
    struct connection *client = self->parent;
    client->event_triggered = false;

    if (client->eof) {
        if (client->info->refcount <= 0) {
            conn_free(client);
            conn_recycle(client->ctx, client);
        }
        return;
    }

    client->info->last_active = time(NULL);

    if (mask & E_READABLE) {
        if (client_read(client, 0) == CORVUS_ERR) {
            client_eof(client);
            return;
        }
    }
}

struct connection *client_create(struct context *ctx, int fd)
{
    struct connection *client = conn_create(ctx);
    client->fd = fd;

    if (socket_set_nonblocking(client->fd) == -1) {
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }
    if (socket_set_tcpnodelay(client->fd) == -1) {
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }

    client->info = conn_info_create(ctx);
    if (client->info == NULL) {
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }

    int evfd = socket_create_eventfd();
    client->ev = conn_create(ctx);
    if (evfd == -1 || client->ev == NULL) {
        LOG(ERROR, "%s: fail to create event connection", __func__);
        if (evfd != -1) close(evfd);
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }

    client->ev->fd = evfd;
    client->ev->ready = client_event_ready;
    client->ev->parent = client;

    client->ready = client_ready;
    client->info->last_active = time(NULL);
    return client;
}

void client_eof(struct connection *client)
{
    LOG(DEBUG, "client eof");
    client->eof = true;

    struct command *cmd;
    while (!STAILQ_EMPTY(&client->info->cmd_queue)) {
        cmd = STAILQ_FIRST(&client->info->cmd_queue);
        STAILQ_REMOVE_HEAD(&client->info->cmd_queue, cmd_next);
        cmd_set_stale(cmd);
    }

    ATOMIC_DEC(client->ctx->stats.connected_clients, 1);

    event_deregister(&client->ctx->loop, client);
    if (client->ev != NULL && !client->event_triggered) {
        event_deregister(&client->ctx->loop, client->ev);
    }

    // don't care response any more
    cmd_iov_clear(client->ctx, &client->info->iov);
    cmd_iov_reset(&client->info->iov);

    // request may not write
    if (client->info->refcount <= 0) {
        conn_buf_free(client);
        if (!client->event_triggered) {
            conn_free(client);
            conn_recycle(client->ctx, client);
        }
    }
}

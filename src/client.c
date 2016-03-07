#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "corvus.h"
#include "client.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"

int client_read(struct connection *client)
{
    int status;
    struct command *cmd;

    do {
        cmd = cmd_get(client);
        if (cmd == NULL) {
            LOG(ERROR, "client_read: fail to create command from client %d", client->fd);
            return CORVUS_ERR;
        }

        cmd->client = client;
        status = cmd_read(cmd, client, MODE_REQ);
    } while (status == CORVUS_OK);

    return status;
}

void client_make_iov(struct connection *client)
{
    struct command *cmd;
    int64_t t = get_time();

    while (!STAILQ_EMPTY(&client->cmd_queue)) {
        cmd = STAILQ_FIRST(&client->cmd_queue);
        LOG(DEBUG, "client make iov %d %d", cmd->cmd_count, cmd->cmd_done_count);
        if (cmd->cmd_count != cmd->cmd_done_count) {
            break;
        }
        STAILQ_REMOVE_HEAD(&client->cmd_queue, cmd_next);
        STAILQ_NEXT(cmd, cmd_next) = NULL;

        /* before write */
        cmd->req_time[1] = t;

        cmd_make_iovec(cmd, &client->iov);

        cmd_stats(cmd);
        cmd_free(cmd);
    }
    LOG(DEBUG, "client make iov %d", client->iov.len);
}

int client_write(struct connection *client)
{
    if (!STAILQ_EMPTY(&client->cmd_queue)) {
        client_make_iov(client);
    }

    if (client->iov.len <= 0) {
        cmd_iov_reset(&client->iov);
        return CORVUS_OK;
    }

    int status = conn_write(client, 1);

    if (status == CORVUS_ERR) {
        LOG(ERROR, "client_write: client %d fail to write iov", client->fd);
        return CORVUS_ERR;
    }
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    if (client->iov.cursor >= client->iov.len) {
        cmd_iov_reset(&client->iov);
    }
    if (conn_register(client) == CORVUS_ERR) {
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
    int status;

    self->last_active = time(NULL);

    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        client_eof(self);
        return;
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "client readable");

        status = client_read(self);
        if (status == CORVUS_ERR || status == CORVUS_EOF) {
            client_eof(self);
            return;
        }
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "client writable");
        if (!STAILQ_EMPTY(&self->cmd_queue)) {
            if (client_write(self) == CORVUS_ERR) {
                client_eof(self);
                return;
            }
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

    client->ready = client_ready;
    client->last_active = time(NULL);
    return client;
}

void client_eof(struct connection *client)
{
    LOG(DEBUG, "client eof");

    struct command *cmd;
    while (!STAILQ_EMPTY(&client->cmd_queue)) {
        cmd = STAILQ_FIRST(&client->cmd_queue);
        STAILQ_REMOVE_HEAD(&client->cmd_queue, cmd_next);
        cmd_set_stale(cmd);
    }

    client->ctx->stats.connected_clients--;

    event_deregister(&client->ctx->loop, client);

    // don't care response any more
    cmd_iov_clear(client->ctx, &client->iov);
    cmd_iov_reset(&client->iov);

    // request may not write
    if (client->refcount <= 0) {
        conn_free(client);
        conn_buf_free(client);
        conn_recycle(client->ctx, client);
    }
}

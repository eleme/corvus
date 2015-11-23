#include <unistd.h>
#include <stdlib.h>
#include "corvus.h"
#include "client.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"

static int client_write(struct connection *client)
{
    int status;
    struct command *cmd = STAILQ_FIRST(&client->cmd_queue);
    LOG(DEBUG, "client %d %d", cmd->cmd_count, cmd->cmd_done_count);
    if (cmd->cmd_count <= 0 || cmd->cmd_count != cmd->cmd_done_count)
        return CORVUS_OK;

    if (cmd->iov.head == NULL) {
        /* before write */
        cmd->req_time[1] = get_time();
        cmd_make_iovec(cmd, &cmd->iov);
        cmd->iov.head = cmd->iov.data;
        cmd->iov.size = cmd->iov.len;
    }

    if (cmd->iov.len <= 0) {
        LOG(WARN, "no data to write");
        STAILQ_REMOVE_HEAD(&client->cmd_queue, cmd_next);
        cmd_free(cmd);
        return CORVUS_ERR;
    }
    status = cmd_iov_write(cmd->ctx, &cmd->iov, client->fd);

    if (status == CORVUS_ERR) return CORVUS_ERR;
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    if (cmd->iov.len <= 0) {
        STAILQ_REMOVE_HEAD(&client->cmd_queue, cmd_next);
        cmd_stats(cmd);
        cmd_free(cmd);
    } else {
        switch (conn_register(client)) {
            case CORVUS_ERR:
                LOG(ERROR, "fail to register client %d", client->fd);
                return CORVUS_ERR;
            case CORVUS_OK:
                break;
        }
    }

    LOG(DEBUG, "client write ok");
    return CORVUS_OK;
}

static void client_ready(struct connection *self, uint32_t mask)
{
    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        client_eof(self);
        return;
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "client readable");
        struct command *cmd = cmd_get_lastest(self->ctx, &self->cmd_queue);
        cmd->client = self;
        cmd->req_time[0] = get_time();
        switch (cmd_read_request(cmd, self->fd)) {
            case CORVUS_ERR:
            case CORVUS_EOF:
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
    socket_set_nonblocking(fd);

    client->fd = fd;
    client->ready = client_ready;
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

    event_deregister(client->ctx->loop, client);
    conn_free(client);
    conn_recycle(client->ctx, client);
}

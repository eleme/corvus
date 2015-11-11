#include <unistd.h>
#include <stdlib.h>
#include "corvus.h"
#include "client.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"

static void client_eof(struct connection *client)
{
    LOG(DEBUG, "client eof");

    struct command *cmd;
    while (!STAILQ_EMPTY(&client->cmd_queue)) {
        cmd = STAILQ_FIRST(&client->cmd_queue);
        STAILQ_REMOVE_HEAD(&client->cmd_queue, cmd_next);
        cmd_free(cmd);
    }
    conn_free(client);
    conn_recycle(client->ctx, client);
}

static int on_write(struct connection *client)
{
    int status;
    struct command *cmd = STAILQ_FIRST(&client->cmd_queue);
    LOG(DEBUG, "client %d %d", cmd->cmd_count, cmd->cmd_done_count);
    if (cmd->cmd_count <= 0 || cmd->cmd_count != cmd->cmd_done_count)
        return CORVUS_OK;

    if (cmd->iov.head == NULL) {
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
    status = cmd_write_iov(cmd, client->fd);

    if (status == CORVUS_AGAIN) return CORVUS_OK;
    if (cmd->iov.len <= 0) {
        cmd_free_iov(&cmd->iov);
        STAILQ_REMOVE_HEAD(&client->cmd_queue, cmd_next);
        cmd_free(cmd);
    } else {
        event_reregister(client->ctx->loop, client, E_READABLE | E_WRITABLE);
    }

    if (status == CORVUS_ERR) return CORVUS_ERR;

    LOG(DEBUG, "client write ok");
    return CORVUS_OK;
}

static void client_ready(struct connection *self, struct event_loop *loop, uint32_t mask)
{
    if (mask & E_ERROR) {
        LOG(DEBUG, "error");
        event_deregister(loop, self);
        client_eof(self);
    }
    if (mask & E_READABLE) {
        LOG(DEBUG, "client readable");
        struct command *cmd = cmd_get_lastest(self->ctx, &self->cmd_queue);
        cmd->client = self;
        switch (cmd_read_request(cmd, self->fd)) {
            case CORVUS_ERR:
            case CORVUS_EOF:
                event_deregister(loop, self);
                client_eof(self);
                break;
        }
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "client writable");
        if (!STAILQ_EMPTY(&self->cmd_queue)) {
            switch (on_write(self)) {
                case CORVUS_ERR:
                    event_deregister(loop, self);
                    client_eof(self);
                    break;

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

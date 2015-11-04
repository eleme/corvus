#include <unistd.h>
#include <stdlib.h>
#include "corvus.h"
#include "client.h"
#include "connection.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"
#include "command.h"
#include <assert.h>

static int on_read(struct connection *client)
{
    int status;
    struct command *cmd = STAILQ_FIRST(&client->cmd_queue);
    if (!cmd->cmd_done) return 0;

    struct iov_data iov = {.data = NULL, .max_size = 0, .len = 0};
    cmd_make_iovec(cmd, &iov);
    if (iov.len <= 0) {
        LOG(WARN, "no data to write");
        STAILQ_REMOVE_HEAD(&client->cmd_queue, next);
        return 0;
    }
    status = socket_write(client->fd, iov.data, iov.len);
    if (status == CORVUS_AGAIN) return 0;
    STAILQ_REMOVE_HEAD(&client->cmd_queue, next);
    free(iov.data);
    /* cmd_free(cmd); */

    if (status == CORVUS_ERR) return -1;
    return 0;
}

static void ready(struct connection *self, struct event_loop *loop, uint32_t mask)
{
    if (mask & E_READABLE) {
        LOG(DEBUG, "client readable");
        struct command *cmd = cmd_get_lastest(self->ctx, &self->cmd_queue);
        cmd->client = self;
        if (cmd_read_request(cmd, self->fd) == -1) {
            LOG(ERROR, "protocol error");
            event_deregister(loop, self);
            close(self->fd);
        }
    }
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "client writable");
        if (!STAILQ_EMPTY(&self->cmd_queue)) {
            on_read(self);
        }
    }
}

struct connection *client_create(struct context *ctx, int fd)
{
    struct connection *client = conn_create(ctx);
    socket_set_nonblocking(fd);

    client->fd = fd;
    client->ready = ready;
    return client;
}

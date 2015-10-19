#include <stdlib.h>
#include "corvus.h"
#include "client.h"
#include "connection.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"

static void ready(struct connection *self, struct event_loop *loop, uint32_t mask)
{
    if (mask & E_READABLE) {
        struct mbuf *buf = mbuf_queue_top(self->ctx, &self->buf_queue);
        int n = socket_read(self->fd, buf);
        logger(DEBUG, "Read %d bytes", n);
        logger(DEBUG, "Buf data: %.*s", mbuf_read_size(buf), buf->pos);
    }
}

struct connection *client_create(struct context *ctx, int fd)
{
    struct connection *client = malloc(sizeof(struct connection));
    client->ctx = ctx;
    client->fd = fd;
    client->ready = ready;
    mbuf_queue_init(&client->buf_queue);
    return client;
}

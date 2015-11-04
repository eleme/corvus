#include <stdlib.h>
#include <unistd.h>
#include "corvus.h"
#include "proxy.h"
#include "socket.h"
#include "connection.h"
#include "mbuf.h"
#include "event.h"
#include "client.h"
#include "logging.h"
#include "command.h"

static void ready(struct connection *self, struct event_loop *loop, uint32_t mask)
{
    char ip[16];
    int port;

    if (mask & E_READABLE) {
        int fd = socket_accept(self->fd, ip, sizeof(ip), &port);
        LOG(DEBUG, "accepted %s:%d", ip, port);

        struct connection *client = client_create(self->ctx, fd);
        event_register(loop, client);
    }
}

struct connection *proxy_create(struct context *ctx, char *host, int port)
{
    struct connection *proxy;
    int fd = socket_create_server(host, port);
    if (fd == -1) return NULL;

    LOG(INFO, "serve at %s:%d", host, port);

    proxy = malloc(sizeof(struct connection));
    proxy->ctx = ctx;
    proxy->fd = fd;
    proxy->ready = ready;
    cmd_queue_init(&proxy->cmd_queue);
    return proxy;
}

void proxy_destroy(struct connection *proxy)
{
    close(proxy->fd);
    free(proxy);
}

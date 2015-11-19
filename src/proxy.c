#include <stdlib.h>
#include <unistd.h>
#include "corvus.h"
#include "proxy.h"
#include "socket.h"
#include "event.h"
#include "client.h"
#include "logging.h"

static void proxy_ready(struct connection *self, uint32_t mask)
{
    char ip[16];
    int port;

    if (mask & E_READABLE) {
        int fd = socket_accept(self->fd, ip, sizeof(ip), &port);
        struct connection *client;
        switch (fd) {
            case CORVUS_ERR: close(fd); break;
            case CORVUS_AGAIN: break;
            default:
                client = client_create(self->ctx, fd);
                if (conn_register(client) == CORVUS_ERR) {
                    LOG(ERROR, "fail to register client");
                    conn_free(client);
                    conn_recycle(self->ctx, client);
                }
                self->ctx->stats.connected_clients++;
                break;
        }
        if (fd != CORVUS_ERR) conn_register(self);
    }
}

struct connection *proxy_create(struct context *ctx, char *host, int port)
{
    struct connection *proxy;
    int fd = socket_create_server(host, port);
    if (fd == -1) return NULL;

    proxy = conn_create(ctx);
    ctx->proxy = proxy;
    proxy->fd = fd;
    proxy->ready = proxy_ready;
    return proxy;
}

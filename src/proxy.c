#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "corvus.h"
#include "proxy.h"
#include "socket.h"
#include "event.h"
#include "client.h"
#include "logging.h"

int proxy_accept(struct connection *proxy)
{
    char ip[16];
    int port, fd = socket_accept(proxy->fd, ip, sizeof(ip), &port);
    if (fd == CORVUS_ERR || fd == CORVUS_AGAIN) {
        return fd;
    }

    struct connection *client;
    if ((client = client_create(proxy->ctx, fd)) == NULL) {
        LOG(ERROR, "fail to create client");
        return CORVUS_ERR;
    }
    if (conn_register(client) == CORVUS_ERR) {
        LOG(ERROR, "fail to register client");
        conn_free(client);
        conn_recycle(proxy->ctx, client);
        return CORVUS_ERR;
    }
    proxy->ctx->stats.connected_clients++;
    return CORVUS_OK;
}

void proxy_ready(struct connection *self, uint32_t mask)
{
    if (mask & E_READABLE) {
        int status;
        while (1) {
            status = proxy_accept(self);
            if (status == CORVUS_ERR) {
                LOG(WARN, "proxy_accept error");
                break;
            }
            if (status == CORVUS_AGAIN) break;
        }
        conn_register(self);
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

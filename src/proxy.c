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
    struct context *ctx = proxy->ctx;
    if (fd == CORVUS_ERR || fd == CORVUS_AGAIN) {
        return fd;
    }

    struct connection *client;
    if ((client = client_create(ctx, fd)) == NULL) {
        LOG(ERROR, "proxy_accept: fail to create client");
        return CORVUS_ERR;
    }

    strcpy(client->info->addr.ip, ip);
    client->info->addr.port = port;

    if (conn_register(client) == CORVUS_ERR) {
        LOG(ERROR, "proxy_accept: fail to register client");
        conn_free(client);
        conn_recycle(ctx, client);
        return CORVUS_ERR;
    }

    if (event_register(&client->ctx->loop, client->ev, E_READABLE) == CORVUS_ERR) {
        LOG(ERROR, "proxy_accept: fail to register client event");
        conn_free(client);
        conn_recycle(ctx, client);
        return CORVUS_ERR;
    }
    TAILQ_INSERT_TAIL(&ctx->conns, client, next);

    ATOMIC_INC(ctx->stats.connected_clients, 1);
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

int proxy_init(struct connection *proxy, struct context *ctx, char *host, int port)
{
    int fd = socket_create_server(host, port);
    if (fd == -1) {
        LOG(ERROR, "proxy_init: fail to create server fd");
        return CORVUS_ERR;
    }

    conn_init(proxy, ctx);
    proxy->fd = fd;
    proxy->ready = proxy_ready;
    return CORVUS_OK;
}

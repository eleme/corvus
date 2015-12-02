#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "corvus.h"
#include "socket.h"
#include "command.h"
#include "slot.h"
#include "logging.h"
#include "event.h"
#include "server.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define EMPTY_CMD_QUEUE(queue, field)     \
do {                                      \
    struct command *c;                    \
    while (!STAILQ_EMPTY(queue)) {        \
        c = STAILQ_FIRST(queue);          \
        STAILQ_REMOVE_HEAD(queue, field); \
        cmd_free(c);                      \
    }                                     \
} while (0)

static int verify_server(struct connection *server, struct address *addr)
{
    if (server->status != DISCONNECTED) return 0;

    if (server->fd != -1) close(server->fd);
    memcpy(&server->addr, addr, sizeof(struct address));

    server->fd = conn_create_fd();
    if (server->fd == -1) {
        LOG(ERROR, "fail to create fd");
        conn_free(server);
        return -1;
    }

    if (conn_connect(server) == -1) {
        LOG(ERROR, "fail to connect %s:%d", server->addr.host, server->addr.port);
        conn_free(server);
        return -1;
    }
    server->registered = 0;
    return 0;
}

static struct connection *conn_create_server(struct context *ctx, struct address *addr, char *key)
{
    int fd = conn_create_fd();
    if (fd == -1) return NULL;
    struct connection *server = server_create(ctx, fd);
    memcpy(&server->addr, addr, sizeof(server->addr));

    if (conn_connect(server) == -1) {
        LOG(ERROR, "fail to connect %s:%d", server->addr.host, server->addr.port);
        conn_free(server);
        conn_buf_free(server);
        conn_recycle(ctx, server);
        return NULL;
    }

    hash_set(ctx->server_table, key, (void*)server);
    STAILQ_INSERT_TAIL(&ctx->servers, server, next);
    return server;
}

void conn_init(struct connection *conn, struct context *ctx)
{
    conn->ctx = ctx;
    conn->fd = -1;
    conn->status = DISCONNECTED;
    conn->ready = NULL;
    conn->registered = 0;
    conn->send_bytes = conn->recv_bytes = 0;
    conn->completed_commands = 0;
    memset(&conn->addr, 0, sizeof(conn->addr));
    STAILQ_INIT(&conn->cmd_queue);
    STAILQ_INIT(&conn->ready_queue);
    STAILQ_INIT(&conn->waiting_queue);
    STAILQ_INIT(&conn->data);

    memset(&conn->iov, 0, sizeof(conn->iov));
}

struct connection *conn_create(struct context *ctx)
{
    struct connection *conn;
    if (!STAILQ_EMPTY(&ctx->free_connq)) {
        LOG(DEBUG, "connection get cache");
        conn = STAILQ_FIRST(&ctx->free_connq);
        STAILQ_REMOVE_HEAD(&ctx->free_connq, next);
        ctx->nfree_connq--;
        STAILQ_NEXT(conn, next) = NULL;
    } else {
        conn = malloc(sizeof(struct connection));
    }
    conn_init(conn, ctx);
    return conn;
}

int conn_connect(struct connection *conn)
{
    int status = -1;
    status = socket_connect(conn->fd, conn->addr.host, conn->addr.port);
    switch (status) {
        case CORVUS_ERR: conn->status = DISCONNECTED; return -1;
        case CORVUS_INPROGRESS: conn->status = CONNECTING; break;
        case CORVUS_OK: conn->status = CONNECTED; break;
    }
    return 0;
}

void conn_free(struct connection *conn)
{
    if (conn == NULL) return;
    if (conn->fd != -1) {
        close(conn->fd);
        conn->fd = -1;
    }
    conn->status = DISCONNECTED;
    conn->registered = 0;
    memset(&conn->addr, 0, sizeof(conn->addr));

    cmd_iov_free(&conn->iov);
    if (conn->iov.data != NULL) {
        free(conn->iov.data);
        conn->iov.max_size = 0;
        conn->iov.data = NULL;
    }

    EMPTY_CMD_QUEUE(&conn->cmd_queue, cmd_next);
    EMPTY_CMD_QUEUE(&conn->ready_queue, ready_next);
    EMPTY_CMD_QUEUE(&conn->waiting_queue, waiting_next);
}

void conn_buf_free(struct connection *conn)
{
    struct mbuf *buf;
    while (!STAILQ_EMPTY(&conn->data)) {
        buf = STAILQ_FIRST(&conn->data);
        STAILQ_REMOVE_HEAD(&conn->data, next);
        mbuf_recycle(conn->ctx, buf);
    }
}

void conn_recycle(struct context *ctx, struct connection *conn)
{
    if (!STAILQ_EMPTY(&conn->data)) {
        LOG(WARN, "connection recycle, data buffer not empty");
    }

    STAILQ_NEXT(conn, next) = NULL;
    STAILQ_INSERT_HEAD(&ctx->free_connq, conn, next);
    ctx->nfree_connq++;
}

int conn_create_fd()
{
    int fd = socket_create_stream();
    if (fd == -1) return -1;
    if (socket_set_nonblocking(fd) == -1) {
        LOG(ERROR, "fail to set nonblocking");
        return -1;
    }
    if (socket_set_tcpnodelay(fd) == -1) {
        LOG(ERROR, "fail to set tcpnodelay");
        return -1;
    }
    return fd;
}

struct connection *conn_get_server_from_pool(struct context *ctx, struct address *addr)
{
    struct connection *server;
    char *key;

    key = socket_get_key(addr);
    server = hash_get(ctx->server_table, key);
    if (server != NULL) {
        free(key);
        if (verify_server(server, addr) == -1) return NULL;
        return server;
    }

    server = conn_create_server(ctx, addr, key);
    if (server == NULL) free(key);
    return server;
}

struct connection *conn_get_raw_server(struct context *ctx)
{
    int i, port;
    char *addr;
    struct connection *server = NULL;
    struct address a;

    for (i = 0; i < ctx->node_conf->len; i++) {
        addr = ctx->node_conf->nodes[i];
        port = socket_parse_addr(addr, &a);
        if (port == -1) continue;

        server = conn_get_server_from_pool(ctx, &a);
        if (server == NULL) continue;
        break;
    }
    if (i >= ctx->node_conf->len) {
        LOG(ERROR, "cannot connect to redis server.");
        return NULL;
    }
    return server;
}

struct connection *conn_get_server(struct context *ctx, uint16_t slot)
{
    struct address addr;
    struct connection *server = NULL;

    server = slot_get_node_addr(slot, &addr) ?
        conn_get_server_from_pool(ctx, &addr) :
        conn_get_raw_server(ctx);
    return server;
}

struct mbuf *conn_get_buf(struct connection *conn)
{
    struct mbuf *buf = NULL;

    if (!STAILQ_EMPTY(&conn->data)) buf = STAILQ_LAST(&conn->data, mbuf, next);

    if (buf == NULL || buf->pos >= buf->end) {
        buf = mbuf_get(conn->ctx);
        STAILQ_INSERT_TAIL(&conn->data, buf, next);
    }
    return buf;
}

int conn_register(struct connection *conn)
{
    int status;
    struct context *ctx = conn->ctx;
    switch (conn->registered) {
        case 1:
            status = event_reregister(ctx->loop, conn, E_WRITABLE | E_READABLE);
            if (status == -1) return CORVUS_ERR;
            break;
        case 0:
            status = event_register(ctx->loop, conn);
            if (status == -1) return CORVUS_ERR;
            break;
    }
    return CORVUS_OK;
}

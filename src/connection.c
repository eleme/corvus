#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "connection.h"
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

void conn_init(struct connection *conn, struct context *ctx)
{
    conn->ctx = ctx;
    conn->fd = -1;
    conn->hostname = NULL;
    conn->status = DISCONNECTED;
    conn->ready = NULL;
    conn->registered = 0;
    memset(&conn->addr, 0, sizeof(conn->addr));
    cmd_queue_init(&conn->cmd_queue);
    cmd_queue_init(&conn->waiting_queue);
    cmd_queue_init(&conn->retry_queue);
    STAILQ_INIT(&conn->data);
}

struct connection *conn_create(struct context *ctx)
{
    struct connection *conn = malloc(sizeof(struct connection));
    conn_init(conn, ctx);
    return conn;
}

int conn_connect(struct connection *conn, int use_addr)
{
    int status;
    if (use_addr) {
        status = socket_connect_addr(conn->fd, &conn->addr);
    } else {
        status = socket_connect(conn->fd, conn->hostname, conn->port);
    }
    switch (status) {
        case CORVUS_ERR: conn->status = DISCONNECTED; return -1;
        case CORVUS_INPROGRESS: conn->status = CONNECTING; break;
        case CORVUS_OK: conn->status = CONNECTED; break;
    }
    return 0;
}

void conn_free(struct connection *conn)
{
    if (conn->hostname != NULL) free(conn->hostname);
    close(conn->fd);
    conn->status = DISCONNECTED;
}

int conn_pool_resize(struct connection ***conn, int orig_size, int more)
{
    int bytes = sizeof(struct connection*) * more;
    if (*conn == NULL) {
        *conn = malloc(bytes);
        memset(*conn, 0, bytes);
    } else {
        *conn = realloc(*conn, sizeof(struct connection*) * (orig_size + more));
        memset(*conn + orig_size, 0, bytes);
    }
    return orig_size + more;
}

int conn_create_fd()
{
    int fd = socket_create_stream();
    if (fd == -1) return -1;
    if (socket_set_nonblocking(fd) == -1) {
        LOG(ERROR, "can't set nonblocking");
        return -1;
    }
    return fd;
}

struct connection *conn_get_server_from_pool(struct context *ctx, struct sockaddr *addr)
{
    int fd;
    struct connection *server;
    char key[17];
    socket_get_key(addr, key);

    server = hash_get(ctx->server_table, key);
    if (server != NULL) {
        return server;
    }

    fd = conn_create_fd();
    server = server_create(ctx, fd);
    memcpy(&server->addr, addr, sizeof(struct sockaddr));

    hash_set(ctx->server_table, key, (void*)server);

    if (conn_connect(server, true) == -1) {
        LOG(ERROR, "can't connect");
        return NULL;
    }
    return server;
}

struct connection *conn_get_raw_server(struct context *ctx)
{
    size_t i;
    int fd, port;
    char *hostname, *addr, key[17] = {0};
    struct connection *server = NULL;
    struct sockaddr sockaddr;

    for (i = 0; i < initial_nodes.len; i++) {
        addr = initial_nodes.nodes[i];
        port = socket_parse_addr(addr, &hostname);
        if (port == -1) continue;

        socket_get_addr(hostname, port, &sockaddr);
        socket_get_key(&sockaddr, key);
        server = hash_get(ctx->server_table, key);
        if (server != NULL) {
            free(hostname);
            break;
        }

        fd = conn_create_fd();
        if (fd == -1) continue;
        server = server_create(ctx, fd);
        server->hostname = hostname;
        server->port = port;
        memcpy(&server->addr, &sockaddr, sizeof(sockaddr));
        if (conn_connect(server, true) == -1) {
            conn_free(server);
            continue;
        };
        hash_set(ctx->server_table, key, (void*)server);
        break;
    }
    if (i >= initial_nodes.len) {
        LOG(ERROR, "cannot connect to redis server.");
        return NULL;
    }
    return server;
}

struct connection *conn_get_server(struct context *ctx, uint16_t slot)
{
    struct node_info *node = slot_get_node_info(slot);
    struct connection *server;

    server = (node == NULL) ?
        conn_get_raw_server(ctx) :
        conn_get_server_from_pool(ctx, &node->master);
    return server;
}

struct mbuf *conn_get_buf(struct connection *conn)
{
    struct mbuf *buf;
    STAILQ_FOREACH(buf, &conn->data, next) {
        LOG(DEBUG, "refcount %d", buf->refcount);
    }
    buf = mbuf_queue_top(conn->ctx, &conn->data);
    if (mbuf_full(buf)) {
        buf = mbuf_get(conn->ctx);
        mbuf_queue_insert(&conn->data, buf);
    }
    return buf;
}

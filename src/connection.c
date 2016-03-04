#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "corvus.h"
#include "socket.h"
#include "command.h"
#include "slot.h"
#include "logging.h"
#include "event.h"
#include "server.h"
#include "dict.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <assert.h>

#define EMPTY_CMD_QUEUE(queue, field)     \
do {                                      \
    struct command *c;                    \
    while (!STAILQ_EMPTY(queue)) {        \
        c = STAILQ_FIRST(queue);          \
        STAILQ_REMOVE_HEAD(queue, field); \
        cmd_free(c);                      \
    }                                     \
} while (0)

#define TAILQ_RESET(var, field)           \
do {                                      \
    (var)->field.tqe_next = NULL;         \
    (var)->field.tqe_prev = NULL;         \
} while (0)

static int verify_server(struct connection *server)
{
    if (server->status != DISCONNECTED) {
        return CORVUS_OK;
    }

    if (server->fd != -1) {
        close(server->fd);
    }

    server->fd = conn_create_fd();
    if (server->fd == -1) {
        LOG(ERROR, "verify_server: fail to create fd");
        conn_free(server);
        return CORVUS_ERR;
    }

    if (conn_connect(server) == CORVUS_ERR) {
        LOG(ERROR, "verify_server: fail to connect %s:%d",
                server->addr.host, server->addr.port);
        conn_free(server);
        return CORVUS_ERR;
    }
    server->registered = 0;
    return CORVUS_OK;
}

static struct connection *conn_create_server(struct context *ctx, struct address *addr, char *key)
{
    int fd = conn_create_fd();
    if (fd == -1) {
        LOG(ERROR, "conn_create_server: fail to create fd");
        return NULL;
    }
    struct connection *server = server_create(ctx, fd);
    memcpy(&server->addr, addr, sizeof(server->addr));

    if (conn_connect(server) == CORVUS_ERR) {
        LOG(ERROR, "conn_create_server: fail to connect %s:%d",
                server->addr.host, server->addr.port);
        conn_free(server);
        conn_buf_free(server);
        conn_recycle(ctx, server);
        return NULL;
    }

    strncpy(server->dsn, key, DSN_MAX);
    dict_set(&ctx->server_table, server->dsn, (void*)server);
    TAILQ_INSERT_TAIL(&ctx->servers, server, next);
    return server;
}

void conn_init(struct connection *conn, struct context *ctx)
{
    conn->ctx = ctx;
    conn->fd = -1;
    conn->refcount = 0;
    conn->last_active = -1;
    conn->status = DISCONNECTED;
    conn->ready = NULL;
    conn->registered = 0;
    conn->send_bytes = conn->recv_bytes = 0;
    conn->completed_commands = 0;
    memset(&conn->addr, 0, sizeof(conn->addr));
    STAILQ_INIT(&conn->cmd_queue);
    STAILQ_INIT(&conn->ready_queue);
    STAILQ_INIT(&conn->waiting_queue);
    TAILQ_INIT(&conn->data);
    TAILQ_INIT(&conn->local_data);

    TAILQ_RESET(conn, next);

    memset(&conn->dsn, 0, sizeof(conn->dsn));

    reader_init(&conn->reader);
}

struct connection *conn_create(struct context *ctx)
{
    struct connection *conn;
    if ((conn = TAILQ_FIRST(&ctx->conns)) != NULL && conn->fd == -1) {
        LOG(DEBUG, "connection get cache");
        TAILQ_REMOVE(&ctx->conns, conn, next);
        ctx->nfree_connq--;
    } else {
        conn = malloc(sizeof(struct connection));
        memset(&conn->iov, 0, sizeof(conn->iov));
    }
    conn_init(conn, ctx);
    ctx->stats.conns++;
    return conn;
}

int conn_connect(struct connection *conn)
{
    int status;
    status = socket_connect(conn->fd, conn->addr.host, conn->addr.port);
    switch (status) {
        case CORVUS_ERR: conn->status = DISCONNECTED; return CORVUS_ERR;
        case CORVUS_INPROGRESS: conn->status = CONNECTING; break;
        case CORVUS_OK: conn->status = CONNECTED; break;
    }
    return CORVUS_OK;
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

    EMPTY_CMD_QUEUE(&conn->cmd_queue, cmd_next);
    EMPTY_CMD_QUEUE(&conn->ready_queue, ready_next);
    EMPTY_CMD_QUEUE(&conn->waiting_queue, waiting_next);
}

void conn_buf_free(struct connection *conn)
{
    struct mbuf *buf;
    while (!TAILQ_EMPTY(&conn->data)) {
        buf = TAILQ_FIRST(&conn->data);
        TAILQ_REMOVE(&conn->data, buf, next);
        mbuf_recycle(conn->ctx, buf);
    }
    while (!TAILQ_EMPTY(&conn->local_data)) {
        buf = TAILQ_FIRST(&conn->local_data);
        TAILQ_REMOVE(&conn->local_data, buf, next);
        mbuf_recycle(conn->ctx, buf);
    }
}

void conn_recycle(struct context *ctx, struct connection *conn)
{
    if (!TAILQ_EMPTY(&conn->data)) {
        LOG(WARN, "connection recycle, data buffer not empty");
    }

    ctx->stats.conns--;
    if (conn->next.tqe_next != NULL || conn->next.tqe_prev != NULL) {
        TAILQ_REMOVE(&ctx->conns, conn, next);
        TAILQ_RESET(conn, next);
    }
    TAILQ_INSERT_HEAD(&ctx->conns, conn, next);
    ctx->nfree_connq++;
}

int conn_create_fd()
{
    int fd = socket_create_stream();
    if (fd == -1) {
        LOG(ERROR, "conn_create_fd: fail to create socket");
        return CORVUS_ERR;
    }
    if (socket_set_nonblocking(fd) == -1) {
        LOG(ERROR, "fail to set nonblocking on fd %d", fd);
        return CORVUS_ERR;
    }
    if (socket_set_tcpnodelay(fd) == -1) {
        LOG(WARN, "fail to set tcpnodelay on fd %d", fd);
    }
    return fd;
}

struct connection *conn_get_server_from_pool(struct context *ctx, struct address *addr)
{
    struct connection *server = NULL;
    char key[DSN_MAX];

    socket_get_key(addr, key);
    server = dict_get(&ctx->server_table, key);
    if (server != NULL) {
        if (verify_server(server) == CORVUS_ERR) return NULL;
        return server;
    }

    server = conn_create_server(ctx, addr, key);
    return server;
}

struct connection *conn_get_raw_server(struct context *ctx)
{
    int i;
    struct connection *server = NULL;

    for (i = 0; i < config.node.len; i++) {
        server = conn_get_server_from_pool(ctx, &config.node.addr[i]);
        if (server == NULL) continue;
        break;
    }
    if (server == NULL) {
        LOG(ERROR, "conn_get_raw_server: cannot connect to redis server.");
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

    if (!TAILQ_EMPTY(&conn->data)) buf = TAILQ_LAST(&conn->data, mhdr);

    if (buf == NULL || buf->pos >= buf->end) {
        buf = mbuf_get(conn->ctx);
        buf->queue = &conn->data;
        TAILQ_INSERT_TAIL(&conn->data, buf, next);
    }
    return buf;
}

int conn_register(struct connection *conn)
{
    struct context *ctx = conn->ctx;
    switch (conn->registered) {
        case 1:
            return event_reregister(&ctx->loop, conn, E_WRITABLE | E_READABLE);
        case 0:
            return event_register(&ctx->loop, conn);
    }
    return CORVUS_OK;
}

void conn_add_data(struct connection *conn, uint8_t *data, int n,
        struct buf_ptr *start, struct buf_ptr *end)
{
    struct mhdr *queue = &conn->local_data;
    struct context *ctx = conn->ctx;
    struct mbuf *buf = mbuf_queue_get(ctx, queue);
    int remain = n, wlen, size, len = 0;

    if (remain > 0 && start != NULL) {
        start->pos = buf->last;
        start->buf = buf;
    }

    while (remain > 0) {
        wlen = mbuf_write_size(buf);
        size = remain < wlen ? remain : wlen;
        memcpy(buf->last, data + len, size);
        buf->last += size;
        len += size;
        remain -= size;
        if (remain <= 0 && end != NULL) {
            end->pos = buf->last;
            end->buf = buf;
        }
        if (wlen - size <= 0) {
            buf = mbuf_queue_get(ctx, queue);
        }
    }
}

int conn_write(struct connection *conn, int clear)
{
    ssize_t remain = 0, status, bytes = 0, count = 0;

    int i, n = 0;
    struct iovec *vec = conn->iov.data + conn->iov.cursor;
    struct mbuf **bufs = conn->iov.buf_ptr + conn->iov.cursor;

    while (n < conn->iov.len - conn->iov.cursor) {
        if (n >= CORVUS_IOV_MAX || bytes >= SSIZE_MAX) break;
        bytes += vec[n++].iov_len;
    }

    status = socket_write(conn->fd, vec, n);
    if (status == CORVUS_AGAIN || status == CORVUS_ERR) return status;

    conn->ctx->stats.send_bytes += status;

    if (status < bytes) {
        for (i = 0; i < n; i++) {
            count += vec[i].iov_len;
            if (count > status) {
                remain = vec[i].iov_len - (count - status);
                vec[i].iov_base = (char*)vec[i].iov_base + remain;
                vec[i].iov_len -= remain;
                break;
            }
        }
        n = i;
    }

    conn->iov.cursor += n;

    if (clear) {
        mbuf_decref(conn->ctx, bufs, n);
    }

    return status;
}

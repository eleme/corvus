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
    if (server->info == NULL) {
        LOG(ERROR, "verify_server: connection info of server %d is null",
                server->fd);
        return CORVUS_ERR;
    }
    struct conn_info *info = server->info;
    if (info->status != DISCONNECTED) {
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
                info->addr.ip, info->addr.port);
        conn_free(server);
        return CORVUS_ERR;
    }
    server->registered = false;
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
    struct conn_info *info = server->info;
    memcpy(&info->addr, addr, sizeof(info->addr));

    if (conn_connect(server) == CORVUS_ERR) {
        LOG(ERROR, "conn_create_server: fail to connect %s:%d",
                info->addr.ip, info->addr.port);
        conn_free(server);
        conn_buf_free(server);
        conn_recycle(ctx, server);
        return NULL;
    }

    strncpy(info->dsn, key, DSN_LEN);
    dict_set(&ctx->server_table, info->dsn, (void*)server);
    TAILQ_INSERT_TAIL(&ctx->servers, server, next);
    return server;
}

void conn_info_init(struct conn_info *info)
{
    info->refcount = 0;

    memset(&info->addr, 0, sizeof(info->addr));
    memset(info->dsn, 0, sizeof(info->dsn));

    reader_init(&info->reader);

    info->last_active = -1;
    info->current_buf = NULL;

    STAILQ_INIT(&info->cmd_queue);
    STAILQ_INIT(&info->ready_queue);
    STAILQ_INIT(&info->waiting_queue);
    STAILQ_INIT(&info->buf_times);
    TAILQ_INIT(&info->data);
    TAILQ_INIT(&info->local_data);

    ATOMIC_SET(info->send_bytes, 0);
    ATOMIC_SET(info->recv_bytes, 0);
    ATOMIC_SET(info->completed_commands, 0);
    info->status = DISCONNECTED;
}

void conn_init(struct connection *conn, struct context *ctx)
{
    memset(conn, 0, sizeof(struct connection));

    conn->ctx = ctx;
    conn->fd = -1;
}

struct connection *conn_create(struct context *ctx)
{
    struct connection *conn;
    if ((conn = TAILQ_FIRST(&ctx->conns)) != NULL && conn->fd == -1) {
        LOG(DEBUG, "connection get cache");
        TAILQ_REMOVE(&ctx->conns, conn, next);
        ctx->mstats.free_conns--;
    } else {
        conn = malloc(sizeof(struct connection));
    }
    conn_init(conn, ctx);
    ctx->mstats.conns++;
    return conn;
}

struct conn_info *conn_info_create(struct context *ctx)
{
    struct conn_info *info;
    if (!STAILQ_EMPTY(&ctx->free_conn_infoq)) {
        info = STAILQ_FIRST(&ctx->free_conn_infoq);
        STAILQ_REMOVE_HEAD(&ctx->free_conn_infoq, next);
        ctx->mstats.free_conn_info--;
    } else {
        info = malloc(sizeof(struct conn_info));
        // init iov here
        memset(&info->iov, 0, sizeof(info->iov));
    }
    conn_info_init(info);
    ctx->mstats.conn_info++;
    return info;
}

int conn_connect(struct connection *conn)
{
    int status;
    struct conn_info *info = conn->info;
    if (info == NULL) {
        LOG(ERROR, "connection info of %d is null", conn->fd);
        return CORVUS_ERR;
    }
    status = socket_connect(conn->fd, info->addr.ip, info->addr.port);
    switch (status) {
        case CORVUS_ERR: info->status = DISCONNECTED; return CORVUS_ERR;
        case CORVUS_INPROGRESS: info->status = CONNECTING; break;
        case CORVUS_OK: info->status = CONNECTED; break;
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

    conn->registered = false;

    if (conn->ev != NULL) {
        conn->ev->info = NULL;
        conn_free(conn->ev);
        conn_recycle(conn->ctx, conn->ev);
        conn->ev = NULL;
    }

    if (conn->info == NULL) return;
    struct conn_info *info = conn->info;

    info->status = DISCONNECTED;

    reader_free(&info->reader);
    reader_init(&info->reader);

    EMPTY_CMD_QUEUE(&info->cmd_queue, cmd_next);
    EMPTY_CMD_QUEUE(&info->ready_queue, ready_next);
    EMPTY_CMD_QUEUE(&info->waiting_queue, waiting_next);
}

void conn_buf_free(struct connection *conn)
{
    if (conn->info == NULL) return;
    struct conn_info *info = conn->info;
    struct buf_time *t;
    struct mbuf *buf;

    while (!STAILQ_EMPTY(&info->buf_times)) {
        t = STAILQ_FIRST(&info->buf_times);
        STAILQ_REMOVE_HEAD(&info->buf_times, next);
        buf_time_free(t);
    }
    while (!TAILQ_EMPTY(&info->data)) {
        buf = TAILQ_FIRST(&info->data);
        TAILQ_REMOVE(&info->data, buf, next);
        mbuf_recycle(conn->ctx, buf);
    }
    while (!TAILQ_EMPTY(&info->local_data)) {
        buf = TAILQ_FIRST(&info->local_data);
        TAILQ_REMOVE(&info->local_data, buf, next);
        mbuf_recycle(conn->ctx, buf);
    }
}

void conn_recycle(struct context *ctx, struct connection *conn)
{
    if (conn->info != NULL) {
        ctx->mstats.conn_info--;

        struct conn_info *info = conn->info;
        if (!TAILQ_EMPTY(&info->data)) {
            LOG(WARN, "connection recycle, data buffer not empty");
        }
        STAILQ_INSERT_TAIL(&ctx->free_conn_infoq, info, next);

        ctx->mstats.free_conn_info++;
        conn->info = NULL;
    }

    ctx->mstats.conns--;

    if (conn->next.tqe_next != NULL || conn->next.tqe_prev != NULL) {
        TAILQ_REMOVE(&ctx->conns, conn, next);
        TAILQ_RESET(conn, next);
    }
    TAILQ_INSERT_HEAD(&ctx->conns, conn, next);

    ctx->mstats.free_conns++;
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
    char key[DSN_LEN + 1];

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

/*
 * 'unprocessed buf': buf is full and has data unprocessed.
 *
 * 1. If last buf is nut full, it is returned.
 * 2. If `unprocessed` is true and the last buf is the unprocessed buf,
 *    the last buf is returned.
 * 3. Otherwise a new buf is returned.
 *
 * `local` means whether to get buf from `info->local_data` or `info->data`.
 */
struct mbuf *conn_get_buf(struct connection *conn, bool unprocessed, bool local)
{
    struct mbuf *buf = NULL;
    struct mhdr *queue = local ? &conn->info->local_data : &conn->info->data;

    if (!TAILQ_EMPTY(queue)) {
        buf = TAILQ_LAST(queue, mhdr);
    }

    if (buf == NULL || (unprocessed ? buf->pos : buf->last) >= buf->end) {
        buf = mbuf_get(conn->ctx);
        buf->queue = queue;
        TAILQ_INSERT_TAIL(queue, buf, next);
    }
    return buf;
}

int conn_register(struct connection *conn)
{
    struct context *ctx = conn->ctx;
    if (conn->registered) {
        return event_reregister(&ctx->loop, conn, E_WRITABLE | E_READABLE);
    } else {
        return event_register(&ctx->loop, conn, E_WRITABLE | E_READABLE);
    }
}

void conn_add_data(struct connection *conn, uint8_t *data, int n,
        struct buf_ptr *start, struct buf_ptr *end)
{
    // get buffer from local_data.
    struct mbuf *buf = conn_get_buf(conn, false, true);
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
            buf = conn_get_buf(conn, false, true);
        }
    }
}

int conn_write(struct connection *conn, int clear)
{
    ssize_t remain = 0, status, bytes = 0, count = 0;
    struct conn_info *info = conn->info;

    int i, n = 0;
    struct iovec *vec = info->iov.data + info->iov.cursor;
    struct mbuf **bufs = info->iov.buf_ptr + info->iov.cursor;

    while (n < info->iov.len - info->iov.cursor) {
        if (n >= CORVUS_IOV_MAX || bytes >= SSIZE_MAX) break;
        bytes += vec[n++].iov_len;
    }

    status = socket_write(conn->fd, vec, n);
    if (status == CORVUS_AGAIN || status == CORVUS_ERR) return status;

    ATOMIC_INC(conn->ctx->stats.send_bytes, status);

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

    info->iov.cursor += n;

    if (clear) {
        mbuf_decref(conn->ctx, bufs, n);
    }

    return status;
}

int conn_read(struct connection *conn, struct mbuf *buf)
{
    int n = socket_read(conn->fd, buf);
    if (n == 0) return CORVUS_EOF;
    if (n == CORVUS_ERR) return CORVUS_ERR;
    if (n == CORVUS_AGAIN) return CORVUS_AGAIN;
    ATOMIC_INC(conn->ctx->stats.recv_bytes, n);
    ATOMIC_INC(conn->info->recv_bytes, n);
    return CORVUS_OK;
}

struct command *conn_get_cmd(struct connection *client)
{
    struct command *cmd;
    int reuse = 0;

    if (!STAILQ_EMPTY(&client->info->cmd_queue)) {
        cmd = STAILQ_LAST(&client->info->cmd_queue, command, cmd_next);
        if (!cmd->parse_done) {
            reuse = 1;
        }
    }

    if (!reuse) {
        cmd = cmd_create(client->ctx);
        STAILQ_INSERT_TAIL(&client->info->cmd_queue, cmd, cmd_next);
    }
    return cmd;
}

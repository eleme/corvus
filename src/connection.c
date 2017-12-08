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
#include "alloc.h"

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

static int verify_server(struct connection *server, bool readonly)
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
    if (readonly) {
        server->info->readonly = true;
    }
    return CORVUS_OK;
}

// 创建连接
static struct connection *conn_create_server(struct context *ctx,
        struct address *addr, char *key, bool readonly)
{
    int fd = conn_create_fd();  // 创建socket fd
    if (fd == -1) {
        LOG(ERROR, "conn_create_server: fail to create fd");
        return NULL;
    }
    // 创建server连接, 绑定fd
    struct connection *server = server_create(ctx, fd);
    struct conn_info *info = server->info;
    // 更新server连接中的地址为对应的redis实例地址
    memcpy(&info->addr, addr, sizeof(info->addr));
    extern const size_t CMD_NUM;
    info->slow_cmd_counts = cv_calloc(CMD_NUM, sizeof(uint32_t));

    // 对redis实例建立连接(这里建立了corvus与对应redis实例的真正的链接)
    if (conn_connect(server) == CORVUS_ERR) {
        LOG(ERROR, "conn_create_server: fail to connect %s:%d",
                info->addr.ip, info->addr.port);
        conn_free(server);
        conn_buf_free(server);
        conn_recycle(ctx, server);
        return NULL;
    }

    if (readonly) {
        server->info->readonly = true;
    }

    strncpy(info->dsn, key, ADDRESS_LEN);
    // 把该连接加入连接池
    dict_set(&ctx->server_table, info->dsn, (void*)server);
    TAILQ_INSERT_TAIL(&ctx->servers, server, next);
    return server;
的}

void conn_info_init(struct conn_info *info)
{
    info->refcount = 0;
    info->authenticated = false;
    info->readonly = false;
    info->readonly_sent = false;
    info->quit = false;
    info->slow_cmd_counts = NULL;

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

// 初始化连接, 把context绑定在这个链接上
void conn_init(struct connection *conn, struct context *ctx)
{
    memset(conn, 0, sizeof(struct connection));

    conn->ctx = ctx;
    conn->fd = -1;
}

// 创建一个新的连接
struct connection *conn_create(struct context *ctx)
{
    struct connection *conn;
    // 检查连接队列里队首连接是否可用,
    // 1. 如果不可用, 就从队列中取出这个连接conn
    // 2. 如果可用, 就向内存申请空间, 用于初始化conn
    // 3. 最后初始化conn
    if ((conn = TAILQ_FIRST(&ctx->conns)) != NULL && conn->fd == -1) {
        LOG(DEBUG, "connection get cache");
        TAILQ_REMOVE(&ctx->conns, conn, next);
        ctx->mstats.free_conns--;
    } else {
        conn = cv_malloc(sizeof(struct connection));    // 申请内存空间
    }
    conn_init(conn, ctx);   // 初始化连接
    ctx->mstats.conns++;    // 打点更新连接数
    return conn;
}

struct conn_info *conn_info_create(struct context *ctx)
{
    struct conn_info *info;
    // 检查空闲conn_info队列是否为空,
    // 1. 如果不为空, 则取出队首conn_info
    // 2. 如果为空, 则向内存申请空间, 用于初始化conn_info
    // 3. 最后初始化conn_info
    if (!STAILQ_EMPTY(&ctx->free_conn_infoq)) {
        info = STAILQ_FIRST(&ctx->free_conn_infoq);
        STAILQ_REMOVE_HEAD(&ctx->free_conn_infoq, next);
        ctx->mstats.free_conn_info--;
    } else {
        info = cv_malloc(sizeof(struct conn_info));
        // init iov here
        memset(&info->iov, 0, sizeof(info->iov));
    }
    conn_info_init(info);
    ctx->mstats.conn_info++;
    return info;
}

// 通过给予的connection实例, 对实例里面提供的ip地址和端口建立连接
int conn_connect(struct connection *conn)
{
    int status;
    struct conn_info *info = conn->info;
    if (info == NULL) {
        LOG(ERROR, "connection info of %d is null", conn->fd);
        return CORVUS_ERR;
    }
    // 建立TCP连接并绑定fd
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

// 创建套接字描述符, 并进行配置
int conn_create_fd()
{
    int fd = socket_create_stream();    // 创建socket描述符
    if (fd == -1) {
        LOG(ERROR, "conn_create_fd: fail to create socket");
        return CORVUS_ERR;
    }
    // 设置非阻塞
    if (socket_set_nonblocking(fd) == -1) {
        LOG(ERROR, "fail to set nonblocking on fd %d", fd);
        return CORVUS_ERR;
    }
    // 设置禁用Nagle算法
    if (socket_set_tcpnodelay(fd) == -1) {
        LOG(WARN, "fail to set tcpnodelay on fd %d", fd);
    }
    return fd;
}

// 从连接池中获取从corvus到对应redis实例的连接
struct connection *conn_get_server_from_pool(struct context *ctx,
        struct address *addr, bool readonly)
{
    struct connection *server = NULL;
    char key[ADDRESS_LEN];
    snprintf(key, ADDRESS_LEN, "%s:%d", addr->ip, addr->port);

    server = dict_get(&ctx->server_table, key);
    if (server != NULL) {
        // 如果有连接, 则验证一下链接是否可用, 可用就返回
        if (verify_server(server, readonly) == CORVUS_ERR) return NULL;
        return server;
    }

    // 如果没有获取到链接, 则手动创建一个链接
    server = conn_create_server(ctx, addr, key, readonly);
    return server;
}

struct connection *conn_get_raw_server(struct context *ctx)
{
    int i;
    struct connection *server = NULL;

    struct node_conf *node = config_get_node();
    for (i = 0; i < node->len; i++) {
        server = conn_get_server_from_pool(ctx, &node->addr[i], false);
        if (server == NULL) continue;
        break;
    }
    config_node_dec_ref(node);
    if (server == NULL) {
        LOG(ERROR, "conn_get_raw_server: cannot connect to redis server.");
        return NULL;
    }
    return server;
}

// 获取从corvus到redis实例的连接
struct connection *conn_get_server(struct context *ctx, uint16_t slot,
        int access)
{
    struct address *addr;
    struct node_info info;
    bool readonly = false;

    if (slot_get_node_addr(slot, &info)) {      // 根据slot获取所在的redis节点, 读取到node_info里
        addr = &info.nodes[0];
        // 如果是读请求, 并且启用了读写分离配置, 则判断redis指令的access方式(access详情可以看command.h的第13行)
        // 然后挑选一台slave机器作为目标机
        if (access != CMD_ACCESS_WRITE && config.readslave && info.index > 1) {
            int r = rand_r(&ctx->seed);
            if (!config.readmasterslave || r % info.index != 0) {
                int i = r % (info.index - 1);
                addr = &info.nodes[++i];
                readonly = true;
            }
        }
        // 从连接池中获取从corvus到目标redis机器的连接
        if (addr->port > 0) {
            return conn_get_server_from_pool(ctx, addr, readonly);
        }
    }
    // 如果没有捕获到该slot对应的redis节点, 则直接建立连接
    return conn_get_raw_server(ctx);
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

// 注册连接的对应事件类型到epoll事件循环上, 事件类型为可读或可写
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

// 发送请求到conn连接
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

// 把连接的socket收到的请求读取到缓冲区
int conn_read(struct connection *conn, struct mbuf *buf)
{
    int n = socket_read(conn->fd, buf);
    if (n == 0) return CORVUS_EOF;
    if (n == CORVUS_ERR) return CORVUS_ERR;
    if (n == CORVUS_AGAIN) return CORVUS_AGAIN;
    // 打点相关逻辑
    ATOMIC_INC(conn->ctx->stats.recv_bytes, n);
    ATOMIC_INC(conn->info->recv_bytes, n);
    return CORVUS_OK;
}

// 给对应连接构建cmd对象
struct command *conn_get_cmd(struct connection *client)
{
    struct command *cmd;
    int reuse = 0;

    // 判断cmd_queue队列是否为空
    // 1. 如果不为空, 则取出队尾的command对象, 判断这个对象是否解析完毕.
    //      1. 如果该对象没有解析完毕, 则返回.
    //      2. 如果已经解析完毕, 则根据当前连接创建一个新的command对象, 插入cmd_queue队尾并返回
    // 2. 如果为空, 则根据当前连接创建新command对象, 插入cmd_queue队尾并返回
    if (!STAILQ_EMPTY(&client->info->cmd_queue)) {
        cmd = STAILQ_LAST(&client->info->cmd_queue, command, cmd_next);
        if (!cmd->parse_done) {
            reuse = 1;
        }
    }

    if (!reuse) {
        cmd = cmd_create(client->ctx);      // 创建command对象
        STAILQ_INSERT_TAIL(&client->info->cmd_queue, cmd, cmd_next);
    }
    return cmd;
}

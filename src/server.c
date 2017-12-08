#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "server.h"
#include "corvus.h"
#include "event.h"
#include "logging.h"
#include "socket.h"
#include "slot.h"

#define SERVER_RETRY_TIMES 3
#define SERVER_NULL -1
#define SERVER_REGISTER_ERROR -2

#define CHECK_REDIRECTED(c, info_addr, msg)                               \
do {                                                                      \
    if (c->redirected >= SERVER_RETRY_TIMES) {                            \
        if (msg == NULL) {                                                \
            LOG(WARN, "mark cmd done after retrying %d times",            \
                    SERVER_RETRY_TIMES);                                  \
            cmd_mark_done(c);                                             \
        } else {                                                          \
            LOG(WARN,                                                     \
                "redirect error after retring %d times: (%d)%s:%d -> %s", \
                SERVER_RETRY_TIMES, c->slot,                              \
                c->server->info->addr.ip,                                 \
                c->server->info->addr.port, info_addr);                   \
            mbuf_range_clear(c->ctx, c->rep_buf);                         \
            cmd_mark_fail(c, msg);                                        \
        }                                                                 \
        return CORVUS_OK;                                                 \
    }                                                                     \
    c->redirected++;                                                      \
} while (0)

static const char *req_ask = "*1\r\n$6\r\nASKING\r\n";
static const char *req_readonly = "*1\r\n$8\r\nREADONLY\r\n";

// corvus server构造发送出去的信息
void server_make_iov(struct conn_info *info)
{
    struct command *cmd;
    int64_t t = get_time();

    while (!STAILQ_EMPTY(&info->ready_queue)) {
        if (info->iov.len - info->iov.cursor > CORVUS_IOV_MAX) {
            break;
        }
        // 从ready_queue队列中获取队首command对象
        cmd = STAILQ_FIRST(&info->ready_queue);
        STAILQ_REMOVE_HEAD(&info->ready_queue, ready_next);
        STAILQ_NEXT(cmd, ready_next) = NULL;

        if (cmd->stale) {
            cmd_free(cmd);
            continue;
        }

        if (info->readonly) {
            cmd_iov_add(&info->iov, (void*)req_readonly, strlen(req_readonly), NULL);
            info->readonly = false;
            info->readonly_sent = true;
        }

        if (cmd->asking) {  // 重定向ASKING
            cmd_iov_add(&info->iov, (void*)req_ask, strlen(req_ask), NULL);
        }
        cmd->rep_time[0] = t;
        if (cmd->parent) {
            int64_t parent_rep_start_time = cmd->parent->rep_time[0];
            if (parent_rep_start_time == 0 || parent_rep_start_time > t)
                cmd->parent->rep_time[0] = t;
        }

        if (cmd->prefix != NULL) {
            cmd_iov_add(&info->iov, (void*)cmd->prefix, strlen(cmd->prefix), NULL);
        }
        cmd_create_iovec(cmd->req_buf, &info->iov);     // 构造准备发送到redis实例的请求数据
        // 把command对象放到corvus server监听的waiting_queue队列的队尾
        // 当corvus server发生可读事件的时候, 会调用这个队列
        STAILQ_INSERT_TAIL(&info->waiting_queue, cmd, waiting_next);
    }
}

// corvus server捕获到可写事件触发执行的函数
// 该函数主要作用是:
// 1. 通过command对象, 构造发送到对应redis实例的请求数据
// 2. 把构造好的数据发送到对应redis实例上
int server_write(struct connection *server)
{
    struct conn_info *info = server->info;
    // 构造发送到redis实例的请求数据
    if (!STAILQ_EMPTY(&info->ready_queue)) {
        server_make_iov(info);
    }
    if (info->iov.len <= 0) {
        cmd_iov_reset(&info->iov);
        return CORVUS_OK;
    }

    // 发送请求到对应的redis实例中
    int status = conn_write(server, 0);

    if (status == CORVUS_ERR) {
        LOG(ERROR, "server_write: server %d fail to write iov", server->fd);
        return CORVUS_ERR;
    }
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    ATOMIC_INC(info->send_bytes, status);

    if (info->iov.cursor >= info->iov.len) {
        cmd_iov_free(&info->iov);
    }

    if (!STAILQ_EMPTY(&info->ready_queue) || info->iov.cursor < info->iov.len) {
        if (conn_register(server) == CORVUS_ERR) {
            LOG(ERROR, "server_write: fail to reregister server %d", server->fd);
            return CORVUS_ERR;
        }
    }

    info->last_active = time(NULL);

    return CORVUS_OK;
}

// 请求错误之后, 重试的逻辑
// 1. 针对产生错误的command请求, 重新获取对应的redis实例的连接
// 2. 重新发送请求
int server_enqueue(struct connection *server, struct command *cmd)
{
    // 如果没有获取到对应的连接, 报错返回
    if (server == NULL) {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        cmd_mark_fail(cmd, rep_server_err);
        return SERVER_NULL;
    }
    // 获取到连接, 把连接注册到事件循环上
    if (conn_register(server) == CORVUS_ERR) {
        return SERVER_REGISTER_ERROR;
    }
    server->info->last_active = time(NULL);     // 更新该连接最后活跃时间
    mbuf_range_clear(cmd->ctx, cmd->rep_buf);
    cmd->server = server;
    // 把重新获取了到redis连接的command对象重新放入ready_queue队列中,
    // 等待corvus server触发可写事件时, 执行server_write函数发送请求
    STAILQ_INSERT_TAIL(&server->info->ready_queue, cmd, ready_next);
    return CORVUS_OK;
}

// corvus server重新发送command到redis实例
int server_retry(struct command *cmd)
{
    // 重新获取command对象对应的redis连接
    struct connection *server = conn_get_server(cmd->ctx, cmd->slot, cmd->cmd_access);

    // 重发请求
    switch (server_enqueue(server, cmd)) {
        case SERVER_NULL:   // 没有获取到对应的redis连接
            LOG(WARN, "server_retry: slot %d fail to get server", cmd->slot);
            return CORVUS_OK;
        case SERVER_REGISTER_ERROR:
            LOG(ERROR, "server_retry: fail to reregister connection %d", server->fd);
            return CORVUS_ERR;
        default:
            return CORVUS_OK;
    }
}

int server_redirect(struct command *cmd, struct redirect_info *info)
{
    int port;
    struct address addr;

    port = socket_parse_addr(info->addr, &addr);
    if (port == CORVUS_ERR) {
        LOG(WARN, "server_redirect: fail to parse addr %s", info->addr);
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        cmd_mark_fail(cmd, rep_addr_err);
        return CORVUS_OK;
    }

    // redirection always points to master
    struct connection *server = conn_get_server_from_pool(cmd->ctx, &addr, false);

    switch (server_enqueue(server, cmd)) {
        case SERVER_NULL:
            LOG(WARN, "server_redirect: fail to get server %s", info->addr);
            return CORVUS_OK;
        case SERVER_REGISTER_ERROR:
            LOG(ERROR, "server_redirect: fail to reregister connection %d", server->fd);
            return CORVUS_ERR;
        default:
            return CORVUS_OK;
    }
}

// 读取redis实例返回的response
int server_read_reply(struct connection *server, struct command *cmd)
{
    // 读取并解析redis返回的response, 并把解析结果存入command对象中
    int status = cmd_read_rep(cmd, server);
    if (status != CORVUS_OK) return status;

    ATOMIC_INC(server->info->completed_commands, 1);    // statsd打点相关

    if (server->info->readonly_sent) {
        return CORVUS_READONLY;
    }

    if (cmd->asking) return CORVUS_ASKING;

    if (cmd->stale) {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        return CORVUS_OK;
    }

    if (cmd->reply_type != REP_ERROR) {
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }

    // 初始化redirect_info对象
    struct redirect_info info = {.type = CMD_ERR, .slot = -1};
    memset(info.addr, 0, sizeof(info.addr));

    // 分析redis返回的值, 判断是否需要重定向请求
    if (cmd_parse_redirect(cmd, &info) == CORVUS_ERR) {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        cmd_mark_fail(cmd, rep_redirect_err);
        return CORVUS_OK;
    }
    switch (info.type) {
        // 如果返回值为MOVED, 说明这个slot已经迁移到了另一台redis实例上.
        // 1. corvus就对正确的redis目标机再发送一次请求,
        // 2. 更新slot_job变量, 触发后台线程更新slot-node映射
        case CMD_ERR_MOVED:
            ATOMIC_INC(cmd->ctx->stats.moved_recv, 1);
            slot_create_job(SLOT_UPDATE);
            // 检查发送重定向请求的次数
            CHECK_REDIRECTED(cmd, info.addr, rep_redirect_err);
            return server_redirect(cmd, &info);
        // 如果返回值为ASK, 说明redis集群正在进行slot的迁移.
        // 那么corvus就对正确的redis目标机再发送一次请求,
        case CMD_ERR_ASK:
            ATOMIC_INC(cmd->ctx->stats.ask_recv, 1);
            // 检查发送重定向请求的次数
            CHECK_REDIRECTED(cmd, info.addr, rep_redirect_err);
            cmd->asking = 1;
            return server_redirect(cmd, &info);
        // 如果返回值为CLUSTERDOWN, 说明redis集群发生故障.
        // 1. 更新slot_job变量, 触发后台线程更新slot-node映射
        // 2. 重试命令
        case CMD_ERR_CLUSTERDOWN:
            slot_create_job(SLOT_UPDATE);
            // 检查发送重定向请求的次数
            CHECK_REDIRECTED(cmd, NULL, NULL);
            return server_retry(cmd);
        // 如果没有发生以上三种情况, 说明请求已经正确执行并返回
        default:
            cmd_mark_done(cmd);
            break;
    }
    return CORVUS_OK;
}

// corvus server发生可读事件触发执行的函数
int server_read(struct connection *server)
{
    int status = CORVUS_OK;
    struct command *cmd;
    struct conn_info *info = server->info;
    int64_t now = get_time();

    // 循环读取waiting_queue队列, 以获取发送出去的请求
    while (!STAILQ_EMPTY(&info->waiting_queue)) {
        cmd = STAILQ_FIRST(&info->waiting_queue);   // 获取发送的redis请求
        status = server_read_reply(server, cmd);    // 读取redis返回的响应

        cmd->rep_time[1] = now;
        if (cmd->parent) {
            int64_t parent_rep_end_time = cmd->parent->rep_time[1];
            if (parent_rep_end_time == 0 || parent_rep_end_time < now)
                cmd->parent->rep_time[1] = now;
        }

        switch (status) {
            case CORVUS_ASKING:
                LOG(DEBUG, "recv asking");
                mbuf_range_clear(cmd->ctx, cmd->rep_buf);
                cmd->asking = 0;
                continue;
            case CORVUS_READONLY:
                LOG(DEBUG, "recv readonly");
                mbuf_range_clear(cmd->ctx, cmd->rep_buf);
                server->info->readonly_sent = false;
                continue;
            case CORVUS_OK:
                STAILQ_REMOVE_HEAD(&info->waiting_queue, waiting_next);
                STAILQ_NEXT(cmd, waiting_next) = NULL;
                if (cmd->stale) cmd_free(cmd);
                continue;
        }
        break;
    }
    info->last_active = time(NULL);
    return status;
}

// corvus server连接触发后执行的函数
void server_ready(struct connection *self, uint32_t mask)
{
    struct conn_info *info = self->info;

    if (mask & E_ERROR) {
        LOG(ERROR, "server error: %s:%d %d", info->addr.ip, info->addr.port, self->fd);
        server_eof(self, rep_err);
        return;
    }
    if (mask & E_WRITABLE) {    // 当发生可写事件时
        LOG(DEBUG, "server writable");
        if (info->status == CONNECTING) info->status = CONNECTED;
        if (info->status == CONNECTED) {
            if (server_write(self) == CORVUS_ERR) {
                server_eof(self, rep_err);
                return;
            }
        } else {
            LOG(ERROR, "server not connected");
            server_eof(self, rep_err);
            return;
        }
    }
    if (mask & E_READABLE) {    // 当发生可读事件时
        LOG(DEBUG, "server readable");

        // 查找waiting_queue队列是否为空
        // 如果不是空, 则说明corvus向redis实例发送了请求, 因为发生了可读事件, 所以已经获得了redis的响应
        // 如果是空, 则忽略这个事件
        if (!STAILQ_EMPTY(&info->waiting_queue)) {
            switch (server_read(self)) {
                case CORVUS_ERR:
                case CORVUS_EOF:
                    server_eof(self, rep_err);
                    return;
            }
        } else {
            LOG(WARN, "server is readable but waiting_queue is empty");
            server_eof(self, rep_err);
            return;
        }
    }
}

// 创建server连接, 把该连接与fd绑定.
// 注意, 这里还没有建立corvus与redis实例真正的连接, 这里只是初始化
// 了一个新的connection实例, 并设定了本连接触发后的处理函数
struct connection *server_create(struct context *ctx, int fd)
{
    struct connection *server = conn_create(ctx);
    server->info = conn_info_create(ctx);   // 给该连接初始化conn_info
    server->fd = fd;                // 绑定fd
    server->ready = server_ready;   // 设置本连接触发后的处理函数
    return server;
}

void server_eof(struct connection *server, const char *reason)
{
    LOG(WARN, "server eof");

    struct command *c;
    while (!STAILQ_EMPTY(&server->info->ready_queue)) {
        c = STAILQ_FIRST(&server->info->ready_queue);
        STAILQ_REMOVE_HEAD(&server->info->ready_queue, ready_next);
        STAILQ_NEXT(c, ready_next) = NULL;
        if (c->stale) {
            cmd_free(c);
        } else {
            cmd_mark_fail(c, reason);
        }
    }

    // remove unprocessed data
    struct mbuf *b = TAILQ_LAST(&server->info->data, mhdr);
    if (b != NULL && b->pos < b->last) {
        b->pos = b->last;
    }

    while (!STAILQ_EMPTY(&server->info->waiting_queue)) {
        c = STAILQ_FIRST(&server->info->waiting_queue);
        STAILQ_REMOVE_HEAD(&server->info->waiting_queue, waiting_next);
        STAILQ_NEXT(c, waiting_next) = NULL;
        mbuf_range_clear(server->ctx, c->rep_buf);
        if (c->stale) {
            cmd_free(c);
        } else {
            cmd_mark_fail(c, reason);
        }
    }

    event_deregister(&server->ctx->loop, server);

    // drop all unsent requests
    cmd_iov_free(&server->info->iov);
    conn_free(server);
    slot_create_job(SLOT_UPDATE);
}

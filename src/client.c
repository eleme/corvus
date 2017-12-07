#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "corvus.h"
#include "client.h"
#include "mbuf.h"
#include "socket.h"
#include "logging.h"
#include "event.h"

#define CMD_MIN_LIMIT 64
#define CMD_MAX_LIMIT 512

/**********************************
 *
 * Corvus Client工作逻辑:
 * 1. 当客户端通过corvus proxy建立与corvus client的连接之后, 客户端发送redis请求.
 * 2. 这时, 因为该链接已经被注册到了epoll事件循环中监听读写事件, 所以, 当收到请求后, 会触发调用client_ready函数
 * 3. 该函数会判断事件类型, 因为是收到请求, 所以是可读事件, 所以会调用client_read函数
 * 4. client_read函数的工作流程如下:
 *          1. 先把套接字里面的请求数据循环读取到缓冲区中
 *          2. 循环读取缓冲区中的数据, 直至数据全部读取完毕. 在每次读取时, corvus client会根据redis协议进行解析
 *             用户请求, 构造出对应的command对象. 计算CRC16%16384得到slot, 并从连接池中获取corvus到对应redis实例
 *             的tcp连接. 并把这个连接注册在epoll事件循环上, 监听读写事件
 *          3. 把构造出来的command对象放到队列ready_queue的队尾(该队列被corvus server监听)
 *
 * 在从连接池中获取corvus到redis实例的连接的过程中, 如果没有连接, 就会创建corvus server连接(connection实例), 并
 * 与redis实例建立tcp连接.
 *
 **********************************
 */

// 手动触发client链接上的事件
int client_trigger_event(struct connection *client)
{
    struct mbuf *buf = client->info->current_buf;
    if (buf == NULL) {
        return CORVUS_OK;
    }

    if (buf->pos < buf->last && !client->event_triggered) {
        // 如果读取的缓冲区小于缓冲区开辟空间大小, 并且没有触发过, 则手动触发事件
        if (socket_trigger_event(client->ev->fd) == CORVUS_ERR) {
            LOG(ERROR, "%s: fail to trigger readable event", __func__);
            return CORVUS_ERR;
        }
        client->event_triggered = true;
    }
    return CORVUS_OK;
}

void client_range_clear(struct connection *client, struct command *cmd)
{
    struct mbuf **cur = &client->info->current_buf;
    struct mbuf *end = cmd->req_buf[1].buf;

    if (end == NULL) {
        *cur = NULL;
    } else if (end == *cur && end->pos >= end->last && end->refcount <= 1) {
        // end buf will be recycled so point current buf to the next
        *cur = TAILQ_NEXT(end, next);
    }
    mbuf_range_clear(client->ctx, cmd->req_buf);
}

// 获取缓冲区, 并把该缓冲区和当前连接做绑定
struct mbuf *client_get_buf(struct connection *client)
{
    // not get unprocessed buf
    struct mbuf *buf = conn_get_buf(client, false, false);
    if (client->info->current_buf == NULL) {
        client->info->current_buf = buf;
    }
    return buf;
}

// 从socket中读取从客户端发送到corvus client的请求信息, 把信息读取到缓冲区中
int client_read_socket(struct connection *client)
{
    while (true) {
        struct mbuf *buf = client_get_buf(client);  // 获取缓冲区
        int status = conn_read(client, buf);        // 从socket中把数据读取到缓冲区
        if (status != CORVUS_OK) {
            return status;
        }

        // Append time to queue after read, this is the start time of cmd.
        // Every read has a corresponding buf_time.
        // 在把数据读取到缓冲区之后, 把这个缓冲区以及它的相关信息读取到conn_info->buf_times这个队列中
        // 这一步主要是用来追踪请求从开始处理到返回的耗时
        buf_time_append(client->ctx, &client->info->buf_times, buf, get_time());

        if (buf->last < buf->end) {
            // 全部数据都读取到缓冲区, 重新注册corvus client到epoll事件循环
            if (conn_register(client) == CORVUS_ERR) {
                LOG(ERROR, "%s: fail to reregister client %d", __func__, client->fd);
                return CORVUS_ERR;
            }
            return CORVUS_OK;
        }
    }
    return CORVUS_OK;
}

// 读取客户端发送过来的请求信息
int client_read(struct connection *client, bool read_socket)
{
    struct command *cmd;
    struct mbuf *buf;
    int status = CORVUS_OK;

    if (read_socket) {
        status = client_read_socket(client);    // 读取从客户端发来的请求
        if (status == CORVUS_EOF || status == CORVUS_ERR) {
            return status;
        }
    }

    // TODO wait for commands in cmd_queue to finish
    // cmd = STAILQ_FIRST(&client->info->cmd_queue);
    // if (cmd != NULL && cmd->parse_done) {
    //     return CORVUS_OK;
    // }

    // calculate limit
    long long free_cmds = client->ctx->mstats.free_cmds;    // 空闲command对象的数量
    long long clients = ATOMIC_GET(client->ctx->stats.connected_clients);   // 连接的client数量
    free_cmds /= clients;
    int limit = free_cmds > CMD_MIN_LIMIT ? free_cmds : CMD_MIN_LIMIT;
    if (limit > CMD_MAX_LIMIT) {
        limit = CMD_MAX_LIMIT;
    }

    // 循环读取缓冲区并做响应处理, 确保所有缓冲区都读取完毕
    while (true) {
        // 获取当前请求对应的缓冲区(请求数据已经被读取到这个缓冲区中)
        buf = client->info->current_buf;
        if (buf == NULL || mbuf_read_size(buf) <= 0) {
            break;
        }

        // get current buf time before parse
        struct buf_time *t = STAILQ_FIRST(&client->info->buf_times);
        if (t == NULL) {
            LOG(ERROR, "client_read: fail to get buffer read time %d", client->fd);
            return CORVUS_ERR;
        }
        cmd = conn_get_cmd(client);     // 构造command对象
        cmd->client = client;           // 给command对象绑定client连接
        if (cmd->parse_time <= 0) {
            cmd->parse_time = t->read_time; // 设置command准备执行parse的时刻
        }
        // 解析客户端发送过来的redis请求, 并转发
        status = cmd_parse_req(cmd, buf);
        if (status == CORVUS_ERR) {
            LOG(ERROR, "client_read: command parse error");
            return CORVUS_ERR;
        }
        // pop buf times after parse
        // 解析完成后, 把和缓冲区对应的buf_time从buf_times队列中取出来, 放到free_buf_times中
        while (true) {
            struct buf_time *t = STAILQ_FIRST(&client->info->buf_times);
            if (t == NULL || buf != t->buf || buf->pos < t->pos) {
                break;
            }
            STAILQ_REMOVE_HEAD(&client->info->buf_times, next);
            buf_time_free(t);
        }

        // if buf is full point current_buf to the next buf
        // 请求还未解析完毕, 但是超过了缓冲区, 那么接着读取队列中的下一个缓冲区
        if (buf->pos >= buf->end) {
            client->info->current_buf = TAILQ_NEXT(buf, next);
            if (client->info->current_buf == NULL) {
                break;
            }
        }
        // 解析redis请求处理完毕, 并且limit<=1, 手动触发该连接上的事件
        if (cmd->parse_done && (--limit) <= 0) {
            return client_trigger_event(client);
        }
    }

    return status;
}

void client_make_iov(struct conn_info *info)
{
    struct command *cmd;

    while (!STAILQ_EMPTY(&info->cmd_queue)) {
        cmd = STAILQ_FIRST(&info->cmd_queue);
        LOG(DEBUG, "client make iov %d %d", cmd->cmd_count, cmd->cmd_done_count);
        if (cmd->cmd_count != cmd->cmd_done_count) {
            break;
        }
        STAILQ_REMOVE_HEAD(&info->cmd_queue, cmd_next);
        STAILQ_NEXT(cmd, cmd_next) = NULL;

        if (!info->quit) {
            cmd_make_iovec(cmd, &info->iov);
            cmd_stats(cmd, get_time());
        } else {
            mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        }

        if (cmd->cmd_type == CMD_QUIT) {
            info->quit = true;
        }

        cmd_free(cmd);
    }
    LOG(DEBUG, "client make iov %d", info->iov.len);
}

int client_write(struct connection *client)
{
    struct context *ctx = client->ctx;
    struct conn_info *info = client->info;

    if (!STAILQ_EMPTY(&info->cmd_queue)) {
        client_make_iov(info);
    }

    if (info->iov.len <= 0) {
        cmd_iov_reset(&info->iov);
        return CORVUS_OK;
    }

    // wait for all cmds in cmd_queue to be done
    struct command *cmd = STAILQ_FIRST(&client->info->cmd_queue);
    if (cmd != NULL && cmd->parse_done && !info->quit) {
        return CORVUS_OK;
    }

    int status = conn_write(client, 1);

    if (status == CORVUS_ERR) {
        LOG(ERROR, "client_write: client %d fail to write iov", client->fd);
        return CORVUS_ERR;
    }
    if (status == CORVUS_AGAIN) return CORVUS_OK;

    if (info->iov.cursor >= info->iov.len) {
        cmd_iov_free(&info->iov);
        if (info->quit) {
            return CORVUS_ERR;
        }
        if (event_reregister(&ctx->loop, client, E_READABLE) == CORVUS_ERR) {
            LOG(ERROR, "client_write: fail to reregister client %d", client->fd);
            return CORVUS_ERR;
        }
        if (client_trigger_event(client) == CORVUS_ERR) {
            LOG(ERROR, "client_write: fail to trigger event %d %d",
                    client->fd, client->ev->fd);
            return CORVUS_ERR;
        }
    } else if (conn_register(client) == CORVUS_ERR) {
        LOG(ERROR, "client_write: fail to reregister client %d", client->fd);
        return CORVUS_ERR;
    }

    if (client->ctx->state == CTX_BEFORE_QUIT
            || client->ctx->state == CTX_QUITTING)
    {
        return CORVUS_ERR;
    }

    LOG(DEBUG, "client write ok");
    return CORVUS_OK;
}

// corvus client链接可读和可写事件触发的处理函数
void client_ready(struct connection *self, uint32_t mask)
{
    if (self->eof) {
        if (self->info->refcount <= 0) {
            conn_free(self);
            conn_recycle(self->ctx, self);
        }
        return;
    }

    self->info->last_active = time(NULL);   // 更新链接最后活跃时间

    if (mask & E_ERROR) {
        LOG(DEBUG, "client error");
        client_eof(self);
        return;
    }
    // 可读事件, 即从客户端发送请求到corvus client的时候会触发
    if (mask & E_READABLE) {
        LOG(DEBUG, "client readable");

        int status = client_read(self, true);
        if (status == CORVUS_ERR || status == CORVUS_EOF) {
            client_eof(self);
            return;
        }
    }
    // 可写事件
    if (mask & E_WRITABLE) {
        LOG(DEBUG, "client writable");
        if (client_write(self) == CORVUS_ERR) {
            client_eof(self);
            return;
        }
    }
}

// 连接上注册的事件被触发后的处理函数
void client_event_ready(struct connection *self, uint32_t mask)
{
    struct connection *client = self->parent;
    client->event_triggered = false;

    if (client->eof) {
        if (client->info->refcount <= 0) {
            conn_free(client);
            conn_recycle(client->ctx, client);
        }
        return;
    }

    client->info->last_active = time(NULL);

    if (mask & E_READABLE) {
        if (client_read(client, false) == CORVUS_ERR) {
            client_eof(client);
            return;
        }
    }
}

// 创建一个新的从客户端到corvus的连接, 分几步进行:
// 1. 创建一个连接, 绑定之前accept创建的套接字到这个连接上
// 2. 设置socket相关参数
// 3. 创建conn_info绑定到这个连接上
// 4. 注册这个连接触发后的处理函数
// 5. 创建epoll事件, 并把该事件注册在这个连接上
// 6. 注册这个连接上的事件触发后的处理函数
struct connection *client_create(struct context *ctx, int fd)
{
    struct connection *client = conn_create(ctx);  // 初始化连接
    client->fd = fd;    // 绑定之前创建的套接字fd到这个连接上

    // 设置非阻塞I/O
    if (socket_set_nonblocking(client->fd) == -1) {
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }
    // 设置禁用Nagle算法, 减小延迟
    if (socket_set_tcpnodelay(client->fd) == -1) {
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }

    // 初始化conn_info
    client->info = conn_info_create(ctx);
    if (client->info == NULL) {
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }

    // 创建epoll监听事件, 返回事件描述符
    int evfd = socket_create_eventfd();
    // 初始化连接监听事件
    client->ev = conn_create(ctx);
    if (evfd == -1 || client->ev == NULL) {
        LOG(ERROR, "%s: fail to create event connection", __func__);
        if (evfd != -1) close(evfd);
        conn_free(client);
        conn_recycle(ctx, client);
        return NULL;
    }

    client->ev->fd = evfd;                      // 在连接上绑定监听事件与事件fd
    client->ev->ready = client_event_ready;     // 设置该连接上的事件触发后的处理函数
    client->ev->parent = client;                // 设定上级

    client->ready = client_ready;               // 设定本连接触发后的处理函数
    client->info->last_active = time(NULL);     // 创建本链接最后活跃时间
    return client;
}

void client_eof(struct connection *client)
{
    LOG(DEBUG, "client eof");
    if (client->eof == true) {
        LOG(ERROR, "client eof again");
    }

    client->eof = true;

    struct command *cmd;
    while (!STAILQ_EMPTY(&client->info->cmd_queue)) {
        cmd = STAILQ_FIRST(&client->info->cmd_queue);
        STAILQ_REMOVE_HEAD(&client->info->cmd_queue, cmd_next);
        cmd_set_stale(cmd);
    }

    ATOMIC_DEC(client->ctx->stats.connected_clients, 1);

    event_deregister(&client->ctx->loop, client);
    if (client->ev != NULL && !client->event_triggered) {
        event_deregister(&client->ctx->loop, client->ev);
    }

    // don't care response any more
    cmd_iov_clear(client->ctx, &client->info->iov);
    cmd_iov_free(&client->info->iov);

    // request may not write
    if (client->info->refcount <= 0) {
        conn_buf_free(client);
        if (!client->event_triggered) {
            conn_free(client);
            conn_recycle(client->ctx, client);
        }
    }
}

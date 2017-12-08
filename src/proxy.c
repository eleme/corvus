#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "corvus.h"
#include "proxy.h"
#include "socket.h"
#include "event.h"
#include "client.h"
#include "logging.h"

/*********************************
 *
 * Corvus Proxy逻辑:
 * 1. corvus的main_loop通过调用proxy_init函数来创建socket server, 并把fd注册到epoll事件循环中监听,
 *    corvus为了提升性能, 使用了多线程绑定监听同一个port的方式, 去除了单线程监听, 分发给多worker处理的性能瓶颈
 * 2. 当客户端请求建立连接时, corvus的事件循环捕获到请求, 触发执行proxy_ready函数, 该函数会调用proxy_accept函数.
 * 3. proxy_accept会接受来自客户端的请求, 创建一个新的socket描述符和一个新的链接, 该链接表示从客户端到corvus client
 *    的链接, 并把链接和socket描述符绑定, 最后把这个连接注册到epoll事件循环上
 * 4. 后面的流程就比较清楚了, 当客户端利用这个链接发送redis请求到corvus时, corvus会通过client来执行请求, 而不会再通过proxy
 *
 *
 * 可以发现, Corvus Proxy的主要功能就是:
 *      接受来自客户端创建链接的请求, 在没有使用corvus时, 客户端会直接与redis实例创建链接,
 *      在使用corvus后, 客户端创建的链接实际上是客户端到corvus client的链接
 *
 *********************************
 */

// proxy接受来自客户端的请求. 该函数主要包含以下几个部分:
// 1. corvus接受来自客户端的请求, 并创建新的socket描述符
// 2. 创建一个新的连接, 并把这个fd绑定在这个连接上
// 3. 让epoll事件循环监听这个连接的可读和可写事件
// 4. 让epoll事件循环监听这个连接上注册的事件, 事件类型为可读
int proxy_accept(struct connection *proxy)
{
    char ip[16];
    // 接收请求, 创建socket server的第三步，accept请求, 创建一个新的socket描述符
    int port, fd = socket_accept(proxy->fd, ip, sizeof(ip), &port);
    struct context *ctx = proxy->ctx;
    if (fd == CORVUS_ERR || fd == CORVUS_AGAIN) {
        return fd;
    }

    struct connection *client;
    // 创建客户端到corvus client的连接
    if ((client = client_create(ctx, fd)) == NULL) {
        LOG(ERROR, "proxy_accept: fail to create client");
        return CORVUS_ERR;
    }

    strcpy(client->info->addr.ip, ip);
    client->info->addr.port = port;

    // 注册这个连接到epoll事件循环上
    if (conn_register(client) == CORVUS_ERR) {
        LOG(ERROR, "proxy_accept: fail to register client");
        conn_free(client);
        conn_recycle(ctx, client);
        return CORVUS_ERR;
    }

    // 注册这个连接上的事件到epoll事件循环上, 事件类型为可读
    if (event_register(&client->ctx->loop, client->ev, E_READABLE) == CORVUS_ERR) {
        LOG(ERROR, "proxy_accept: fail to register client event");
        conn_free(client);
        conn_recycle(ctx, client);
        return CORVUS_ERR;
    }
    // 把这个连接插入到所在context的连接队列中
    TAILQ_INSERT_TAIL(&ctx->conns, client, next);

    // 增加打点
    ATOMIC_INC(ctx->stats.connected_clients, 1);
    return CORVUS_OK;
}

// 当server接收到请求之后, epoll触发的事件处理函数
void proxy_ready(struct connection *self, uint32_t mask)
{
    if (mask & E_READABLE) {    // 判断事件类型是否为可读事件
        int status;
        while (1) {
            // 接收来自客户端的请求
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

// 初始化proxy对象, 创建TCP server
int proxy_init(struct connection *proxy, struct context *ctx, char *host, int port)
{
    int fd = socket_create_server(host, port);  // 以host和port创建TCP server服务器
    if (fd == -1) {
        LOG(ERROR, "proxy_init: fail to create server fd");
        return CORVUS_ERR;
    }

    conn_init(proxy, ctx);          // 初始化proxy这个链接(为proxy申请内存空间, 绑定ctx到proxy上)
    proxy->fd = fd;                 // 绑定server的文件描述符到proxy这个链接上
    proxy->ready = proxy_ready;     // 设置处理函数
    return CORVUS_OK;
}

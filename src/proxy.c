#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "corvus.h"
#include "proxy.h"
#include "socket.h"
#include "event.h"
#include "client.h"
#include "logging.h"

// proxy接受来自客户端的请求. 该函数主要包含以下几个部分:
// 1. corvus接受来自客户端的请求, 并创建新的socket描述符
// 2. 创建一个新的连接, 并把这个fd绑定在这个连接上
// 3. 让epoll事件循环监听这个连接
// 4. 让epoll事件循环监听这个连接上注册的事件
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
    // 创建客户端到corvus的连接
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

    // 注册这个连接上的事件到epoll事件循环上
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

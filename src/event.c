#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include "event.h"
#include "logging.h"
#include "alloc.h"

// 初始化epoll事件循环, 包括创建epoll句柄, 申请事件空间, 更新event_loop对象
int event_init(struct event_loop *loop, int nevent)
{
    int epfd;
    struct epoll_event *events;

    assert(nevent > 0);

    // 创建epoll句柄(在内核申请空间), 告诉内核最多监听nevent个fd
    epfd = epoll_create(nevent);
    if (epfd < 0) {
        return -1;
    }

    // 为nevent个epoll事件申请内存空间
    events = cv_calloc(nevent, sizeof(struct epoll_event));
    if (events == NULL) {
        close(epfd);
        return -1;
    }

    memset(loop, 0, sizeof(struct event_loop));     // 把event_loop写入loop指针
    loop->epfd = epfd;
    loop->events = events;
    loop->nevent = nevent;

    return 0;
}

void event_free(struct event_loop *loop)
{
    if (loop == NULL) return;

    close(loop->epfd);
    cv_free(loop->events);
}

// 添加epoll的fd监听指定的fd, 以及监听的事件类型
int event_register(struct event_loop *loop, struct connection *c, int mask)
{
    // 结构体epoll_event有个属性events, 它是一下几个宏的集合:
    // 1. EPOLLIN:          触发该事件, 表示对应的fd上有可读数据
    // 2. EPOLLOUT:         触发该事件, 表示对应的fd上可以写数据
    // 3. EPOLLPRI：        表示对应的文件描述符有紧急的数据可读（这里应该表示有带外数据到来）；
    // 4. EPOLLERR：        表示对应的文件描述符发生错误；
    // 5. EPOLLHUP：        表示对应的文件描述符被挂断；
    // 6. EPOLLET：         将EPOLL设为边缘触发(Edge Triggered)模式，这是相对于水平触发(Level Triggered)来说的。
    // 7. EPOLLONESHOT：  只监听一次事件，当监听完这次事件之后，如果还需要继续监听这个socket的话，需要再次把这个socket加入到EPOLL队列里。
    struct epoll_event event;

    event.data.ptr = c;
    event.events = EPOLLET;
    if (mask & E_WRITABLE) event.events |= EPOLLOUT;    // 监听可写事件
    if (mask & E_READABLE) event.events |= EPOLLIN;     // 监听可读事件

    // epoll_ctl用于控制某个epoll文件描述符上的事件(注册, 修改, 删除事件)
    // 它接受的参数分别为(epoll的文件描述符, 要进行的操作, 需要监听的文件描述符, 告诉内核需要监听什么事件)
    if (epoll_ctl(loop->epfd, EPOLL_CTL_ADD, c->fd, &event) == -1) {
        LOG(ERROR, "event_register: %d %s", c->fd, strerror(errno));
        return -1;
    }
    c->registered = true;
    return 0;
}

int event_reregister(struct event_loop *loop, struct connection *c, int mask)
{
    struct epoll_event event;

    int op = mask == E_NONE ? EPOLL_CTL_DEL : EPOLL_CTL_MOD;

    event.data.ptr = c;
    event.events = EPOLLET;
    if (mask & E_WRITABLE) event.events |= EPOLLOUT;
    if (mask & E_READABLE) event.events |= EPOLLIN;

    if (epoll_ctl(loop->epfd, op, c->fd, &event) == -1) {
        LOG(ERROR, "event_reregister: %d %s", c->fd, strerror(errno));
        return -1;
    }
    return 0;
}

int event_deregister(struct event_loop *loop, struct connection *c)
{
    if (c->fd == -1) {
        return 0;
    }

    if (epoll_ctl(loop->epfd, EPOLL_CTL_DEL, c->fd, NULL) == -1) {
        LOG(ERROR, "event_deregister: %d %s", c->fd, strerror(errno));
        return -1;
    }
    c->registered = false;
    return 0;
}

// 监听epoll上面注册的fd的对应事件
// 当获取到事件后, 需要获取监听事件类型, 然后通过调用对应事件的连接的ready函数来执行对应的事件
int event_wait(struct event_loop *loop, int timeout)
{
    int i, j, nevents;

    while (true) {
        nevents = epoll_wait(loop->epfd, loop->events, loop->nevent, timeout);
        if (nevents >= 0) {
            for (i = 0; i < nevents; i++) {
                struct epoll_event *e = &loop->events[i];   // 获取事件
                struct connection *c = e->data.ptr;         // 获取对应的连接
                uint32_t mask = 0;

                bool duplicated = false;

                if (c->parent != NULL) {
                    for (j = i + 1; j < nevents; j++) {
                        int fd = ((struct connection*)(loop->events[j].data.ptr))->fd;
                        if (fd == c->parent->fd) {
                            c->parent->event_triggered = false;
                            duplicated = true;
                            break;
                        }
                    }
                }
                if (duplicated) continue;

                // 获取监听事件类别
                if (e->events & EPOLLIN) mask |= E_READABLE;
                if (e->events & EPOLLOUT) mask |= E_WRITABLE;
                if (e->events & EPOLLHUP) mask |= E_READABLE;
                if (e->events & EPOLLERR) mask |= E_ERROR;

                // 执行触发函数
                c->ready(c, mask);
            }
            return nevents;
        }

        if (errno == EINTR) {
            continue;
        }

        return -1;
    }
}

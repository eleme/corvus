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

int event_register(struct event_loop *loop, struct connection *c, int mask)
{
    struct epoll_event event;

    event.data.ptr = c;
    event.events = EPOLLET;
    if (mask & E_WRITABLE) event.events |= EPOLLOUT;
    if (mask & E_READABLE) event.events |= EPOLLIN;

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

int event_wait(struct event_loop *loop, int timeout)
{
    int i, j, nevents;

    while (true) {
        nevents = epoll_wait(loop->epfd, loop->events, loop->nevent, timeout);
        if (nevents >= 0) {
            for (i = 0; i < nevents; i++) {
                struct epoll_event *e = &loop->events[i];
                struct connection *c = e->data.ptr;
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

                if (e->events & EPOLLIN) mask |= E_READABLE;
                if (e->events & EPOLLOUT) mask |= E_WRITABLE;
                if (e->events & EPOLLHUP) mask |= E_READABLE;
                if (e->events & EPOLLERR) mask |= E_ERROR;

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

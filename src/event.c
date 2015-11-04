#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include "event.h"

struct event_loop *event_create(int nevent)
{
    int epfd;
    struct event_loop *loop;
    struct epoll_event *events;

    assert(nevent > 0);

    epfd = epoll_create(nevent);
    if (epfd < 0) {
        return NULL;
    }

    events = malloc(nevent * sizeof(struct epoll_event));
    if (events == NULL) {
        close(epfd);
        return NULL;
    }

    loop = malloc(sizeof(struct event_loop));
    if (loop == NULL) {
        free(events);
        close(epfd);
        return NULL;
    }

    loop->epfd = epfd;
    loop->events = events;
    loop->nevent = nevent;

    return loop;
}

int event_register(struct event_loop *loop, struct connection *c)
{
    int status;
    struct epoll_event event;

    event.events = (uint32_t)(EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = c;

    status = epoll_ctl(loop->epfd, EPOLL_CTL_ADD, c->fd, &event);
    if (status < 0) {
        perror("event_register: epoll_ctl");
    }
    c->registered = 1;
    return status;
}

int event_reregister(struct event_loop *loop, struct connection *c, int mask)
{
    int status;
    struct epoll_event event;

    int op = mask == E_NONE ? EPOLL_CTL_DEL : EPOLL_CTL_MOD;

    event.data.ptr = c;
    event.events = EPOLLET;
    if (mask & E_WRITABLE) event.events |= EPOLLOUT;
    if (mask & E_READABLE) event.events |= EPOLLIN;

    status = epoll_ctl(loop->epfd, op, c->fd, &event);
    if (status < 0) {
        perror("evnet_reregister: epoll_ctl");
    }
    return status;
}

int event_deregister(struct event_loop *loop, struct connection *c)
{
    int status = epoll_ctl(loop->epfd, EPOLL_CTL_DEL, c->fd, NULL);
    if (status < 0) {
        perror("event_deregister: epoll_ctl");
    }
    return status;
}

int event_wait(struct event_loop *loop, int timeout)
{
    int i, nevents;

    while (true) {
        nevents = epoll_wait(loop->epfd, loop->events, loop->nevent, timeout);
        if (nevents >= 0) {
            for (i = 0; i < nevents; i++) {
                struct epoll_event *e = &loop->events[i];
                struct connection *c = e->data.ptr;
                uint32_t mask = 0;

                if (e->events & EPOLLIN) mask |= E_READABLE;
                if (e->events & EPOLLOUT) mask |= E_WRITABLE;
                if (e->events & EPOLLHUP) mask |= E_READABLE;
                if (e->events & EPOLLERR) mask |= E_ERROR;

                c->ready(c, loop, mask);
            }
            return nevents;
        }

        if (errno == EINTR) {
            continue;
        }

        return -1;
    }
}

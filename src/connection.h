#ifndef __CONNECTION_H
#define __CONNECTION_H

#include <sys/types.h>
#include <sys/socket.h>
#include "command.h"

struct event_loop;
struct context;

enum {
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
};

struct connection {
    struct context *ctx;
    int fd;
    char *hostname;
    uint16_t port;

    struct sockaddr addr;

    int registered;

    struct cmd_tqh cmd_queue;
    struct cmd_tqh waiting_queue;
    struct cmd_tqh retry_queue;

    struct mhdr data;
    int status;
    void (*ready)(struct connection *self, struct event_loop *loop, uint32_t mask);
};

int conn_connect(struct connection *conn, int use_addr);
void conn_free(struct connection *conn);
void conn_init(struct connection *conn, struct context *ctx);
int conn_pool_resize(struct connection ***conn, int orig_size, int more);
struct connection *conn_create(struct context *ctx);
int conn_create_fd();
struct connection *conn_get_server_from_pool(struct context *ctx, struct sockaddr *addr);
struct connection *conn_get_server(struct context *ctx, uint16_t slot);
struct mbuf *conn_get_buf(struct connection *conn);

#endif /* end of include guard: __CONNECTION_H */

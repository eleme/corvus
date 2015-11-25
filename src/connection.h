#ifndef __CONNECTION_H
#define __CONNECTION_H

#include <sys/types.h>
#include <sys/socket.h>
#include "command.h"
#include "socket.h"

struct event_loop;
struct context;

enum {
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
};

struct connection {
    STAILQ_ENTRY(connection) next;
    struct context *ctx;
    int fd;

    struct address addr;

    int registered;

    struct cmd_tqh cmd_queue;
    struct cmd_tqh ready_queue;
    struct cmd_tqh waiting_queue;

    struct mhdr data;
    struct iov_data iov;

    long long send_bytes;
    long long recv_bytes;

    int status;
    void (*ready)(struct connection *self, uint32_t mask);
};

STAILQ_HEAD(conn_tqh, connection);

void conn_init(struct connection *conn, struct context *ctx);
struct connection *conn_create(struct context *ctx);
int conn_connect(struct connection *conn);
void conn_free(struct connection *conn);
void conn_buf_free(struct connection *conn);
void conn_recycle(struct context *ctx, struct connection *conn);
struct connection *conn_get_server_from_pool(struct context *ctx, struct address *addr);
struct connection *conn_get_server(struct context *ctx, uint16_t slot);
struct mbuf *conn_get_buf(struct connection *conn);
int conn_create_fd();
int conn_register(struct connection *conn);

#endif /* end of include guard: __CONNECTION_H */

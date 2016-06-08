#ifndef CONNECTION_H
#define CONNECTION_H

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
    struct context *ctx;

    TAILQ_ENTRY(connection) next;

    int fd;

    struct conn_info *info;

    struct connection *ev;
    struct connection *parent;
    bool event_triggered;
    bool eof;
    bool registered;

    void (*ready)(struct connection *self, uint32_t mask);
};

struct conn_info {
    STAILQ_ENTRY(conn_info) next;

    int refcount;

    struct address addr;
    char dsn[ADDRESS_LEN + 1];

    struct reader reader;

    int64_t last_active;

    struct buf_time_tqh buf_times;

    struct cmd_tqh cmd_queue;
    struct cmd_tqh ready_queue;
    struct cmd_tqh waiting_queue;

    // Point to current parsing buffer in data
    struct mbuf *current_buf;
    struct mhdr data;
    struct mhdr local_data;
    struct iov_data iov;

    // If `requirepass` config is setted the client should be verified.
    bool authenticated;

    bool readonly;
    bool readonly_sent;
    bool quit;

    long long send_bytes;
    long long recv_bytes;
    long long completed_commands;

    int8_t status;

    // slow log, only for server connection in worker thread
    uint32_t *slow_cmd_counts;
};

TAILQ_HEAD(conn_tqh, connection);
STAILQ_HEAD(conn_info_tqh, conn_info);

void conn_init(struct connection *conn, struct context *ctx);
struct conn_info *conn_info_create(struct context *ctx);
struct connection *conn_create(struct context *ctx);
int conn_connect(struct connection *conn);
void conn_free(struct connection *conn);
void conn_buf_free(struct connection *conn);
void conn_recycle(struct context *ctx, struct connection *conn);
struct connection *conn_get_server_from_pool(struct context *ctx, struct address *addr, bool readonly);
struct connection *conn_get_server(struct context *ctx, uint16_t slot, int access);
struct mbuf *conn_get_buf(struct connection *conn, bool unprocessed, bool local);
int conn_create_fd();
int conn_register(struct connection *conn);
void conn_add_data(struct connection *conn, uint8_t *data, int n,
        struct buf_ptr *start, struct buf_ptr *end);
int conn_write(struct connection *conn, int clear);
int conn_read(struct connection *conn, struct mbuf *buf);
struct command *conn_get_cmd(struct connection *client);

#endif /* end of include guard: CONNECTION_H */

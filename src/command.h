#ifndef __COMMAND_H
#define __COMMAND_H

#include "socket.h"
#include "parser.h"

#ifndef IOV_MAX
#define CORVUS_IOV_MAX 128
#else
#define CORVUS_IOV_MAX IOV_MAX
#endif

struct context;

enum {
    CMD_ERR,
    CMD_ERR_MOVED,
    CMD_ERR_ASK,
    CMD_ERR_CLUSTERDOWN,
};

STAILQ_HEAD(cmd_tqh, command);

struct iov_data {
    struct iovec *data;
    struct mbuf **buf_ptr;
    char buf[32];
    int cursor;
    int len;
    int max_size;
};

struct command {
    STAILQ_ENTRY(command) cmd_next;
    STAILQ_ENTRY(command) ready_next;
    STAILQ_ENTRY(command) waiting_next;
    STAILQ_ENTRY(command) sub_cmd_next;

    struct context *ctx;

    struct connection *conn_ref;
    struct command *cmd_ref;
    int refcount;

    int parse_done;

    /* redirect */
    int asking;
    int redirected;

    char *prefix;

    int stale;

    int slot;
    int cmd_type;
    int request_type;
    int reply_type;
    int keys;
    int integer_data; /* for integer response */

    int cmd_count;
    int cmd_fail;
    int cmd_done_count;
    struct cmd_tqh sub_cmds;
    struct command *parent;

    struct buf_ptr req_buf[2];
    struct buf_ptr rep_buf[2];

    char *fail_reason;

    struct connection *client;
    struct connection *server;

    /* before read, after write*/
    int64_t req_time[2];
    /* before write, after read */
    int64_t rep_time[2];
};

struct redirect_info {
    uint16_t slot;
    char addr[DSN_MAX];
    int type;
};

/* error responses */
const char *rep_err,
      *rep_parse_err,
      *rep_forward_err,
      *rep_redirect_err,
      *rep_addr_err,
      *rep_server_err,
      *rep_timeout_err;

void cmd_map_init();
void cmd_map_destroy();
struct command *cmd_create(struct context *ctx);
struct command *cmd_get(struct connection *client);
int cmd_read(struct command *cmd, struct connection *conn, int mode);
void cmd_create_iovec(struct buf_ptr ptr[], struct iov_data *iov);
void cmd_make_iovec(struct command *cmd, struct iov_data *iov);
int cmd_parse_redirect(struct command *cmd, struct redirect_info *info);
void cmd_mark_done(struct command *cmd);
void cmd_mark_fail(struct command *cmd, const char *reason);
void cmd_stats(struct command *cmd);
void cmd_set_stale(struct command *cmd);
void cmd_iov_add(struct iov_data *iov, void *buf, size_t len, struct mbuf *b);
void cmd_iov_reset(struct iov_data *iov);
void cmd_iov_clear(struct context *ctx, struct iov_data *iov);
void cmd_iov_free(struct iov_data *iov);
void cmd_free(struct command *cmd);

#endif /* end of include guard: __COMMAND_H */

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
    struct buf_ptr req_buf[2];
    struct buf_ptr rep_buf[2];

    STAILQ_ENTRY(command) cmd_next;
    STAILQ_ENTRY(command) ready_next;
    STAILQ_ENTRY(command) waiting_next;
    STAILQ_ENTRY(command) sub_cmd_next;

    struct context *ctx;
    struct connection *conn_ref;
    struct command *cmd_ref;

    struct connection *client;
    struct connection *server;
    struct command *parent;

    char *prefix;
    char *fail_reason;

    /* before read, after write*/
    int64_t parse_time;
    /* before write, after read */
    int64_t rep_time[2];

    int refcount;

    int32_t slot;
    int32_t cmd_type;
    int16_t request_type;
    int16_t reply_type;
    int keys;
    int integer_data; /* for integer response */

    int cmd_count;
    int cmd_done_count;
    struct cmd_tqh sub_cmds;

    /* redirect */
    int16_t redirected;
    bool asking;

    bool parse_done;
    bool stale;
    bool cmd_fail;
};

struct redirect_info {
    uint16_t slot;
    char addr[DSN_LEN + 1];
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
int cmd_read_rep(struct command *cmd, struct connection *server);
void cmd_create_iovec(struct buf_ptr ptr[], struct iov_data *iov);
void cmd_make_iovec(struct command *cmd, struct iov_data *iov);
int cmd_parse_req(struct command *cmd, struct mbuf *buf);
int cmd_parse_redirect(struct command *cmd, struct redirect_info *info);
void cmd_mark_done(struct command *cmd);
void cmd_mark_fail(struct command *cmd, const char *reason);
void cmd_stats(struct command *cmd, int64_t end_time);
void cmd_set_stale(struct command *cmd);
void cmd_iov_add(struct iov_data *iov, void *buf, size_t len, struct mbuf *b);
void cmd_iov_reset(struct iov_data *iov);
void cmd_iov_clear(struct context *ctx, struct iov_data *iov);
void cmd_iov_free(struct iov_data *iov);
void cmd_free(struct command *cmd);

#endif /* end of include guard: __COMMAND_H */

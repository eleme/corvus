#ifndef COMMAND_H
#define COMMAND_H

#include "socket.h"
#include "parser.h"

#ifndef IOV_MAX
#define CORVUS_IOV_MAX 128
#else
#define CORVUS_IOV_MAX IOV_MAX
#endif

#define CMD_DO(HANDLER)                           \
    /* keys command */                            \
    HANDLER(DEL,               COMPLEX,  WRITE)   \
    HANDLER(DUMP,              BASIC,    READ)    \
    HANDLER(EXISTS,            COMPLEX,  READ)    \
    HANDLER(EXPIRE,            BASIC,    READ)    \
    HANDLER(EXPIREAT,          BASIC,    WRITE)   \
    HANDLER(KEYS,              UNIMPL,   UNKNOWN) \
    HANDLER(MIGRATE,           UNIMPL,   UNKNOWN) \
    HANDLER(MOVE,              UNIMPL,   UNKNOWN) \
    HANDLER(OBJECT,            UNIMPL,   UNKNOWN) \
    HANDLER(PERSIST,           BASIC,    WRITE)   \
    HANDLER(PEXPIRE,           BASIC,    WRITE)   \
    HANDLER(PEXPIREAT,         BASIC,    WRITE)   \
    HANDLER(PTTL,              BASIC,    READ)    \
    HANDLER(RANDOMKEY,         UNIMPL,   UNKNOWN) \
    HANDLER(RENAME,            UNIMPL,   UNKNOWN) \
    HANDLER(RENAMENX,          UNIMPL,   UNKNOWN) \
    HANDLER(RESTORE,           BASIC,    WRITE)   \
    HANDLER(SCAN,              UNIMPL,   UNKNOWN) \
    HANDLER(SORT,              BASIC,    WRITE)   \
    HANDLER(TTL,               BASIC,    READ)    \
    HANDLER(TYPE,              BASIC,    READ)    \
    HANDLER(WAIT,              UNIMPL,   UNKNOWN) \
    /* strings command */                         \
    HANDLER(APPEND,            BASIC,    WRITE)   \
    HANDLER(BITCOUNT,          BASIC,    READ)    \
    HANDLER(BITOP,             UNIMPL,   UNKNOWN) \
    HANDLER(BITPOS,            BASIC,    READ)    \
    HANDLER(DECR,              BASIC,    WRITE)   \
    HANDLER(DECRBY,            BASIC,    WRITE)   \
    HANDLER(GET,               BASIC,    READ)    \
    HANDLER(GETBIT,            BASIC,    READ)    \
    HANDLER(GETRANGE,          BASIC,    READ)    \
    HANDLER(GETSET,            BASIC,    WRITE)   \
    HANDLER(INCR,              BASIC,    WRITE)   \
    HANDLER(INCRBY,            BASIC,    WRITE)   \
    HANDLER(INCRBYFLOAT,       BASIC,    WRITE)   \
    HANDLER(MGET,              COMPLEX,  READ)    \
    HANDLER(MSET,              COMPLEX,  WRITE)   \
    HANDLER(MSETNX,            UNIMPL,   UNKNOWN) \
    HANDLER(PSETEX,            BASIC,    WRITE)   \
    HANDLER(SET,               BASIC,    WRITE)   \
    HANDLER(SETBIT,            BASIC,    WRITE)   \
    HANDLER(SETEX,             BASIC,    WRITE)   \
    HANDLER(SETNX,             BASIC,    WRITE)   \
    HANDLER(SETRANGE,          BASIC,    WRITE)   \
    HANDLER(STRLEN,            BASIC,    READ)    \
    /* hashes */                                  \
    HANDLER(HDEL,              BASIC,    WRITE)   \
    HANDLER(HEXISTS,           BASIC,    READ)    \
    HANDLER(HGET,              BASIC,    READ)    \
    HANDLER(HGETALL,           BASIC,    READ)    \
    HANDLER(HINCRBY,           BASIC,    WRITE)   \
    HANDLER(HINCRBYFLOAT,      BASIC,    WRITE)   \
    HANDLER(HKEYS,             BASIC,    READ)    \
    HANDLER(HLEN,              BASIC,    READ)    \
    HANDLER(HMGET,             BASIC,    READ)    \
    HANDLER(HMSET,             BASIC,    WRITE)   \
    HANDLER(HSET,              BASIC,    WRITE)   \
    HANDLER(HSETNX,            BASIC,    WRITE)   \
    HANDLER(HSTRLEN,           BASIC,    READ)    \
    HANDLER(HVALS,             BASIC,    READ)    \
    HANDLER(HSCAN,             BASIC,    READ)    \
    /* lists */                                   \
    HANDLER(BLPOP,             UNIMPL,   UNKNOWN) \
    HANDLER(BRPOP,             UNIMPL,   UNKNOWN) \
    HANDLER(BRPOPLPUSH,        UNIMPL,   UNKNOWN) \
    HANDLER(LINDEX,            BASIC,    READ)    \
    HANDLER(LINSERT,           BASIC,    WRITE)   \
    HANDLER(LLEN,              BASIC,    READ)    \
    HANDLER(LPOP,              BASIC,    WRITE)   \
    HANDLER(LPUSH,             BASIC,    WRITE)   \
    HANDLER(LPUSHX,            BASIC,    WRITE)   \
    HANDLER(LRANGE,            BASIC,    READ)    \
    HANDLER(LREM,              BASIC,    WRITE)   \
    HANDLER(LSET,              BASIC,    WRITE)   \
    HANDLER(LTRIM,             BASIC,    WRITE)   \
    HANDLER(RPOP,              BASIC,    WRITE)   \
    HANDLER(RPOPLPUSH,         BASIC,    WRITE)   \
    HANDLER(RPUSH,             BASIC,    WRITE)   \
    HANDLER(RPUSHX,            BASIC,    WRITE)   \
    /* sets */                                    \
    HANDLER(SADD,              BASIC,    WRITE)   \
    HANDLER(SCARD,             BASIC,    READ)    \
    HANDLER(SDIFF,             BASIC,    READ)    \
    HANDLER(SDIFFSTORE,        BASIC,    WRITE)   \
    HANDLER(SINTER,            BASIC,    READ)    \
    HANDLER(SINTERSTORE,       BASIC,    WRITE)   \
    HANDLER(SISMEMBER,         BASIC,    READ)    \
    HANDLER(SMEMBERS,          BASIC,    READ)    \
    HANDLER(SMOVE,             BASIC,    WRITE)   \
    HANDLER(SPOP,              BASIC,    WRITE)   \
    HANDLER(SRANDMEMBER,       BASIC,    READ)    \
    HANDLER(SREM,              BASIC,    WRITE)   \
    HANDLER(SUNION,            BASIC,    READ)    \
    HANDLER(SUNIONSTORE,       BASIC,    WRITE)   \
    HANDLER(SSCAN,             BASIC,    READ)    \
    /* sorted sets */                             \
    HANDLER(ZADD,              BASIC,    WRITE)   \
    HANDLER(ZCARD,             BASIC,    READ)    \
    HANDLER(ZCOUNT,            BASIC,    READ)    \
    HANDLER(ZINCRBY,           BASIC,    WRITE)   \
    HANDLER(ZINTERSTORE,       BASIC,    WRITE)   \
    HANDLER(ZLEXCOUNT,         BASIC,    READ)    \
    HANDLER(ZRANGE,            BASIC,    READ)    \
    HANDLER(ZRANGEBYLEX,       BASIC,    READ)    \
    HANDLER(ZRANGEBYSCORE,     BASIC,    READ)    \
    HANDLER(ZRANK,             BASIC,    READ)    \
    HANDLER(ZREM,              BASIC,    WRITE)   \
    HANDLER(ZREMRANGEBYLEX,    BASIC,    WRITE)   \
    HANDLER(ZREMRANGEBYRANK,   BASIC,    WRITE)   \
    HANDLER(ZREMRANGEBYSCORE,  BASIC,    WRITE)   \
    HANDLER(ZREVRANGE,         BASIC,    READ)    \
    HANDLER(ZREVRANGEBYLEX,    BASIC,    READ)    \
    HANDLER(ZREVRANGEBYSCORE,  BASIC,    READ)    \
    HANDLER(ZREVRANK,          BASIC,    READ)    \
    HANDLER(ZSCORE,            BASIC,    READ)    \
    HANDLER(ZUNIONSTORE,       BASIC,    WRITE)   \
    HANDLER(ZSCAN,             BASIC,    READ)    \
    /* hyperloglog */                             \
    HANDLER(PFADD,             BASIC,    WRITE)   \
    HANDLER(PFCOUNT,           BASIC,    READ)    \
    HANDLER(PFMERGE,           BASIC,    WRITE)   \
    /* script */                                  \
    HANDLER(EVAL,              COMPLEX,  WRITE)   \
    HANDLER(EVALSHA,           UNIMPL,   UNKNOWN) \
    /* misc */                                    \
    HANDLER(AUTH,              EXTRA,    UNKNOWN) \
    HANDLER(ECHO,              UNIMPL,   UNKNOWN) \
    HANDLER(PING,              EXTRA,    UNKNOWN) \
    HANDLER(INFO,              EXTRA,    UNKNOWN) \
    HANDLER(PROXY,             EXTRA,    UNKNOWN) \
    HANDLER(QUIT,              EXTRA,    UNKNOWN) \
    HANDLER(SELECT,            UNIMPL,   UNKNOWN) \
    HANDLER(TIME,              EXTRA,    UNKNOWN)

#define CMD_DEFINE(cmd, type, access) CMD_##cmd,

enum {
    CMD_DO(CMD_DEFINE)
};

struct context;

enum {
    CMD_ERR,
    CMD_ERR_MOVED,
    CMD_ERR_ASK,
    CMD_ERR_CLUSTERDOWN,

    CMD_ACCESS_UNKNOWN,
    CMD_ACCESS_WRITE,
    CMD_ACCESS_READ,
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
    int32_t cmd_access;
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
    char addr[ADDRESS_LEN + 1];
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

#endif /* end of include guard: COMMAND_H */

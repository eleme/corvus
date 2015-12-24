#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <time.h>
#include "corvus.h"
#include "socket.h"
#include "logging.h"
#include "hash.h"
#include "slot.h"
#include "event.h"
#include "server.h"
#include "client.h"
#include "stats.h"

#define CMD_DEFINE(cmd, type) CMD_##cmd,
#define CMD_BUILD_MAP(cmd, type) {#cmd, CMD_##cmd, CMD_##type},

#define CMD_DO(HANDLER)              \
    /* keys command */               \
    HANDLER(DEL, COMPLEX)            \
    HANDLER(DUMP, BASIC)             \
    HANDLER(EXISTS, COMPLEX)         \
    HANDLER(EXPIRE, BASIC)           \
    HANDLER(EXPIREAT, BASIC)         \
    HANDLER(KEYS, UNIMPL)            \
    HANDLER(MIGRATE, UNIMPL)         \
    HANDLER(MOVE, UNIMPL)            \
    HANDLER(OBJECT, UNIMPL)          \
    HANDLER(PERSIST, BASIC)          \
    HANDLER(PEXPIRE, BASIC)          \
    HANDLER(PEXPIREAT, BASIC)        \
    HANDLER(PTTL, BASIC)             \
    HANDLER(RANDOMKEY, UNIMPL)       \
    HANDLER(RENAME, UNIMPL)          \
    HANDLER(RENAMENX, UNIMPL)        \
    HANDLER(RESTORE, BASIC)          \
    HANDLER(SCAN, UNIMPL)            \
    HANDLER(SORT, BASIC)             \
    HANDLER(TTL, BASIC)              \
    HANDLER(TYPE, BASIC)             \
    HANDLER(WAIT, UNIMPL)            \
    /* strings command */            \
    HANDLER(APPEND, BASIC)           \
    HANDLER(BITCOUNT, BASIC)         \
    HANDLER(BITOP, UNIMPL)           \
    HANDLER(BITPOS, BASIC)           \
    HANDLER(DECR, BASIC)             \
    HANDLER(DECRBY, BASIC)           \
    HANDLER(GET, BASIC)              \
    HANDLER(GETBIT, BASIC)           \
    HANDLER(GETRANGE, BASIC)         \
    HANDLER(GETSET, BASIC)           \
    HANDLER(INCR, BASIC)             \
    HANDLER(INCRBY, BASIC)           \
    HANDLER(INCRBYFLOAT, BASIC)      \
    HANDLER(MGET, COMPLEX)           \
    HANDLER(MSET, COMPLEX)           \
    HANDLER(MSETNX, UNIMPL)          \
    HANDLER(PSETEX, BASIC)           \
    HANDLER(SET, BASIC)              \
    HANDLER(SETBIT, BASIC)           \
    HANDLER(SETEX, BASIC)            \
    HANDLER(SETNX, BASIC)            \
    HANDLER(SETRANGE, BASIC)         \
    HANDLER(STRLEN, BASIC)           \
    /* hashes */                     \
    HANDLER(HDEL, BASIC)             \
    HANDLER(HEXISTS, BASIC)          \
    HANDLER(HGET, BASIC)             \
    HANDLER(HGETALL, BASIC)          \
    HANDLER(HINCRBY, BASIC)          \
    HANDLER(HINCRBYFLOAT, BASIC)     \
    HANDLER(HKEYS, BASIC)            \
    HANDLER(HLEN, BASIC)             \
    HANDLER(HMGET, BASIC)            \
    HANDLER(HMSET, BASIC)            \
    HANDLER(HSET, BASIC)             \
    HANDLER(HSETNX, BASIC)           \
    HANDLER(HSTRLEN, BASIC)          \
    HANDLER(HVALS, BASIC)            \
    HANDLER(HSCAN, BASIC)            \
    /* lists */                      \
    HANDLER(BLPOP, UNIMPL)           \
    HANDLER(BRPOP, UNIMPL)           \
    HANDLER(BRPOPLPUSH, UNIMPL)      \
    HANDLER(LINDEX, BASIC)           \
    HANDLER(LINSERT, BASIC)          \
    HANDLER(LLEN, BASIC)             \
    HANDLER(LPOP, BASIC)             \
    HANDLER(LPUSH, BASIC)            \
    HANDLER(LPUSHX, BASIC)           \
    HANDLER(LRANGE, BASIC)           \
    HANDLER(LREM, BASIC)             \
    HANDLER(LSET, BASIC)             \
    HANDLER(LTRIM, BASIC)            \
    HANDLER(RPOP, BASIC)             \
    HANDLER(RPOPLPUSH, BASIC)        \
    HANDLER(RPUSH, BASIC)            \
    HANDLER(RPUSHX, BASIC)           \
    /* sets */                       \
    HANDLER(SADD, BASIC)             \
    HANDLER(SCARD, BASIC)            \
    HANDLER(SDIFF, BASIC)            \
    HANDLER(SDIFFSTORE, BASIC)       \
    HANDLER(SINTER, BASIC)           \
    HANDLER(SINTERSTORE, BASIC)      \
    HANDLER(SISMEMBER, BASIC)        \
    HANDLER(SMEMBERS, BASIC)         \
    HANDLER(SMOVE, BASIC)            \
    HANDLER(SPOP, BASIC)             \
    HANDLER(SRANDMEMBER, BASIC)      \
    HANDLER(SREM, BASIC)             \
    HANDLER(SUNION, BASIC)           \
    HANDLER(SUNIONSTORE, BASIC)      \
    HANDLER(SSCAN, BASIC)            \
    /* sorted sets */                \
    HANDLER(ZADD, BASIC)             \
    HANDLER(ZCARD, BASIC)            \
    HANDLER(ZCOUNT, BASIC)           \
    HANDLER(ZINCRBY, BASIC)          \
    HANDLER(ZINTERSTORE, BASIC)      \
    HANDLER(ZLEXCOUNT, BASIC)        \
    HANDLER(ZRANGE, BASIC)           \
    HANDLER(ZRANGEBYLEX, BASIC)      \
    HANDLER(ZRANGEBYSCORE, BASIC)    \
    HANDLER(ZRANK, BASIC)            \
    HANDLER(ZREM, BASIC)             \
    HANDLER(ZREMRANGEBYLEX, BASIC)   \
    HANDLER(ZREMRANGEBYRANK, BASIC)  \
    HANDLER(ZREMRANGEBYSCORE, BASIC) \
    HANDLER(ZREVRANGE, BASIC)        \
    HANDLER(ZREVRANGEBYLEX, BASIC)   \
    HANDLER(ZREVRANGEBYSCORE, BASIC) \
    HANDLER(ZREVRANK, BASIC)         \
    HANDLER(ZSCORE, BASIC)           \
    HANDLER(ZUNIONSTORE, BASIC)      \
    HANDLER(ZSCAN, BASIC)            \
    /* hyperloglog */                \
    HANDLER(PFADD, BASIC)            \
    HANDLER(PFCOUNT, BASIC)          \
    HANDLER(PFMERGE, BASIC)          \
    /* script */                     \
    HANDLER(EVAL, COMPLEX)           \
    HANDLER(EVALSHA, UNIMPL)         \
    /* misc */                       \
    HANDLER(AUTH, UNIMPL)            \
    HANDLER(ECHO, UNIMPL)            \
    HANDLER(PING, PROXY)             \
    HANDLER(INFO, PROXY)             \
    HANDLER(QUIT, UNIMPL)            \
    HANDLER(SELECT, UNIMPL)

enum {
    CMD_DO(CMD_DEFINE)
};

enum {
    CMD_UNIMPL,
    CMD_BASIC,
    CMD_COMPLEX,
    CMD_PROXY,
};

struct cmd_item {
    char *cmd;
    int value;
    int type;
};

static const char *rep_err = "-ERR server error\r\n";
static const char *rep_get = "*2\r\n$3\r\nGET\r\n";
static const char *rep_set = "*3\r\n$3\r\nSET\r\n";
static const char *rep_del = "*2\r\n$3\r\nDEL\r\n";
static const char *rep_exists = "*2\r\n$6\r\nEXISTS\r\n";
static const char *rep_ok = "+OK\r\n";
static const char *rep_zero = ":0\r\n";
static const char *rep_ping = "+PONG\r\n";

static struct cmd_item cmds[] = {CMD_DO(CMD_BUILD_MAP)};
static struct dict command_map;

static inline uint8_t *cmd_get_data(struct mbuf *b, struct buf_ptr *start,
        struct buf_ptr *end, int *len)
{
    uint8_t *data;
    if (b == start->buf && b == end->buf) {
        data = start->pos;
        *len = end->pos - start->pos;
    } else if (b == start->buf) {
        data = start->pos;
        *len = b->last - start->pos;
    } else if (b == end->buf) {
        data = b->start;
        *len = end->pos - b->start;
    } else {
        data = b->start;
        *len = b->last - b->start;
    }
    return data;
}

static void cmd_init(struct context *ctx, struct command *cmd)
{
    memset(cmd, 0, sizeof(struct command));

    cmd->ctx = ctx;
    reader_init(&cmd->reader);

    STAILQ_INIT(&cmd->buf_queue);
    STAILQ_INIT(&cmd->rep_queue);

    cmd->slot = -1;
    cmd->cmd_type = -1;
    cmd->request_type = -1;

    STAILQ_INIT(&cmd->sub_cmds);
}

static void cmd_recycle(struct context *ctx, struct command *cmd)
{
    ctx->stats.cmds--;
    STAILQ_NEXT(cmd, cmd_next) = NULL;
    STAILQ_NEXT(cmd, ready_next) = NULL;
    STAILQ_NEXT(cmd, waiting_next) = NULL;
    STAILQ_NEXT(cmd, sub_cmd_next) = NULL;
    STAILQ_INSERT_HEAD(&ctx->free_cmdq, cmd, cmd_next);
    ctx->nfree_cmdq++;
}

static int cmd_in_queue(struct command *cmd, struct connection *server)
{
    struct command *ready = STAILQ_LAST(&server->ready_queue, command, ready_next),
                   *wait = STAILQ_LAST(&server->waiting_queue, command, waiting_next);

    return STAILQ_NEXT(cmd, ready_next) != NULL
        || STAILQ_NEXT(cmd, waiting_next) != NULL
        || ready == cmd
        || wait == cmd;
}

static void cmd_get_map_key(struct pos_array *pos, char *key)
{
    int i, j, h;
    struct pos *p;
    for (i = 0, h = 0; i < pos->pos_len; i++) {
        p = &pos->items[i];
        for (j = 0; j < (int)p->len; j++, h++) {
            key[h] = toupper(p->str[j]);
        }
    }
}

static int cmd_get_type(struct command *cmd, struct pos_array *pos)
{
    char key[pos->str_len + 1];
    cmd_get_map_key(pos, key);
    key[pos->str_len] = '\0';

    struct cmd_item *item = dict_get(&command_map, key);
    if (item == NULL) return -1;
    cmd->cmd_type = item->value;
    return item->type;
}

static int cmd_format_stats(char *dest, size_t n, struct stats *stats, char *latency)
{
    return snprintf(dest, n,
            "version:%s\r\n"
            "pid:%d\r\n"
            "threads:%d\r\n"
            "used_cpu_sys:%.2f\r\n"
            "used_cpu_user:%.2f\r\n"
            "connected_clients:%lld\r\n"
            "completed_commands:%lld\r\n"
            "recv_bytes:%lld\r\n"
            "send_bytes:%lld\r\n"
            "remote_latency:%.6f\r\n"
            "total_latency:%.6f\r\n"
            "last_command_latency:%s\r\n"
            "in_use_buffers:%lld\r\n"
            "free_buffers:%lld\r\n"
            "in_use_cmds:%lld\r\n"
            "free_cmds:%lld\r\n"
            "in_use_conns:%lld\r\n"
            "free_conns:%lld\r\n"
            "remotes:%s\r\n",
            VERSION, stats->pid, stats->threads,
            stats->used_cpu_sys, stats->used_cpu_user,
            stats->basic.connected_clients,
            stats->basic.completed_commands,
            stats->basic.recv_bytes, stats->basic.send_bytes,
            stats->basic.remote_latency,
            stats->basic.total_latency, latency,
            stats->basic.buffers,
            stats->free_buffers,
            stats->basic.cmds,
            stats->free_cmds,
            stats->basic.conns,
            stats->free_conns,
            stats->remote_nodes);
}

int cmd_apply_range(struct command *cmd, int type)
{
    struct mbuf *first, *last;
    struct buf_ptr *start, *end;

    switch (type) {
        case MODE_REQ:
            if (STAILQ_EMPTY(&cmd->buf_queue)) return CORVUS_ERR;
            first = STAILQ_FIRST(&cmd->buf_queue);
            last = STAILQ_LAST(&cmd->buf_queue, mbuf, next);
            start = &cmd->req_buf[0];
            end = &cmd->req_buf[1];
            break;
        case MODE_REP:
            if (STAILQ_EMPTY(&cmd->rep_queue)) return CORVUS_ERR;
            first = STAILQ_FIRST(&cmd->rep_queue);
            last = STAILQ_LAST(&cmd->rep_queue, mbuf, next);
            start = &cmd->rep_buf[0];
            end = &cmd->rep_buf[1];
            break;
        default:
            return CORVUS_ERR;
    }

    start->buf = first;
    start->pos = first->pos;
    end->buf = last;
    end->pos = last->last;
    return CORVUS_OK;
}

void cmd_add_fragment(struct command *cmd, struct pos_array *data)
{
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "$%d\r\n", data->str_len);
    mbuf_queue_copy(cmd->ctx, &cmd->buf_queue, (uint8_t*)buf, n);

    int i;
    struct pos *p;
    for (i = 0; i < data->pos_len; i++) {
        p = &data->items[i];
        mbuf_queue_copy(cmd->ctx, &cmd->buf_queue, p->str, p->len);
    }
    mbuf_queue_copy(cmd->ctx, &cmd->buf_queue, (uint8_t*)"\r\n", 2);
}

int cmd_get_slot(struct redis_data *data)
{
    if (data->elements < 2) return -1;
    struct redis_data *cmd_key = &data->element[1];

    if (cmd_key->type != REP_STRING) return -1;
    struct pos_array *pos = &cmd_key->pos;
    if (pos == NULL) return -1;

    return slot_get(pos);
}

int cmd_forward_basic(struct command *cmd)
{
    int slot;
    struct connection *server = NULL;
    struct context *ctx = cmd->ctx;

    slot = cmd->slot;
    if (slot == -1) return CORVUS_ERR;

    LOG(DEBUG, "slot %d", slot);
    server = conn_get_server(ctx, slot);
    if (server == NULL) return CORVUS_ERR;
    cmd->server = server;

    STAILQ_INSERT_TAIL(&server->ready_queue, cmd, ready_next);
    if (conn_register(server) == -1) {
        LOG(ERROR, "fail to register server %d", server->fd);
        server_eof(server);
    } else {
        LOG(DEBUG, "register server event");
    }
    return CORVUS_OK;
}

/* mget, del exists */
int cmd_forward_multikey(struct command *cmd, struct redis_data *data,
        uint8_t *prefix, size_t len)
{
    struct redis_data *key;
    if (data->elements < 2) {
        LOG(ERROR, "protocol error");
        return -1;
    }

    cmd->cmd_count = data->elements - 1;

    size_t i;
    struct command *ncmd;
    for (i = 1; i < data->elements; i++) {
        key = &data->element[i];

        ncmd = cmd_create(cmd->ctx);
        ncmd->slot = slot_get(&key->pos);
        ncmd->parent = cmd;
        ncmd->client = cmd->client;

        STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);

        mbuf_queue_copy(ncmd->ctx, &ncmd->buf_queue, prefix, len);
        cmd_add_fragment(ncmd, &key->pos);

        if (cmd_apply_range(ncmd, MODE_REQ) == CORVUS_ERR) {
            cmd_mark_fail(ncmd);
            continue;
        }

        if (cmd_forward_basic(ncmd) == CORVUS_ERR) {
            cmd_mark_fail(ncmd);
        }
    }

    return 0;
}

int cmd_forward_mset(struct command *cmd, struct redis_data *data)
{
    LOG(DEBUG, "elements %d", data->elements);
    if (data->elements < 3 || (data->elements & 1) == 0) {
        LOG(ERROR, "protocol error");
        return -1;
    }
    LOG(DEBUG, "process mset");

    cmd->cmd_count = (data->elements - 1) >> 1;

    size_t i;
    struct command *ncmd;
    struct redis_data *key, *value;
    for (i = 1; i < data->elements; i += 2) {
        key = &data->element[i];
        value = &data->element[i + 1];

        ncmd = cmd_create(cmd->ctx);
        ncmd->slot = slot_get(&key->pos);
        ncmd->parent = cmd;
        ncmd->client = cmd->client;

        STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);

        mbuf_queue_copy(ncmd->ctx, &ncmd->buf_queue, (uint8_t*)rep_set, 13);
        cmd_add_fragment(ncmd, &key->pos);
        cmd_add_fragment(ncmd, &value->pos);

        if (cmd_apply_range(ncmd, MODE_REQ) == CORVUS_ERR) {
            cmd_mark_fail(ncmd);
            continue;
        }

        if (cmd_forward_basic(ncmd) == CORVUS_ERR) {
            LOG(DEBUG, "mark fail");
            cmd_mark_fail(ncmd);
        }
    }

    return 0;
}

int cmd_forward_eval(struct command *cmd, struct redis_data *data)
{
    if (data->elements < 4) return -1;

    cmd->slot = slot_get(&data->element[3].pos);
    return cmd_forward_basic(cmd);
}

int cmd_forward_complex(struct command *cmd, struct redis_data *data)
{
    switch (cmd->cmd_type) {
        case CMD_MGET:
            if (cmd_forward_multikey(cmd, data, (uint8_t*)rep_get, 13) == -1)
                return -1;
            break;
        case CMD_MSET:
            if (cmd_forward_mset(cmd, data) == -1) return -1;
            break;
        case CMD_DEL:
            if (cmd_forward_multikey(cmd, data, (uint8_t*)rep_del, 13) == -1)
                return -1;
            break;
        case CMD_EXISTS:
            if (cmd_forward_multikey(cmd, data, (uint8_t*)rep_exists, 16) == -1)
                return -1;
            break;
        case CMD_EVAL:
            if (cmd_forward_eval(cmd, data) == -1) return -1;
            break;
        default:
            return -1;
    }
    return 0;
}

void cmd_proxy_ping(struct command *cmd)
{
    mbuf_queue_copy(cmd->ctx, &cmd->rep_queue, (uint8_t *)rep_ping, 7);
    if (cmd_apply_range(cmd, MODE_REP) == CORVUS_ERR) {
        cmd_mark_fail(cmd);
    } else {
        cmd_mark_done(cmd);
    }
}

void cmd_proxy_info(struct command *cmd)
{
    int i, n = 0, size = 0;
    struct stats stats;
    memset(&stats, 0, sizeof(stats));
    stats_get(&stats);

    char latency[16 * stats.threads];
    memset(latency, 0, sizeof(latency));

    for (i = 0; i < stats.threads; i++) {
        n = snprintf(latency + size, 16, "%.6f", stats.last_command_latency[i]);
        size += n;
        if (i < stats.threads - 1) {
            latency[size] = ',';
            size += 1;
        }
    }
    n = cmd_format_stats(NULL, 0, &stats, latency);
    char info[n + 1];
    cmd_format_stats(info, sizeof(info), &stats, latency);

    char *fmt = "$%lu\r\n";
    size = snprintf(NULL, 0, fmt, n);
    char head[size + 1];
    snprintf(head, sizeof(head), fmt, n);

    mbuf_queue_copy(cmd->ctx, &cmd->rep_queue, (uint8_t*)head, size);
    mbuf_queue_copy(cmd->ctx, &cmd->rep_queue, (uint8_t*)info, n);
    mbuf_queue_copy(cmd->ctx, &cmd->rep_queue, (uint8_t*)"\r\n", 2);
    if (cmd_apply_range(cmd, MODE_REP) == CORVUS_ERR) {
        cmd_mark_fail(cmd);
    } else {
        cmd_mark_done(cmd);
    }
}

int cmd_proxy(struct command *cmd)
{
    switch (cmd->cmd_type) {
        case CMD_PING:
            cmd_proxy_ping(cmd);
            break;
        case CMD_INFO:
            cmd_proxy_info(cmd);
            break;
        default:
            return -1;
    }
    return 0;
}

int cmd_forward(struct command *cmd, struct redis_data *data)
{
    switch (cmd->request_type) {
        case CMD_BASIC:
            cmd->slot = cmd_get_slot(data);
            if (cmd_forward_basic(cmd) == -1) return -1;
            break;
        case CMD_COMPLEX:
            if (cmd_forward_complex(cmd, data) == -1) return -1;
            break;
        case CMD_PROXY:
            if (cmd_proxy(cmd) == -1) return -1;
            break;
        case CMD_UNIMPL:
            return -1;
    }
    return 0;
}

int cmd_parse_token(struct command *cmd, struct redis_data *data)
{
    struct pos *p;
    if (data->type != REP_ARRAY) return -1;
    if (data->elements <= 0) return -1;

    struct redis_data *f1 = &data->element[0];
    if (f1->type != REP_STRING) return -1;
    p = &f1->pos.items[0];

    cmd->request_type = cmd_get_type(cmd, &f1->pos);
    if (cmd->request_type < 0) {
        LOG(WARN, "wrong command type, command %.*s", p->len, p->str);
        return -1;
    }
    return 0;
}

void cmd_gen_mget_iovec(struct command *cmd, struct iov_data *iov)
{
    const char *fmt = "*%ld\r\n";
    int n;

    n = snprintf(iov->buf, sizeof(iov->buf) - 1, fmt, cmd->keys);
    cmd_iov_add(iov, iov->buf, n);

    struct command *c;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail || (c->server != NULL
                    && STAILQ_EMPTY(&c->server->data)))
        {
            cmd_iov_add(iov, (void*)rep_err, strlen(rep_err));
            continue;
        }
        cmd_create_iovec(&c->rep_buf[0], &c->rep_buf[1], iov);
    }
}

void cmd_gen_mset_iovec(struct command *cmd, struct iov_data *iov)
{
    struct command *c;
    int fail = 0;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail || c->reply_type == REP_ERROR) {
            fail = 1;
            break;
        }
    }
    if (fail) {
        cmd_iov_add(iov, (void*)rep_err, strlen(rep_err));
        return;
    }
    cmd_iov_add(iov, (void*)rep_ok, strlen(rep_ok));
}

void cmd_gen_multikey_iovec(struct command *cmd, struct iov_data *iov)
{
    struct command *c;
    const char *fmt = ":%ld\r\n";
    int n = 0;
    int count = 0, fail = 0;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail || c->reply_type == REP_ERROR) {
            fail = 1;
            break;
        }
        if (c->integer_data == 1) {
            count++;
        }
    }

    if (fail) {
        cmd_iov_add(iov, (void*)rep_err, strlen(rep_err));
        return;
    }
    if (count) {
        n = snprintf(iov->buf, sizeof(iov->buf) - 1, fmt, count);
        cmd_iov_add(iov, iov->buf, n);
        return;
    }
    cmd_iov_add(iov, (void*)rep_zero, 4);
}

int cmd_parse_req(struct command *cmd, struct mbuf *buf)
{
    struct command *ncmd;
    struct reader *r = &cmd->reader;
    reader_feed(r, buf);

    while (buf->pos < buf->last) {
        if (parse(r, MODE_REQ) == -1) return CORVUS_ERR;

        if (reader_ready(r)) {
            ncmd = cmd_create(cmd->ctx);
            ncmd->req_time[0] = get_time();
            ncmd->parent = cmd;
            ncmd->client = cmd->client;
            ncmd->keys = r->data.elements - 1;

            memcpy(&ncmd->req_buf[0], &r->start, sizeof(r->start));
            memset(&r->start, 0, sizeof(r->start));

            memcpy(&ncmd->req_buf[1], &r->end, sizeof(r->end));
            memset(&r->end, 0, sizeof(r->end));

            STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);
            cmd->cmd_count++;

            if (cmd_parse_token(ncmd, &r->data) == -1) {
                redis_data_free(&r->data);
                cmd_mark_fail(ncmd);
                continue;
            }
            if (cmd_forward(ncmd, &r->data) == -1) {
                redis_data_free(&r->data);
                cmd_mark_fail(ncmd);
                continue;
            }
            redis_data_free(&r->data);
        }
    }
    return CORVUS_OK;
}

int cmd_parse_rep(struct command *cmd, struct mbuf *buf)
{
    struct reader *r = &cmd->reader;
    reader_feed(r, buf);

    while (buf->pos < buf->last) {
        if (parse(r, MODE_REP) == -1) {
            LOG(ERROR, "cmd_parse_rep, parse error");
            return CORVUS_ERR;
        }

        if (cmd->rep_buf[0].buf == NULL && r->start.buf != NULL) {
            memcpy(&cmd->rep_buf[0], &r->start, sizeof(r->start));
            memset(&r->start, 0, sizeof(r->start));
        }

        if (reader_ready(r)) {
            cmd->reply_type = r->redis_data_type;
            r->redis_data_type = REP_UNKNOWN;
            if (cmd->reply_type == REP_INTEGER) {
                cmd->integer_data = r->integer;
                r->integer = 0;
            }

            memcpy(&cmd->rep_buf[1], &r->end, sizeof(r->end));
            memset(&r->end, 0, sizeof(r->end));
            redis_data_free(&r->data);
            break;
        }
    }
    return CORVUS_OK;
}

void cmd_mark(struct command *cmd, int fail)
{
    struct command *parent = cmd->parent, *root = NULL;
    if (fail) cmd->cmd_fail = 1;
    while (parent != NULL) {
        parent->cmd_done_count++;
        if (parent->cmd_count == parent->cmd_done_count) {
            root = parent;
            parent = parent->parent;
            continue;
        }
        break;
    }
    if (parent == NULL && root != NULL
            && root->cmd_count == root->cmd_done_count)
    {
        if (conn_register(root->client) == -1) {
            LOG(INFO, "fail to reregister client %d", root->client->fd);
            client_eof(root->client);
        }
    }
}

void cmd_map_init()
{
    dict_init(&command_map);

    size_t i, cmds_len = sizeof(cmds) / sizeof(struct cmd_item);

    for (i = 0; i < cmds_len; i++) {
        dict_set(&command_map, cmds[i].cmd, &cmds[i]);
    }
}

void cmd_map_destroy()
{
    dict_free(&command_map);
}

struct command *cmd_create(struct context *ctx)
{
    struct command *cmd;
    if (!STAILQ_EMPTY(&ctx->free_cmdq)) {
        LOG(DEBUG, "cmd get cache");
        cmd = STAILQ_FIRST(&ctx->free_cmdq);
        STAILQ_REMOVE_HEAD(&ctx->free_cmdq, cmd_next);
        ctx->nfree_cmdq--;
        STAILQ_NEXT(cmd, cmd_next) = NULL;
    } else {
        cmd = malloc(sizeof(struct command));
    }
    cmd_init(ctx, cmd);
    ctx->stats.cmds++;
    return cmd;
}

struct command *cmd_get_lastest(struct context *ctx, struct cmd_tqh *q)
{
    struct command *cmd;
    int new_cmd = 0;

    if (!STAILQ_EMPTY(q)) {
        cmd = STAILQ_LAST(q, command, cmd_next);
        if (cmd->parse_done) {
            new_cmd = 1;
        }
    } else {
        new_cmd = 1;
    }

    if (new_cmd) {
        cmd = cmd_create(ctx);
        STAILQ_INSERT_TAIL(q, cmd, cmd_next);
    }
    return cmd;
}

int cmd_read_reply(struct command *cmd, struct connection *server)
{
    int n, rsize;
    struct mbuf *buf;

    while (1) {
        buf = conn_get_buf(server);
        rsize = mbuf_read_size(buf);

        if (rsize > 0) {
            if (cmd_parse_rep(cmd, buf) == -1) return CORVUS_ERR;
            if (reader_ready(&cmd->reader)) return CORVUS_OK;
            continue;
        }

        n = socket_read(server->fd, buf);
        if (n == 0) return CORVUS_EOF;
        if (n == CORVUS_ERR) return CORVUS_ERR;
        if (n == CORVUS_AGAIN) return CORVUS_AGAIN;

        cmd->ctx->stats.recv_bytes += n;
        server->recv_bytes += n;
    }

    return CORVUS_OK;
}

int cmd_read_request(struct command *cmd, int fd)
{
    int n;
    struct mbuf *buf;

    while (1) {
        do {
            buf = mbuf_queue_get(cmd->ctx, &cmd->buf_queue);

            n = socket_read(fd, buf);
            if (n == 0) return CORVUS_EOF;
            if (n == CORVUS_ERR) return CORVUS_ERR;
            if (n == CORVUS_AGAIN) return CORVUS_AGAIN;

            cmd->ctx->stats.recv_bytes += n;

            if (cmd_parse_req(cmd, buf) == CORVUS_ERR) {
                return CORVUS_ERR;
            }
        } while (!reader_ready(&cmd->reader));
    }

    return CORVUS_OK;
}

void cmd_create_iovec(struct buf_ptr *start, struct buf_ptr *end, struct iov_data *iov)
{
    uint8_t *data;
    int len;
    struct mbuf *b = start->buf;

    while (b != NULL) {
        data = cmd_get_data(b, start, end, &len);
        cmd_iov_add(iov, (void*)data, len);

        if (b == end->buf) break;
        b = STAILQ_NEXT(b, next);
    }
}

/* cmd should be done */
void cmd_make_iovec(struct command *cmd, struct iov_data *iov)
{
    LOG(DEBUG, "cmd count %d", cmd->cmd_count);
    struct command *c;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail || (c->server != NULL
                    && STAILQ_EMPTY(&c->server->data)))
        {
            cmd_iov_add(iov, (void*)rep_err, strlen(rep_err));
            continue;
        }
        switch (c->cmd_type) {
            case CMD_MGET:
                cmd_gen_mget_iovec(c, iov);
                break;
            case CMD_MSET:
                cmd_gen_mset_iovec(c, iov);
                break;
            case CMD_DEL:
            case CMD_EXISTS:
                cmd_gen_multikey_iovec(c, iov);
                break;
            default:
                cmd_create_iovec(&c->rep_buf[0], &c->rep_buf[1], iov);
                break;
        }
    }
}

int cmd_parse_redirect(struct command *cmd, struct redirect_info *info)
{
    int n = 31;
    char err[n + 1];
    int len = 0, size;

    struct buf_ptr *start = &cmd->rep_buf[0];
    struct buf_ptr *end = &cmd->rep_buf[1];

    uint8_t *p;

    struct mbuf *b = start->buf;
    while (b != NULL) {
        p = cmd_get_data(b, start, end, &size);

        size = MIN(size, n - len);
        memcpy(err + len, p, size);
        len += size;
        if (len >= n) break;

        if (b == end->buf) break;
        b = STAILQ_NEXT(b, next);
    }
    if (len <= n) {
        err[len] = '\0';
    } else {
        err[n] = '\0';
    }

    char name[8];
    LOG(DEBUG, "%s", err);

    int r = 0;
    if (strncmp(err, "-MOVED", 5) == 0) {
        /* -MOVED 16383 127.0.0.1:8001 */
        info->type = CMD_ERR_MOVED;
        r = sscanf(err, "%s%d%s", name, (int*)&info->slot, info->addr);
        if (r != 3) return CORVUS_ERR;
    } else if (strncmp(err, "-ASK", 3) == 0) {
        /* -ASK 16383 127.0.0.1:8001 */
        info->type = CMD_ERR_ASK;
        r = sscanf(err, "%s%d%s", name, (int*)&info->slot, info->addr);
        if (r != 3) return CORVUS_ERR;
    }
    return CORVUS_OK;
}

void cmd_mark_done(struct command *cmd)
{
    cmd_mark(cmd, 0);
}

/* rep data referenced in server should be freed before mark fail */
void cmd_mark_fail(struct command *cmd)
{
    memset(cmd->rep_buf, 0, sizeof(cmd->rep_buf));
    cmd_mark(cmd, 1);
}

void cmd_stats(struct command *cmd)
{
    struct context *ctx = cmd->ctx;
    struct command *c, *sub_cmd;

    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        ctx->stats.completed_commands++;
        ctx->stats.total_latency += cmd->req_time[1] - c->req_time[0];
        if (STAILQ_NEXT(c, sub_cmd_next) == NULL) {
            ctx->last_command_latency = cmd->req_time[1] - c->req_time[0];
        }

        if (!STAILQ_EMPTY(&c->sub_cmds)) {
            STAILQ_FOREACH(sub_cmd, &c->sub_cmds, sub_cmd_next) {
                ctx->stats.remote_latency += sub_cmd->rep_time[1] - sub_cmd->rep_time[0];
            }
        } else {
            ctx->stats.remote_latency += c->rep_time[1] - c->rep_time[0];
        }
    }
}

void cmd_set_stale(struct command *cmd)
{
    if (STAILQ_EMPTY(&cmd->sub_cmds)) {
        if (cmd->server != NULL && cmd_in_queue(cmd, cmd->server)) {
            LOG(WARN, "command set stale");
            cmd->stale = 1;
        } else {
            cmd_free(cmd);
        }
    } else {
        struct command *c;
        while (!STAILQ_EMPTY(&cmd->sub_cmds)) {
            c = STAILQ_FIRST(&cmd->sub_cmds);
            STAILQ_REMOVE_HEAD(&cmd->sub_cmds, sub_cmd_next);
            STAILQ_NEXT(c, sub_cmd_next) = NULL;
            cmd_set_stale(c);
        }
        cmd_free(cmd);
    }
}

void cmd_iov_add(struct iov_data *iov, void *buf, size_t len)
{
    if (iov->cursor >= CORVUS_IOV_MAX) {
        iov->len -= iov->cursor;
        memmove(iov->data, iov->data + iov->cursor, iov->len * sizeof(struct iovec));
        iov->cursor = 0;
    }

    if (iov->max_size <= iov->len) {
        iov->max_size *= 2;
        if (iov->max_size == 0) iov->max_size = CORVUS_IOV_MAX;
        iov->data = realloc(iov->data, sizeof(struct iovec) * iov->max_size);
    }

    iov->data[iov->len].iov_base = buf;
    iov->data[iov->len].iov_len = len;
    iov->len++;
}

int cmd_iov_write(struct context *ctx, struct iov_data *iov, int fd)
{
    ssize_t remain = 0, status, bytes = 0, count = 0;

    int i, n = 0;
    struct iovec *vec = iov->data + iov->cursor;

    while (n < iov->len - iov->cursor) {
        if (n >= CORVUS_IOV_MAX || bytes >= SSIZE_MAX) break;
        bytes += vec[n++].iov_len;
    }

    status = socket_write(fd, vec, n);
    if (status == CORVUS_AGAIN || status == CORVUS_ERR) return status;

    ctx->stats.send_bytes += status;

    if (status < bytes) {
        for (i = 0; i < n; i++) {
            count += vec[i].iov_len;
            if (count > status) {
                remain = vec[i].iov_len - (count - status);
                vec[i].iov_base = (char*)vec[i].iov_base + remain;
                vec[i].iov_len -= remain;
                break;
            }
        }
        iov->cursor += i;
    } else {
        iov->cursor += n;
    }

    return status;
}

void cmd_iov_free(struct iov_data *iov)
{
    if (iov->data != NULL) free(iov->data);
    iov->data = NULL;
    iov->max_size = 0;
    iov->cursor = 0;
    iov->len = 0;
}

void cmd_free_reply(struct command *cmd)
{
    if (cmd->server == NULL) return;
    if (cmd->rep_buf[0].buf == NULL) return;

    if (cmd->server != NULL && STAILQ_EMPTY(&cmd->server->data)) {
        memset(&cmd->rep_buf, 0, sizeof(cmd->rep_buf));
        return;
    }

    struct mbuf *b, *buf;
    b = cmd->rep_buf[0].buf;
    while (b != NULL) {
        if (b == cmd->rep_buf[0].buf && b == cmd->rep_buf[1].buf) {
            mbuf_dec_ref_by(b, 2);
        } else if (b == cmd->rep_buf[0].buf || b == cmd->rep_buf[1].buf) {
            mbuf_dec_ref(b);
        }

        buf = STAILQ_NEXT(b, next);
        if (b->refcount <= 0) {
            STAILQ_REMOVE(&cmd->server->data, b, mbuf, next);
            STAILQ_NEXT(b, next) = NULL;
            mbuf_recycle(cmd->ctx, b);
        }
        if (b == cmd->rep_buf[1].buf) break;
        b = buf;
    }
    memset(&cmd->rep_buf, 0, sizeof(cmd->rep_buf));
}

void cmd_free(struct command *cmd)
{
    struct mbuf *buf;
    struct command *c;
    struct context *ctx = cmd->ctx;

    cmd_iov_free(&cmd->iov);

    reader_free(&cmd->reader);
    reader_init(&cmd->reader);

    cmd_free_reply(cmd);

    while (!STAILQ_EMPTY(&cmd->rep_queue)) {
        buf = STAILQ_FIRST(&cmd->rep_queue);
        STAILQ_REMOVE_HEAD(&cmd->rep_queue, next);
        mbuf_recycle(ctx, buf);
    }

    while (!STAILQ_EMPTY(&cmd->buf_queue)) {
        buf = STAILQ_FIRST(&cmd->buf_queue);
        STAILQ_REMOVE_HEAD(&cmd->buf_queue, next);
        mbuf_recycle(ctx, buf);
    }

    while (!STAILQ_EMPTY(&cmd->sub_cmds)) {
        c = STAILQ_FIRST(&cmd->sub_cmds);
        STAILQ_REMOVE_HEAD(&cmd->sub_cmds, sub_cmd_next);
        cmd_free(c);
    }
    cmd->client = NULL;
    cmd->server = NULL;
    cmd_recycle(ctx, cmd);
}

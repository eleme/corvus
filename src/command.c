#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include "command.h"
#include "socket.h"
#include "corvus.h"
#include "logging.h"
#include "hash.h"
#include "slot.h"
#include "event.h"
#include "server.h"

#define CMD_DIVIDER 960

#define CMD_COPY_RANGE(start_buf, end_buf, reader) \
do {                                               \
    int bytes = sizeof(struct buf_ptr);            \
    memcpy(start_buf, &reader->start, bytes);      \
    memcpy(end_buf, &reader->end, bytes);          \
    memset(&reader->start, 0, bytes);              \
    memset(&reader->end, 0, bytes);                \
} while (0)

#define CMD_DEFINE(cmd, type) CMD_##cmd,
#define CMD_BUILD_MAP(cmd, type) {#cmd, CMD_##cmd, CMD_##type},

#define CMD_DO(HANDLER)              \
    /* keys command */               \
    HANDLER(DEL, BASIC)              \
    HANDLER(DUMP, BASIC)             \
    HANDLER(EXISTS, BASIC)           \
    HANDLER(EXPIRE, BASIC)           \
    HANDLER(EXPIREAT, BASIC)         \
    HANDLER(PEXPIRE, BASIC)          \
    HANDLER(PEXPIREAT, BASIC)        \
    HANDLER(PERSIST, BASIC)          \
    HANDLER(PTTL, BASIC)             \
    HANDLER(SORT, BASIC)             \
    HANDLER(TTL, BASIC)              \
    HANDLER(TYPE, BASIC)             \
    /* strings command */            \
    HANDLER(APPEND, BASIC)           \
    HANDLER(BITCOUNT, BASIC)         \
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
    HANDLER(MGET, MULTI_KEY)         \
    HANDLER(MSET, MULTI_KEY)         \
    HANDLER(PSETEX, BASIC)           \
    HANDLER(RESTORE, BASIC)          \
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
    HANDLER(HVALS, BASIC)            \
    HANDLER(HSCAN, BASIC)            \
    /* lists */                      \
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
    HANDLER(SDIFF, UNIMPL)           \
    HANDLER(SDIFFSTORE, UNIMPL)      \
    HANDLER(SINTER, UNIMPL)          \
    HANDLER(SINTERSTORE, UNIMPL)     \
    HANDLER(SISMEMBER, BASIC)        \
    HANDLER(SMEMBERS, BASIC)         \
    HANDLER(SMOVE, UNIMPL)           \
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
    HANDLER(ZINTERSTORE, UNIMPL)     \
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
    HANDLER(ZREVRANGEBYSCORE, BASIC) \
    HANDLER(ZREVRANK, BASIC)         \
    HANDLER(ZSCORE, BASIC)           \
    HANDLER(ZUNIONSTORE, UNIMPL)     \
    HANDLER(ZSCAN, BASIC)            \
    /* hyperloglog */                \
    HANDLER(PFADD, BASIC)            \
    HANDLER(PFCOUNT, BASIC)          \
    HANDLER(PFMERGE, BASIC)          \
    /* script */                     \
    HANDLER(EVAL, BASIC)             \
    HANDLER(EVALSHA, BASIC)          \
    /* misc */                       \
    HANDLER(PING, UNIMPL)            \
    HANDLER(INFO, UNIMPL)            \
    HANDLER(QUIT, UNIMPL)            \
    HANDLER(AUTH, UNIMPL)            \
    HANDLER(SELECT, UNIMPL)

enum {
    CMD_DO(CMD_DEFINE)
};

enum {
    CMD_UNIMPL,
    CMD_BASIC,
    CMD_MULTI_KEY,
    CMD_PROXY,
};

struct cmd_item {
    char *cmd;
    int value;
    int type;
};

static const char *cmd_err = "-ERR protocol error\r\n";
static const char *rep_err = "-ERR server error\r\n";
static const char *rep_get = "*2\r\n$3\r\nGET\r\n";
static const char *rep_set = "*3\r\n$3\r\nSET\r\n";
static const char *rep_ok = "+OK\r\n";

void cmd_init(struct context *ctx, struct command *cmd)
{
    cmd->parse_done = 0;
    cmd->cmd_done = 0;
    cmd->cmd_done_count = 0;
    cmd->cmd_fail = 0;
    cmd->cmd_fail_count = 0;
    cmd->ctx = ctx;
    mbuf_queue_init(&cmd->buf_queue);
    mbuf_queue_init(&cmd->rep_queue);

    cmd->reader = NULL;
    cmd->cmd_count = 0;
    cmd->cmd_max_size = 0;
    cmd->parent = NULL;
    cmd->req_data = NULL;
    cmd->rep_data = NULL;
    cmd->client = NULL;
    cmd->slot = -1;

    STAILQ_INIT(&cmd->sub_cmds);
}

static struct command *cmd_create(struct context *ctx)
{
    struct command *cmd = malloc(sizeof(struct command));
    cmd_init(ctx, cmd);
    return cmd;
}

static struct mbuf *cmd_get_buf(struct command *cmd)
{
    struct mbuf *buf;
    buf = mbuf_queue_top(cmd->ctx, &cmd->buf_queue);
    if (mbuf_full(buf)) {
        buf = mbuf_get(cmd->ctx);
        mbuf_queue_insert(&cmd->buf_queue, buf);
    }
    return buf;
}

static int cmd_get_type(struct command *cmd, struct pos_array *pos)
{
    uint32_t hash = lookup3_hash(pos);
    int idx = hash % CMD_DIVIDER;

    struct cmd_info *item = &command_map[idx];
    if (item->value == -1 || item->hash != hash) return -1;
    cmd->cmd_type = item->value;
    return item->type;
}

static int cmd_get_slot(struct command *cmd)
{
    uint16_t slot;
    struct redis_data *data = cmd->req_data;
    struct redis_data *cmd_key = data->element[1];
    struct pos_array *pos = cmd_key->pos;

    slot = slot_get(pos);
    cmd->slot = slot;
    return slot;
}

static void add_fragment(struct command *cmd, struct pos_array *data)
{
    const char *fmt = "$%ld\r\n";
    int n = snprintf(NULL, 0, fmt, data->str_len);
    char buf[n + 1];
    snprintf(buf, sizeof(buf), fmt, data->str_len);

    mbuf_queue_copy(cmd->ctx, &cmd->buf_queue, (uint8_t*)buf, n);

    int i;
    struct pos *p;
    for (i = 0; i < data->pos_len; i++) {
        p = &data->items[i];
        mbuf_queue_copy(cmd->ctx, &cmd->buf_queue, p->str, p->len);
    }
    mbuf_queue_copy(cmd->ctx, &cmd->buf_queue, (uint8_t*)"\r\n", 2);
}

static int cmd_forward_basic(struct command *cmd)
{
    int slot;
    struct connection *server = NULL;
    struct context *ctx = cmd->ctx;

    slot = cmd->slot != -1 ? cmd->slot : cmd_get_slot(cmd);
    if (slot == -1) return -1;

    LOG(DEBUG, "slot %d", slot);
    server = conn_get_server(ctx, slot);
    if (server == NULL) return -1;

    STAILQ_INSERT_TAIL(&server->ready_queue, cmd, ready_next);
    switch (server->registered) {
        case 1: event_reregister(ctx->loop, server, E_WRITABLE | E_READABLE); break;
        case 0: event_register(ctx->loop, server); break;
    }
    LOG(DEBUG, "register server event");

    return 0;
}

static void cmd_apply_range(struct command *cmd)
{
    struct mbuf *first, *last;
    first = STAILQ_FIRST(&cmd->buf_queue);
    last = STAILQ_LAST(&cmd->buf_queue, mbuf, next);
    cmd->req_buf[0].buf = first;
    cmd->req_buf[0].pos = first->pos;
    cmd->req_buf[1].buf = last;
    cmd->req_buf[1].pos = last->last;
}

static int cmd_forward_mget(struct command *cmd)
{
    struct redis_data *key;
    struct redis_data *data = cmd->req_data;
    if (data->elements < 2) {
        LOG(ERROR, "protocol error");
        return -1;
    }

    size_t i;
    uint16_t slot;
    struct command *ncmd;
    for (i = 1; i < data->elements; i++) {
        key = data->element[i];
        slot = slot_get(key->pos);

        ncmd = cmd_create(cmd->ctx);
        ncmd->slot = slot;
        ncmd->parent = cmd;
        ncmd->client = cmd->client;

        mbuf_queue_copy(ncmd->ctx, &ncmd->buf_queue, (uint8_t*)rep_get, strlen(rep_get));
        add_fragment(ncmd, key->pos);
        cmd_apply_range(ncmd);

        STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);
        cmd->cmd_count++;

        if (cmd_forward_basic(ncmd) == -1) {
            cmd_mark_fail(ncmd);
        }
    }

    return 0;
}

static int cmd_forward_mset(struct command *cmd)
{
    struct redis_data *data = cmd->req_data;
    LOG(DEBUG, "elements %d", data->elements);
    if (data->elements < 3 || (data->elements & 1) == 0) {
        LOG(ERROR, "protocol error");
        return -1;
    }
    LOG(DEBUG, "process mset");

    size_t i;
    uint16_t slot;
    struct command *ncmd;
    struct redis_data *key, *value;
    for (i = 1; i < data->elements; i += 2) {
        key = data->element[i];
        value = data->element[i + 1];
        slot = slot_get(key->pos);

        ncmd = cmd_create(cmd->ctx);
        ncmd->slot = slot;
        ncmd->parent = cmd;
        ncmd->client = cmd->client;

        mbuf_queue_copy(ncmd->ctx, &ncmd->buf_queue, (uint8_t*)rep_set, strlen(rep_set));
        add_fragment(ncmd, key->pos);
        add_fragment(ncmd, value->pos);
        cmd_apply_range(ncmd);

        STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);
        cmd->cmd_count++;

        if (cmd_forward_basic(ncmd) == -1) {
            LOG(DEBUG, "mark fail");
            cmd_mark_fail(ncmd);
        }
    }

    return 0;
}

static int cmd_forward_multi_key(struct command *cmd)
{
    switch (cmd->cmd_type) {
        case CMD_MGET:
            if (cmd_forward_mget(cmd) == -1) return -1;
            break;
        case CMD_MSET:
            cmd_forward_mset(cmd);
            break;
        default:
            return -1;
    }
    return 0;
}

int cmd_forward(struct command *cmd)
{
    switch (cmd->request_type) {
        case CMD_BASIC:
            if (cmd_forward_basic(cmd) == -1) return -1;
            break;
        case CMD_MULTI_KEY:
            if (cmd_forward_multi_key(cmd) == -1) return -1;
            break;
        case CMD_PROXY:
            break;
        case CMD_UNIMPL:
            break;
    }
    return 0;
}

static int cmd_parse_token(struct command *cmd)
{
    assert(cmd->req_data != NULL);

    struct pos *p;
    struct redis_data *data = cmd->req_data;

    assert(data->type == DATA_TYPE_ARRAY);
    assert(data->elements > 0);

    struct redis_data *f1 = data->element[0];
    assert(f1->type == DATA_TYPE_STRING);
    p = &f1->pos->items[0];
    LOG(DEBUG, "process command %.*s", p->len, p->str);

    cmd->request_type = cmd_get_type(cmd, f1->pos);
    if (cmd->request_type < 0) {
        LOG(WARN, "wrong command type, command %.*s", p->len, p->str);
        return -1;
    }
    return 0;
}

static int get_buf_count(struct buf_ptr *start, struct buf_ptr *end)
{
    int n = 0;
    struct mbuf *b = start->buf;
    while (b != end->buf && b != NULL) {
        n++;
        b = b->next.stqe_next;
    }
    if (b == end->buf) n++;
    return n;
}

static void iov_add(struct iov_data *iov, void *buf, size_t len)
{
    iov->max_size++;
    iov->data = realloc(iov->data, sizeof(struct iovec) * iov->max_size);
    iov->data[iov->len].iov_base = buf;
    iov->data[iov->len].iov_len = len;
    iov->len++;
}

struct redirect_info *cmd_parse_redirect(struct command *cmd)
{
    int i;
    struct pos_array *pos = cmd->rep_data->pos;
    struct pos *p;
    int remain, len = 0, max_len = 1024;
    char *err = calloc(max_len, sizeof(char));
    LOG(DEBUG, "parse redirect");

    for (i = 0; i < pos->pos_len; i++) {
        p = &pos->items[i];
        remain = max_len - len;
        if (remain < (int)p->len) {
            max_len += p->len - remain;
            err = realloc(err, sizeof(char) * max_len);
        }
        memcpy(err + len, p->str, p->len);
        len += p->len;
    }

    char name[6];
    LOG(DEBUG, "%.*s", len, err);

    struct redirect_info *info = NULL;
    if (strncmp(err, "MOVED", 5) == 0) {
        info = malloc(sizeof(struct redirect_info));
        /* MOVED 16383 127.0.0.1:8001 */
        info->addr = malloc(sizeof(char) * (len - 11));
        info->type = CMD_ERR_MOVED;
        sscanf(err, "%s%d%s", name, (int*)&info->slot, info->addr);
    } else if (strncmp(err, "ASK", 3) == 0) {
        info = malloc(sizeof(struct redirect_info));
        /* ASK 16383 127.0.0.1:8001 */
        info->addr = malloc(sizeof(char) * (len - 9));
        info->type = CMD_ERR_ASK;
        sscanf(err, "%s%d%s", name, (int*)&info->slot, info->addr);
    }
    free(err);
    return info;
}

void cmd_mark_done(struct command *cmd)
{
    int done;
    struct command *parent = cmd->parent;
    cmd->cmd_done = 1;
    while (parent != NULL) {
        parent->cmd_done_count++;
        done = parent->cmd_done_count + parent->cmd_fail_count;
        if (parent->cmd_count == done) {
            parent->cmd_done = 1;
            parent = parent->parent;
        } else {
            break;
        }
    }
}

void cmd_mark_fail(struct command *cmd)
{
    int fail;
    struct command *parent = cmd->parent;
    cmd->cmd_fail = 1;
    while (parent != NULL) {
        parent->cmd_done_count++;
        fail = parent->cmd_fail_count + parent->cmd_done_count;
        if (parent->cmd_count == fail) {
            parent = parent->parent;
            continue;
        }
        break;
    }
}

void cmd_create_iovec(struct buf_ptr *start, struct buf_ptr *end, struct iov_data *iov)
{
    struct mbuf *b;
    int n = get_buf_count(start, end);
    if (n <= 0) return;
    LOG(DEBUG, "buf count %d", n);

    int remain = iov->max_size - iov->len;
    if (n > remain) {
        iov->max_size += n - remain;
        iov->data = realloc(iov->data,
                sizeof(struct iovec) * iov->max_size);
    }

    struct iovec *d = iov->data;
    for (b = start->buf; b != end->buf && b != NULL;
            b = b->next.stqe_next, iov->len++)
    {
        if (b == start->buf) {
            d[iov->len].iov_base = start->pos;
            d[iov->len].iov_len = start->buf->last - start->pos;
            mbuf_dec_ref(b);
        } else {
            d[iov->len].iov_base = b->start;
            d[iov->len].iov_len = b->last - b->start;
        }
    }
    if (b == start->buf) {
        d[iov->len].iov_base = start->pos;
        d[iov->len].iov_len = end->pos - start->pos;
        mbuf_dec_ref_by(b, 2);
        iov->len++;
    } else if (b == end->buf) {
        d[iov->len].iov_base = b->start;
        d[iov->len].iov_len = end->pos - b->start;
        mbuf_dec_ref(b);
        iov->len++;
    }
}

void cmd_gen_mget_iovec(struct command *cmd, struct iov_data *iov)
{
    const char *fmt = "*%ld\r\n";
    int keys = cmd->req_data->elements - 1;
    int n = snprintf(NULL, 0, fmt, keys);
    char *b = malloc(sizeof(char) * (n + 1));
    snprintf(b, sizeof(b), fmt, keys);

    iov_add(iov, (void*)b, n);

    struct command *c;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        cmd_create_iovec(&c->rep_buf[0], &c->rep_buf[1], iov);
    }
}

void cmd_gen_mset_iovec(struct command *cmd, struct iov_data *iov)
{
    struct command *c;
    struct redis_data *rep;
    int fail = 0;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        rep = c->rep_data;
        if (rep->pos == NULL) {
            fail = 1;
            break;
        }
        if (pos_array_compare(rep->pos, "OK", 2) != 0) {
            fail = 1;
            break;
        }
    }
    if (fail) {
        iov_add(iov, (void*)rep_err, strlen(rep_err));
    } else {
        iov_add(iov, (void*)rep_ok, strlen(rep_ok));
    }
}

/* cmd should be done */
void cmd_make_iovec(struct command *cmd, struct iov_data *iov)
{
    LOG(DEBUG, "cmd count %d", cmd->cmd_count);
    struct command *c;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail) {
            iov_add(iov, (void*)cmd_err, strlen(cmd_err));
            continue;
        }
        switch (c->cmd_type) {
            case CMD_MGET:
                cmd_gen_mget_iovec(c, iov);
                break;
            case CMD_MSET:
                cmd_gen_mset_iovec(c, iov);
                break;
            default:
                cmd_create_iovec(&c->rep_buf[0], &c->rep_buf[1], iov);
                break;
        }
    }
}

struct mbuf *cmd_get_reply_buf(struct command *cmd)
{
    struct mbuf *buf;
    buf = mbuf_queue_top(cmd->ctx, &cmd->rep_queue);
    if (mbuf_full(buf)) {
        buf = mbuf_get(cmd->ctx);
        mbuf_queue_insert(&cmd->rep_queue, buf);
    }
    return buf;
}

void cmd_queue_init(struct cmd_tqh *cmd_queue)
{
    STAILQ_INIT(cmd_queue);
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

int cmd_parse_req(struct command *cmd, struct mbuf *buf)
{
    struct command *ncmd;
    struct reader *r = cmd->reader;
    reader_feed(r, buf);

    while (buf->pos < buf->last) {
        if (parse(r) == -1) return CORVUS_ERR;

        if (reader_ready(r)) {
            ncmd = cmd_create(cmd->ctx);
            ncmd->parent = cmd;
            ncmd->client = cmd->client;
            ncmd->req_data = r->data;
            CMD_COPY_RANGE(&ncmd->req_buf[0], &ncmd->req_buf[1], r);

            STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);
            cmd->cmd_count++;

            r->data = NULL;
            r->type = PARSE_BEGIN;

            if (cmd_parse_token(ncmd) == -1) {
                cmd->cmd_fail_count++;
                ncmd->cmd_fail = 1;
                continue;
            }
            if (cmd_forward(ncmd) == -1) {
                cmd->cmd_fail_count++;
                ncmd->cmd_fail = 1;
                continue;
            }
        }
    }
    return CORVUS_OK;
}

int cmd_parse_rep(struct command *cmd, struct mbuf *buf)
{
    struct reader *r = cmd->reader;
    reader_feed(r, buf);

    while (buf->pos < buf->last) {
        if (parse(r) == -1) {
            LOG(ERROR, "cmd_parse_rep, parse error");
            return CORVUS_ERR;
        }

        if (reader_ready(r)) {
            cmd->rep_data = r->data;
            CMD_COPY_RANGE(&cmd->rep_buf[0], &cmd->rep_buf[1], r);

            r->data = NULL;
            r->type = PARSE_BEGIN;
            break;
        }
    }
    return CORVUS_OK;
}

int cmd_read_request(struct command *cmd, int fd)
{
    int n, size;
    struct mbuf *buf;

    if (cmd->reader == NULL) cmd->reader = reader_init(cmd->ctx);

    while (1) {
        do {
            buf = cmd_get_buf(cmd);
            size = mbuf_write_size(buf);

            LOG(DEBUG, "reading");
            n = socket_read(fd, buf);
            LOG(DEBUG, "socket_read %d", n);
            if (n == 0) return CORVUS_OK;
            if (n == CORVUS_ERR) return CORVUS_ERR;
            if (n == CORVUS_AGAIN) return CORVUS_AGAIN;
            if (cmd_parse_req(cmd, buf) == CORVUS_ERR) {
                return CORVUS_ERR;
            }
        } while (!reader_ready(cmd->reader));
    }

    return CORVUS_OK;
}

int cmd_read_reply(struct command *cmd, struct connection *server)
{
    int n, wsize, rsize;
    struct mbuf *buf;

    if (cmd->reader == NULL) cmd->reader = reader_init(cmd->ctx);

    do {
        buf = conn_get_buf(server);
        wsize = mbuf_write_size(buf);
        rsize = mbuf_read_size(buf);

        if (rsize > 0) {
            if (cmd_parse_rep(cmd, buf) == -1) return CORVUS_ERR;
            if (reader_ready(cmd->reader)) return CORVUS_OK;
        }

        n = socket_read(server->fd, buf);
        if (n == 0) return CORVUS_EOF;
        if (n == CORVUS_ERR) return CORVUS_ERR;
        if (n == CORVUS_AGAIN) return CORVUS_AGAIN;
        if (cmd_parse_rep(cmd, buf) == -1) {
            return CORVUS_ERR;
        }
    } while (!reader_ready(cmd->reader));

    return CORVUS_OK;
}

void init_command_map()
{
    uint32_t hash, idx;
    int len;
    struct pos pos;
    struct pos_array arr;
    struct cmd_item cmds[] = {
        CMD_DO(CMD_BUILD_MAP)
    };

    size_t i, cmds_len = sizeof(cmds) / sizeof(struct cmd_item);

    for (i = 0; i < CMD_MAP_LEN; i++) {
        command_map[i].value = -1;
        command_map[i].hash = 0;
        command_map[i].type = CMD_UNIMPL;
    }

    for (i = 0; i < cmds_len; i++) {
        len = strlen(cmds[i].cmd);
        pos.str = (uint8_t*)cmds[i].cmd;
        pos.len = len;

        arr.pos_len = 1;
        arr.str_len = len;
        arr.items = &pos;

        hash = lookup3_hash(&arr);
        idx = hash % CMD_DIVIDER;
        command_map[idx].value = cmds[i].value;
        command_map[idx].type = cmds[i].type;
        command_map[idx].hash = hash;
    }
}

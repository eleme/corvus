#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include "corvus.h"
#include "socket.h"
#include "logging.h"
#include "hash.h"
#include "slot.h"
#include "event.h"
#include "server.h"
#include "client.h"
#include "stats.h"
#include "alloc.h"

#define CMD_RECYCLE_SIZE 1024

#define CMD_DEFINE(cmd, type, access) CMD_##cmd,
#define CMD_BUILD_MAP(cmd, type, access) {#cmd, CMD_##cmd, CMD_##type, CMD_ACCESS_##access},

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
    HANDLER(QUIT,              UNIMPL,   UNKNOWN) \
    HANDLER(SELECT,            UNIMPL,   UNKNOWN)

#define CMD_INCREF(cmd)                                   \
do {                                                      \
    (cmd)->rep_buf[0].buf->refcount++;                    \
    if ((cmd)->rep_buf[1].buf != (cmd)->rep_buf[0].buf) { \
        (cmd)->rep_buf[1].buf->refcount++;                \
    }                                                     \
} while (0)

enum {
    CMD_DO(CMD_DEFINE)
};

enum {
    CMD_UNIMPL,
    CMD_BASIC,
    CMD_COMPLEX,
    CMD_EXTRA,
};

struct cmd_item {
    char *cmd;
    int value;
    int type;
    int access;
};

const char *rep_err = "-ERR Proxy error\r\n";
const char *rep_parse_err = "-ERR Proxy fail to parse command\r\n";
const char *rep_forward_err = "-ERR Proxy fail to forward command\r\n";
const char *rep_redirect_err = "-ERR Proxy redirecting error\r\n";
const char *rep_addr_err = "-ERR Proxy fail to parse server address\r\n";
const char *rep_server_err = "-ERR Proxy fail to get server\r\n";
const char *rep_timeout_err = "-ERR Proxy timed out\r\n";

static const char *rep_get = "*2\r\n$3\r\nGET\r\n";
static const char *rep_set = "*3\r\n$3\r\nSET\r\n";
static const char *rep_del = "*2\r\n$3\r\nDEL\r\n";
static const char *rep_exists = "*2\r\n$6\r\nEXISTS\r\n";
static const char *rep_ok = "+OK\r\n";
static const char *rep_ping = "+PONG\r\n";
static const char *rep_noauth = "-NOAUTH Authentication required.\r\n";
static const char *rep_auth_err = "-ERR invalid password\r\n";
static const char *rep_auth_not_set = "-ERR Client sent AUTH, but no password is set\r\n";

static struct cmd_item cmds[] = {CMD_DO(CMD_BUILD_MAP)};
static struct dict command_map;

static inline uint8_t *cmd_get_data(struct mbuf *b, struct buf_ptr ptr[], int *len)
{
    uint8_t *data;
    if (b == ptr[0].buf && b == ptr[1].buf) {
        data = ptr[0].pos;
        *len = ptr[1].pos - ptr[0].pos;
    } else if (b == ptr[0].buf) {
        data = ptr[0].pos;
        *len = b->last - ptr[0].pos;
    } else if (b == ptr[1].buf) {
        data = b->start;
        *len = ptr[1].pos - b->start;
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

    cmd->slot = -1;
    cmd->cmd_type = -1;
    cmd->request_type = -1;
    cmd->fail_reason = (char*)rep_err;
    cmd->cmd_count = -1;

    STAILQ_INIT(&cmd->sub_cmds);
}

static void cmd_recycle(struct context *ctx, struct command *cmd)
{
    ctx->mstats.cmds--;

    if (ctx->mstats.free_cmds > CMD_RECYCLE_SIZE) {
        cv_free(cmd);
    } else {
        STAILQ_NEXT(cmd, cmd_next) = NULL;
        STAILQ_NEXT(cmd, ready_next) = NULL;
        STAILQ_NEXT(cmd, waiting_next) = NULL;
        STAILQ_NEXT(cmd, sub_cmd_next) = NULL;
        STAILQ_INSERT_HEAD(&ctx->free_cmdq, cmd, cmd_next);

        ctx->mstats.free_cmds++;
    }
}

static int cmd_in_queue(struct command *cmd, struct connection *server)
{
    struct command *ready, *wait;

    ready = STAILQ_LAST(&server->info->ready_queue, command, ready_next);
    wait = STAILQ_LAST(&server->info->waiting_queue, command, waiting_next);

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
    if (item == NULL) {
        return CORVUS_ERR;
    }
    cmd->cmd_type = item->value;
    cmd->cmd_access = item->access;
    return item->type;
}

static int cmd_format_stats(char *dest, size_t n, struct stats *stats, char *latency)
{
    return snprintf(dest, n,
            "cluster:%s\r\n"
            "version:%s\r\n"
            "pid:%d\r\n"
            "threads:%d\r\n"
            "mem_allocator:%s\r\n"
            "used_cpu_sys:%.2f\r\n"
            "used_cpu_user:%.2f\r\n"
            "connected_clients:%lld\r\n"
            "completed_commands:%lld\r\n"
            "recv_bytes:%lld\r\n"
            "send_bytes:%lld\r\n"
            "remote_latency:%.6f\r\n"
            "total_latency:%.6f\r\n"
            "last_command_latency:%s\r\n"
            "remotes:%s\r\n",
            config.cluster, VERSION, getpid(), config.thread,
            CV_MALLOC_LIB,
            stats->used_cpu_sys, stats->used_cpu_user,
            stats->basic.connected_clients,
            stats->basic.completed_commands,
            stats->basic.recv_bytes, stats->basic.send_bytes,
            stats->basic.remote_latency / 1000000.0,
            stats->basic.total_latency / 1000000.0, latency,
            stats->remote_nodes);
}

int cmd_get_slot(struct redis_data *data)
{
    ASSERT_ELEMENTS(data->elements >= 2, data);

    struct redis_data *cmd_key = &data->element[1];
    ASSERT_TYPE(cmd_key, REP_STRING);
    return slot_get(&cmd_key->pos);
}

void cmd_add_fragment(struct command *cmd, struct pos_array *data,
        struct buf_ptr *start, struct buf_ptr *end)
{
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "$%d\r\n", data->str_len);
    conn_add_data(cmd->client, (uint8_t*)buf, n, start, NULL);

    int i;
    struct pos *p;
    for (i = 0; i < data->pos_len; i++) {
        p = &data->items[i];
        conn_add_data(cmd->client, p->str, p->len, NULL, NULL);
    }
    conn_add_data(cmd->client, (uint8_t*)"\r\n", 2, NULL, end);
}

int cmd_forward_basic(struct command *cmd)
{
    int slot;
    struct connection *server = NULL;
    struct context *ctx = cmd->ctx;

    slot = cmd->slot;
    if (slot == -1) {
        LOG(ERROR, "cmd_forward_basic: slot %d is invalid", slot);
        return CORVUS_ERR;
    }

    server = conn_get_server(ctx, slot, cmd->cmd_access);
    if (server == NULL) {
        LOG(ERROR, "cmd_forward_basic: fail to get server with slot %d", slot);
        return CORVUS_ERR;
    }
    cmd->server = server;

    server->info->last_active = time(NULL);

    LOG(DEBUG, "command with slot %d ready", slot);

    STAILQ_INSERT_TAIL(&server->info->ready_queue, cmd, ready_next);
    if (conn_register(server) == -1) {
        LOG(ERROR, "cmd_forward_basic: fail to register server %d", server->fd);
        /* cmd already marked failed in server_eof */
        server_eof(server, rep_err);
    }
    return CORVUS_OK;
}

/* mget, del, exists */
int cmd_forward_multikey(struct command *cmd, struct redis_data *data, const char *prefix)
{
    ASSERT_ELEMENTS(data->elements >= 2, data);

    cmd->cmd_count = data->elements - 1;

    size_t i;
    struct command *ncmd;
    struct redis_data *key;
    for (i = 1; i < data->elements; i++) {
        key = &data->element[i];

        ncmd = cmd_create(cmd->ctx);
        ncmd->parent = cmd;
        ncmd->client = cmd->client;
        STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);

        if (key->type != REP_STRING) {
            LOG(ERROR, "%s: expect data type %d got %d", __func__,
                    REP_STRING, key->type);
            cmd_mark_fail(ncmd, rep_forward_err);
            continue;
        }

        ncmd->slot = slot_get(&key->pos);
        ncmd->cmd_access = cmd->cmd_access;

        // no need to increase buf refcount
        memcpy(&ncmd->req_buf[0], &key->buf[0], sizeof(key->buf[0]));
        memcpy(&ncmd->req_buf[1], &key->buf[1], sizeof(key->buf[1]));
        ncmd->prefix = (char*)prefix;

        if (cmd_forward_basic(ncmd) == CORVUS_ERR) {
            cmd_mark_fail(ncmd, rep_forward_err);
        }
    }

    return CORVUS_OK;
}

int cmd_forward_mset(struct command *cmd, struct redis_data *data)
{
    ASSERT_ELEMENTS(data->elements >= 3 && (data->elements & 1) == 1, data);

    cmd->cmd_count = (data->elements - 1) >> 1;

    size_t i;
    struct command *ncmd;
    struct redis_data *key, *value;
    for (i = 1; i < data->elements; i += 2) {
        key = &data->element[i];
        value = &data->element[i + 1];

        ncmd = cmd_create(cmd->ctx);
        ncmd->parent = cmd;
        ncmd->client = cmd->client;
        STAILQ_INSERT_TAIL(&cmd->sub_cmds, ncmd, sub_cmd_next);

        if (key->type != REP_STRING || value->type != REP_STRING) {
            LOG(ERROR, "%s: expect key/value data type %d got %d/%d",
                    __func__, REP_STRING, key->type, value->type);
            cmd_mark_fail(ncmd, rep_forward_err);
            continue;
        }

        ncmd->slot = slot_get(&key->pos);
        ncmd->cmd_access = cmd->cmd_access;

        // no need to increase buf refcount
        memcpy(&ncmd->req_buf[0], &key->buf[0], sizeof(key->buf[0]));
        memcpy(&ncmd->req_buf[1], &value->buf[1], sizeof(value->buf[1]));
        ncmd->prefix = (char*)rep_set;

        if (cmd_forward_basic(ncmd) == CORVUS_ERR) {
            cmd_mark_fail(ncmd, rep_forward_err);
        }
    }

    return CORVUS_OK;
}

int cmd_forward_eval(struct command *cmd, struct redis_data *data)
{
    ASSERT_ELEMENTS(data->elements >= 4, data);
    ASSERT_TYPE(&data->element[3], REP_STRING);

    cmd->slot = slot_get(&data->element[3].pos);
    return cmd_forward_basic(cmd);
}

int cmd_forward_complex(struct command *cmd, struct redis_data *data)
{
    switch (cmd->cmd_type) {
        case CMD_MGET:
            return cmd_forward_multikey(cmd, data, rep_get);
        case CMD_MSET:
            return cmd_forward_mset(cmd, data);
        case CMD_DEL:
            return cmd_forward_multikey(cmd, data, rep_del);
        case CMD_EXISTS:
            return cmd_forward_multikey(cmd, data, rep_exists);
        case CMD_EVAL:
            return cmd_forward_eval(cmd, data);
        default:
            LOG(ERROR, "%s: unknown command type %d", __func__, cmd->cmd_type);
            return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int cmd_ping(struct command *cmd)
{
    conn_add_data(cmd->client, (uint8_t*)rep_ping, 7,
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);

    cmd_mark_done(cmd);
    return CORVUS_OK;
}

int cmd_info(struct command *cmd)
{
    int i, n = 0, size = 0;
    struct stats stats;
    memset(&stats, 0, sizeof(stats));
    stats_get(&stats);

    char latency[16 * config.thread];
    memset(latency, 0, sizeof(latency));

    for (i = 0; i < config.thread; i++) {
        n = snprintf(latency + size, 16, "%.6f", stats.last_command_latency[i] / 1000000.0);
        size += n;
        if (i < config.thread - 1) {
            latency[size++] = ',';
        }
    }
    n = cmd_format_stats(NULL, 0, &stats, latency);
    char info[n + 1];
    cmd_format_stats(info, sizeof(info), &stats, latency);

    char *fmt = "$%lu\r\n";
    size = snprintf(NULL, 0, fmt, n);
    char head[size + 1];
    snprintf(head, sizeof(head), fmt, n);

    conn_add_data(cmd->client, (uint8_t*)head, size, &cmd->rep_buf[0], NULL);
    conn_add_data(cmd->client, (uint8_t*)info, n, NULL, NULL);
    conn_add_data(cmd->client, (uint8_t*)"\r\n", 2, NULL, &cmd->rep_buf[1]);
    CMD_INCREF(cmd);

    cmd_mark_done(cmd);
    return CORVUS_OK;
}

int cmd_proxy_info(struct command *cmd)
{
    struct memory_stats stats;
    memset(&stats, 0, sizeof(stats));
    stats_get_memory(&stats);

    int n = 1024;
    char data[n + 1];
    snprintf(data, n,
        "+"
        "in_use_buffers:%lld\n"
        "free_buffers:%lld\n"
        "in_use_cmds:%lld\n"
        "free_cmds:%lld\n"
        "in_use_conns:%lld\n"
        "free_conns:%lld\n"
        "in_use_conn_info:%lld\n"
        "free_conn_info:%lld\n"
        "in_use_buf_times:%lld\n"
        "free_buf_times:%lld"
        "\r\n",
        stats.buffers, stats.free_buffers, stats.cmds, stats.free_cmds,
        stats.conns, stats.free_conns, stats.conn_info, stats.free_conn_info,
        stats.buf_times, stats.free_buf_times);

    conn_add_data(cmd->client, (uint8_t*)data, strlen(data),
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);

    cmd_mark_done(cmd);
    return CORVUS_OK;
}

int cmd_proxy(struct command *cmd, struct redis_data *data)
{
    ASSERT_TYPE(data, REP_ARRAY);
    ASSERT_ELEMENTS(data->elements >= 2, data);

    struct redis_data *op = &data->element[1];
    ASSERT_TYPE(op, REP_STRING);

    char type[op->pos.str_len + 1];
    if (pos_to_str(&op->pos, type) == CORVUS_ERR) {
        LOG(ERROR, "cmd_proxy: parse error");
        return CORVUS_ERR;
    }

    if (strcasecmp(type, "INFO") == 0) {
        return cmd_proxy_info(cmd);
    } else if (strcasecmp(type, "UPDATESLOTMAP") == 0) {
        slot_create_job(SLOT_UPDATE);
        conn_add_data(cmd->client, (uint8_t*)rep_ok, strlen(rep_ok),
                &cmd->rep_buf[0], &cmd->rep_buf[1]);
        CMD_INCREF(cmd);
        cmd_mark_done(cmd);
    } else {
        cmd_mark_fail(cmd, rep_err);
    }
    return CORVUS_OK;
}

int cmd_auth(struct command *cmd, struct redis_data *data)
{
    ASSERT_TYPE(data, REP_ARRAY);
    ASSERT_ELEMENTS(data->elements == 2, data);

    struct redis_data *pass_data = &data->element[1];
    ASSERT_TYPE(pass_data, REP_STRING);

    char password[pass_data->pos.str_len + 1];
    if (pos_to_str(&pass_data->pos, password) == CORVUS_ERR) {
        LOG(ERROR, "cmd_auth: parse error");
        return CORVUS_ERR;
    }

    if (config.requirepass == NULL) {
        cmd_mark_fail(cmd, rep_auth_not_set);
    } else if (strcmp(config.requirepass, password) == 0) {
        conn_add_data(cmd->client, (uint8_t*)rep_ok, strlen(rep_ok),
                &cmd->rep_buf[0], &cmd->rep_buf[1]);
        CMD_INCREF(cmd);
        cmd->client->info->authenticated = true;
        cmd_mark_done(cmd);
    } else {
        cmd->client->info->authenticated = false;
        cmd_mark_fail(cmd, rep_auth_err);
    }
    return CORVUS_OK;
}

int cmd_extra(struct command *cmd, struct redis_data *data)
{
    switch (cmd->cmd_type) {
        case CMD_PING:
            return cmd_ping(cmd);
        case CMD_INFO:
            return cmd_info(cmd);
        case CMD_PROXY:
            return cmd_proxy(cmd, data);
        case CMD_AUTH:
            return cmd_auth(cmd, data);
        default:
            LOG(ERROR, "%s: unknown command type %d", __func__, cmd->cmd_type);
            return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int cmd_forward(struct command *cmd, struct redis_data *data)
{
    LOG(DEBUG, "forward command %p(%d)", cmd, cmd->cmd_type);
    struct connection *client = cmd->client;
    if (config.requirepass != NULL && !client->info->authenticated
            && cmd->cmd_type != CMD_AUTH)
    {
        cmd_mark_fail(cmd, rep_noauth);
        return CORVUS_OK;
    }

    switch (cmd->request_type) {
        case CMD_BASIC:
            cmd->slot = cmd_get_slot(data);
            return cmd_forward_basic(cmd);
        case CMD_COMPLEX:
            return cmd_forward_complex(cmd, data);
        case CMD_EXTRA:
            return cmd_extra(cmd, data);
        case CMD_UNIMPL:
            return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int cmd_parse_token(struct command *cmd, struct redis_data *data)
{
    ASSERT_TYPE(data, REP_ARRAY);
    ASSERT_ELEMENTS(data->elements >= 1, data);

    struct redis_data *f1 = &data->element[0];

    ASSERT_TYPE(f1, REP_STRING);

    cmd->request_type = cmd_get_type(cmd, &f1->pos);
    if (cmd->request_type <= 0) {
        char name[f1->pos.str_len + 1];
        pos_to_str(&f1->pos, name);
        LOG(ERROR, "%s: fail to parse command %s", __func__, name);
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int cmd_parse_req(struct command *cmd, struct mbuf *buf)
{
    struct reader *r = &cmd->client->info->reader;
    reader_feed(r, buf);

    if (parse(r, MODE_REQ) == CORVUS_ERR) {
        return CORVUS_ERR;
    }

    if (reader_ready(r)) {
        ASSERT_TYPE(&r->data, REP_ARRAY);
        ASSERT_ELEMENTS(r->data.elements >= 1, &r->data);

        cmd->keys = r->data.elements - 1;
        cmd->parse_done = true;
        cmd->cmd_count = 1;

        memcpy(&cmd->req_buf[0], &r->start, sizeof(r->start));
        memset(&r->start, 0, sizeof(r->start));

        memcpy(&cmd->req_buf[1], &r->end, sizeof(r->end));
        memset(&r->end, 0, sizeof(r->end));

        if (cmd_parse_token(cmd, &r->data) == CORVUS_ERR) {
            redis_data_free(&r->data);
            cmd_mark_fail(cmd, rep_parse_err);
            return CORVUS_OK;
        }
        if (cmd_forward(cmd, &r->data) == CORVUS_ERR) {
            redis_data_free(&r->data);
            cmd_mark_fail(cmd, rep_forward_err);
            return CORVUS_OK;
        }
        redis_data_free(&r->data);
    }
    return CORVUS_OK;
}

int cmd_parse_rep(struct command *cmd, struct mbuf *buf)
{
    struct reader *r = &cmd->server->info->reader;
    reader_feed(r, buf);

    if (parse(r, MODE_REP) == CORVUS_ERR) {
        LOG(ERROR, "cmd_parse_rep: parse error");
        return CORVUS_ERR;
    }

    memcpy(&cmd->rep_buf[0], &r->start, sizeof(r->start));

    if (reader_ready(r)) {
        cmd->reply_type = r->redis_data_type;
        if (cmd->reply_type == REP_INTEGER) {
            cmd->integer_data = r->item_size;
        }

        memcpy(&cmd->rep_buf[1], &r->end, sizeof(r->end));
        memset(&r->start, 0, sizeof(r->start));
        memset(&r->end, 0, sizeof(r->end));
        r->redis_data_type = REP_UNKNOWN;

        redis_data_free(&r->data);
    }
    return CORVUS_OK;
}

void cmd_gen_mget_iovec(struct command *cmd, struct iov_data *iov)
{
    struct command *c, *temp;
    int n, setted = 0;

    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail) {
            temp = c;
            cmd_iov_add(iov, c->fail_reason, strlen(c->fail_reason), NULL);
            setted = 1;
            break;
        }
        if (c->reply_type == REP_ERROR) {
            temp = c;
            cmd_create_iovec(c->rep_buf, iov);
            setted = 1;
            break;
        }
    }
    if (setted) {
        STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
            if (c == temp || c->cmd_fail) continue;
            mbuf_range_clear(cmd->ctx, c->rep_buf);
        }
    } else {
        const char *fmt = "*%ld\r\n";
        n = snprintf(iov->buf, sizeof(iov->buf) - 1, fmt, cmd->keys);
        cmd_iov_add(iov, iov->buf, n, NULL);
        STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
            cmd_create_iovec(c->rep_buf, iov);
        }
    }
}

void cmd_gen_mset_iovec(struct command *cmd, struct iov_data *iov)
{
    struct command *c;
    int setted = 0;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail && !setted) {
            cmd_iov_add(iov, c->fail_reason, strlen(c->fail_reason), NULL);
            setted = 1;
            continue;
        }
        if (c->reply_type == REP_ERROR && !setted) {
            cmd_create_iovec(c->rep_buf, iov);
            setted = 1;
            continue;
        }

        if (!c->cmd_fail) {
            mbuf_range_clear(cmd->ctx, c->rep_buf);
        }
    }
    if (setted) return;
    cmd_iov_add(iov, (void*)rep_ok, strlen(rep_ok), NULL);
}

void cmd_gen_multikey_iovec(struct command *cmd, struct iov_data *iov)
{
    struct command *c;
    int n = 0, count = 0, setted = 0;
    STAILQ_FOREACH(c, &cmd->sub_cmds, sub_cmd_next) {
        if (c->cmd_fail && !setted) {
            cmd_iov_add(iov, c->fail_reason, strlen(c->fail_reason), NULL);
            setted = 1;
            continue;
        }
        if (c->reply_type == REP_ERROR && !setted) {
            cmd_create_iovec(c->rep_buf, iov);
            setted = 1;
            continue;
        }
        if (!c->cmd_fail && c->reply_type != REP_ERROR && c->integer_data == 1) {
            count++;
        }
        if (!c->cmd_fail) {
            mbuf_range_clear(cmd->ctx, c->rep_buf);
        }
    }

    if (setted) return;

    const char *fmt = ":%ld\r\n";
    n = snprintf(iov->buf, sizeof(iov->buf) - 1, fmt, count);
    cmd_iov_add(iov, iov->buf, n, NULL);
}

void cmd_mark(struct command *cmd, int fail)
{
    LOG(DEBUG, "mark cmd %p", cmd);
    struct command *root = NULL;
    if (fail) cmd->cmd_fail = true;

    if (cmd->parent == NULL) {
        cmd->cmd_done_count = 1;
        root = cmd;
    } else {
        cmd->parent->cmd_done_count++;
        if (cmd->parent->cmd_done_count == cmd->parent->cmd_count) {
            root = cmd->parent;
        }
    }

    if (root != NULL && conn_register(root->client) == CORVUS_ERR) {
        LOG(ERROR, "fail to reregister client %d", root->client->fd);
        client_eof(root->client);
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
        ctx->mstats.free_cmds--;
        STAILQ_NEXT(cmd, cmd_next) = NULL;
    } else {
        cmd = cv_malloc(sizeof(struct command));
    }
    cmd_init(ctx, cmd);
    ctx->mstats.cmds++;
    return cmd;
}

int cmd_read_rep(struct command *cmd, struct connection *server)
{
    int rsize, status;
    struct mbuf *buf;

    while (1) {
        buf = conn_get_buf(server, true, false);
        rsize = mbuf_read_size(buf);

        if (rsize <= 0) {
            status = conn_read(server, buf);
            if (status != CORVUS_OK) return status;
        }

        if (cmd_parse_rep(cmd, buf) == CORVUS_ERR) return CORVUS_ERR;
        if (reader_ready(&server->info->reader)) break;
    }

    return CORVUS_OK;
}

void cmd_create_iovec(struct buf_ptr ptr[], struct iov_data *iov)
{
    uint8_t *data;
    int len;
    struct mbuf *b = ptr[0].buf;

    while (b != NULL) {
        data = cmd_get_data(b, ptr, &len);
        cmd_iov_add(iov, (void*)data, len, b);

        if (b == ptr[1].buf) break;
        b = TAILQ_NEXT(b, next);
    }
}

/* cmd should be done */
void cmd_make_iovec(struct command *cmd, struct iov_data *iov)
{
    LOG(DEBUG, "cmd count %d", cmd->cmd_count);
    if (cmd->cmd_fail) {
        cmd_iov_add(iov, cmd->fail_reason, strlen(cmd->fail_reason), NULL);
        return;
    }
    switch (cmd->cmd_type) {
        case CMD_MGET:
            cmd_gen_mget_iovec(cmd, iov);
            break;
        case CMD_MSET:
            cmd_gen_mset_iovec(cmd, iov);
            break;
        case CMD_DEL:
        case CMD_EXISTS:
            cmd_gen_multikey_iovec(cmd, iov);
            break;
        default:
            cmd_create_iovec(cmd->rep_buf, iov);
            break;
    }
}

int cmd_parse_redirect(struct command *cmd, struct redirect_info *info)
{
    int n = 63;
    char err[n + 1];
    int len = 0, size;

    uint8_t *p;

    struct mbuf *b = cmd->rep_buf[0].buf;
    while (b != NULL) {
        p = cmd_get_data(b, cmd->rep_buf, &size);

        size = MIN(size, n - len);
        memcpy(err + len, p, size);
        len += size;
        if (len >= n) break;

        if (b == cmd->rep_buf[1].buf) break;
        b = TAILQ_NEXT(b, next);
    }
    err[len] = '\0';

    char name[8];
    LOG(DEBUG, "parse redirect: %s", err);

    int r = 0;
    if (strncmp(err, "-MOVED", 5) == 0) {
        /* -MOVED 16383 127.0.0.1:8001 */
        info->type = CMD_ERR_MOVED;
        r = sscanf(err, "%s%d%s", name, (int*)&info->slot, info->addr);
        if (r != 3) {
            LOG(ERROR, "cmd_parse_redirect: fail to parse moved response: %s", err);
            return CORVUS_ERR;
        }
    } else if (strncmp(err, "-ASK", 3) == 0) {
        /* -ASK 16383 127.0.0.1:8001 */
        info->type = CMD_ERR_ASK;
        r = sscanf(err, "%s%d%s", name, (int*)&info->slot, info->addr);
        if (r != 3) {
            LOG(ERROR, "cmd_parse_redirect: fail to parse ask response: %s", err);
            return CORVUS_ERR;
        }
    } else if (strncmp(err, "-CLUSTERDOWN", 12) == 0) {
        /* -CLUSTERDOWN The cluster is down */
        info->type = CMD_ERR_CLUSTERDOWN;
    }
    return CORVUS_OK;
}

void cmd_mark_done(struct command *cmd)
{
    cmd_mark(cmd, 0);
}

/* rep data referenced in server should be freed before mark fail */
void cmd_mark_fail(struct command *cmd, const char *reason)
{
    memset(cmd->rep_buf, 0, sizeof(cmd->rep_buf));
    cmd->fail_reason = (char*)reason;
    cmd_mark(cmd, 1);
}

void cmd_stats(struct command *cmd, int64_t end_time)
{
    struct context *ctx = cmd->ctx;
    struct command *last, *first;
    long long latency;

    ATOMIC_INC(ctx->stats.completed_commands, 1);

    latency = end_time - cmd->parse_time;

    ATOMIC_INC(ctx->stats.total_latency, latency);
    ATOMIC_SET(ctx->last_command_latency, latency);

    if (!STAILQ_EMPTY(&cmd->sub_cmds)) {
        first = STAILQ_FIRST(&cmd->sub_cmds);
        last = STAILQ_LAST(&cmd->sub_cmds, command, sub_cmd_next);
        latency = last->rep_time[1] - first->rep_time[0];
    } else {
        latency = cmd->rep_time[1] - cmd->rep_time[0];
    }
    ATOMIC_INC(ctx->stats.remote_latency, latency);
}

void cmd_set_stale(struct command *cmd)
{
    struct command *c;
    if (!STAILQ_EMPTY(&cmd->sub_cmds)) {
        cmd->refcount = cmd->cmd_count + 1;
        while (!STAILQ_EMPTY(&cmd->sub_cmds)) {
            c = STAILQ_FIRST(&cmd->sub_cmds);
            STAILQ_REMOVE_HEAD(&cmd->sub_cmds, sub_cmd_next);
            STAILQ_NEXT(c, sub_cmd_next) = NULL;
            c->cmd_ref = cmd;
            cmd_set_stale(c);
        }
        cmd->refcount--;
        if (cmd->refcount <= 0) {
            cmd_free(cmd);
        }
    } else if (cmd->server != NULL && cmd_in_queue(cmd, cmd->server)) {
        LOG(DEBUG, "command set stale");
        cmd->stale = true;
        cmd->conn_ref = cmd->client;
        cmd->client->info->refcount++;
    } else {
        mbuf_range_clear(cmd->ctx, cmd->rep_buf);
        cmd_free(cmd);
    }
}

void cmd_iov_add(struct iov_data *iov, void *buf, size_t len, struct mbuf *b)
{
    if (iov->cursor >= CORVUS_IOV_MAX) {
        iov->len -= iov->cursor;
        memmove(iov->data, iov->data + iov->cursor, iov->len * sizeof(struct iovec));
        memmove(iov->buf_ptr, iov->buf_ptr + iov->cursor, iov->len * sizeof(struct mbuf*));
        iov->cursor = 0;
    }

    if (iov->max_size <= iov->len) {
        iov->max_size *= 2;
        if (iov->max_size == 0) iov->max_size = CORVUS_IOV_MAX;
        iov->data = cv_realloc(iov->data, sizeof(struct iovec) * iov->max_size);
        iov->buf_ptr = cv_realloc(iov->buf_ptr, sizeof(struct mbuf*) * iov->max_size);
    }

    iov->data[iov->len].iov_base = buf;
    iov->data[iov->len].iov_len = len;
    iov->buf_ptr[iov->len] = b;
    iov->len++;
}

void cmd_iov_reset(struct iov_data *iov)
{
    iov->cursor = 0;
    iov->len = 0;
}

void cmd_iov_clear(struct context *ctx, struct iov_data *iov)
{
    struct mbuf **bufs = iov->buf_ptr + iov->cursor;
    int n = iov->len - iov->cursor;

    mbuf_decref(ctx, bufs, n);
}

void cmd_iov_free(struct iov_data *iov)
{
    if (iov->data != NULL) cv_free(iov->data);
    if (iov->buf_ptr != NULL) cv_free(iov->buf_ptr);
    iov->data = NULL;
    iov->buf_ptr = NULL;
    iov->max_size = 0;
    iov->cursor = 0;
    iov->len = 0;
}

void cmd_free(struct command *cmd)
{
    struct command *c;
    struct context *ctx = cmd->ctx;

    if (cmd->parent == NULL && cmd->client != NULL) {
        client_range_clear(cmd->client, cmd);
    }

    while (!STAILQ_EMPTY(&cmd->sub_cmds)) {
        c = STAILQ_FIRST(&cmd->sub_cmds);
        STAILQ_REMOVE_HEAD(&cmd->sub_cmds, sub_cmd_next);
        cmd_free(c);
    }

    if (cmd->cmd_ref != NULL) {
        cmd->cmd_ref->refcount--;
        if (cmd->cmd_ref->refcount <= 0) {
            cmd_free(cmd->cmd_ref);
        }
        cmd->cmd_ref = NULL;
    }

    if (cmd->conn_ref != NULL) {
        cmd->conn_ref->info->refcount--;
        if (cmd->conn_ref->info->refcount <= 0) {
            conn_buf_free(cmd->conn_ref);
            if (!cmd->conn_ref->event_triggered) {
                conn_free(cmd->conn_ref);
                conn_recycle(ctx, cmd->conn_ref);
            }
        }
        cmd->conn_ref = NULL;
    }

    cmd->client = NULL;
    cmd->server = NULL;
    cmd_recycle(ctx, cmd);
}

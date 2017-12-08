#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
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
#include "slowlog.h"
#include "config.h"
#include "array.h"

#define CMD_RECYCLE_SIZE 1024

// C的宏中, #的功能是将它后面的宏参数进行字符化操作, 在对它所引用的宏变量通过替换后, 在左右各加一个引号
// ##是连接符, 用来将两个token链接为一个token
#define CMD_BUILD_MAP(cmd, type, access) {#cmd, CMD_##cmd, CMD_##type, CMD_ACCESS_##access},

#define CMD_INCREF(cmd)                                   \
do {                                                      \
    (cmd)->rep_buf[0].buf->refcount++;                    \
    if ((cmd)->rep_buf[1].buf != (cmd)->rep_buf[0].buf) { \
        (cmd)->rep_buf[1].buf->refcount++;                \
    }                                                     \
} while (0)

const char *rep_err = "-ERR Proxy error\r\n";
const char *rep_parse_err = "-ERR Proxy fail to parse command\r\n";
const char *rep_forward_err = "-ERR Proxy fail to forward command\r\n";
const char *rep_redirect_err = "-ERR Proxy redirecting error\r\n";
const char *rep_addr_err = "-ERR Proxy fail to parse server address\r\n";
const char *rep_server_err = "-ERR Proxy fail to get server\r\n";
const char *rep_timeout_err = "-ERR Proxy timed out\r\n";
const char *rep_slowlog_not_enabled = "-ERR Slowlog not enabled\r\n";
const char *rep_in_progress = "-ERR Operation in progress\r\n";

const char *rep_config_err = "-ERR Config error\r\n";
const char *rep_config_unsupported_err = "-ERR Config option not supported\r\n";
const char *rep_config_parse_err = "-ERR Invalid config option or value\r\n";

const char *rep_get = "*2\r\n$3\r\nGET\r\n";
const char *rep_set = "*3\r\n$3\r\nSET\r\n";
const char *rep_del = "*2\r\n$3\r\nDEL\r\n";
const char *rep_exists = "*2\r\n$6\r\nEXISTS\r\n";

static const char *rep_ok = "+OK\r\n";
static const char *rep_ping = "+PONG\r\n";
static const char *rep_noauth = "-NOAUTH Authentication required.\r\n";
static const char *rep_auth_err = "-ERR invalid password\r\n";
static const char *rep_auth_not_set = "-ERR Client sent AUTH, but no password is set\r\n";
static const char *rep_select_not_allowed = "-ERR SELECT is not allowed in cluster mode\r\n";


struct cmd_item cmds[] = {CMD_DO(CMD_BUILD_MAP)};
const size_t CMD_NUM = sizeof(cmds) / sizeof(struct cmd_item);
static struct dict command_map;

const char *cmd_extract_prefix(const char *prefix)
{
    const char *get = "$3\r\nGET\r\n";
    const char *set = "$3\r\nSET\r\n";
    const char *del = "$3\r\nDEL\r\n";
    const char *exists = "$6\r\nEXISTS\r\n";
    return prefix == rep_get ? cv_strndup(get, strlen(get)) :
        prefix == rep_set ? cv_strndup(set, strlen(set)) :
        prefix == rep_del ? cv_strndup(del, strlen(del)) :
        prefix == rep_exists ? cv_strndup(exists, strlen(exists)) :
        NULL;
}

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
            "slot_update_jobs:%lld\r\n"
            "recv_bytes:%lld\r\n"
            "send_bytes:%lld\r\n"
            "remote_latency:%.6f\r\n"
            "total_latency:%.6f\r\n"
            "last_command_latency:%s\r\n"
            "ask_recv:%lld\r\n"
            "moved_recv:%lld\r\n"
            "remotes:%s\r\n",
            config.cluster, VERSION, getpid(), config.thread,
            CV_MALLOC_LIB,
            stats->used_cpu_sys, stats->used_cpu_user,
            stats->basic.connected_clients,
            stats->basic.completed_commands,
            stats->basic.slot_update_jobs,
            stats->basic.recv_bytes, stats->basic.send_bytes,
            stats->basic.remote_latency / 1000000.0,
            stats->basic.total_latency / 1000000.0, latency,
            stats->basic.ask_recv,
            stats->basic.moved_recv,
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

// 对type=basic的redis指令的处理函数, 直接转给corvus server, 主要有一下几步:
// 1. 获取corvus到对应redis实例的连接, 并把该连接与command对象绑定
// 2. 把command对象插入ready_queue队列的队尾
// 3. 把这个连接注册到epoll事件循环上, 监听事件类型是可读和可写
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

    // 获取从corvus到目标redis实例(存储所需要的redis key的机器)的连接
    server = conn_get_server(ctx, slot, cmd->cmd_access);
    if (server == NULL) {
        LOG(ERROR, "cmd_forward_basic: fail to get server with slot %d", slot);
        return CORVUS_ERR;
    }
    cmd->server = server;       // 把command对象和到对应redis连接绑定

    server->info->last_active = time(NULL);     // 更新corvus到redis实例的最后活跃时间

    LOG(DEBUG, "command with slot %d ready", slot);

    // 把command对象插入corvus server监听的ready_queue队列的队尾
    STAILQ_INSERT_TAIL(&server->info->ready_queue, cmd, ready_next);
    // 把从corvus到redis实例的连接注册到epoll事件循环中, 监听事件类型为可读和可写
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
        ncmd->data.type = REP_ARRAY;
        ncmd->data.elements = 1;
        ncmd->data.element = key;

        if (cmd_forward_basic(ncmd) == CORVUS_ERR) {
            cmd_mark_fail(ncmd, rep_forward_err);
        }
    }

    return CORVUS_OK;
}

// 对于mset指令的操作, 依次拆分成多个set指令, 放入sub_cmds队列中
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

        ncmd = cmd_create(cmd->ctx);    // 创建子command对象
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
        ncmd->data.type = REP_ARRAY;
        ncmd->data.elements = 2;
        ncmd->data.element = &data->element[i];

        if (cmd_forward_basic(ncmd) == CORVUS_ERR) {    // 依次对每个set命令当做type=basic的命令进行转发操作
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

// 对于type=complex的redis指令, 根据不同的执行, 进行不同的操作
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
    char content[n + 1];
    char data[n + 1];
    int content_len = snprintf(content, sizeof(content),
        "in_use_buffers:%lld\r\n"
        "free_buffers:%lld\r\n"
        "in_use_cmds:%lld\r\n"
        "free_cmds:%lld\r\n"
        "in_use_conns:%lld\r\n"
        "free_conns:%lld\r\n"
        "in_use_conn_info:%lld\r\n"
        "free_conn_info:%lld\r\n"
        "in_use_buf_times:%lld\r\n"
        "free_buf_times:%lld\r\n",
        stats.buffers, stats.free_buffers, stats.cmds, stats.free_cmds,
        stats.conns, stats.free_conns, stats.conn_info, stats.free_conn_info,
        stats.buf_times, stats.free_buf_times);
    int data_len = snprintf(data, sizeof(data), "$%d\r\n%s\r\n", content_len, content);

    conn_add_data(cmd->client, (uint8_t*)data, data_len,
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

int cmd_config_set(struct command *cmd, char *option, struct pos_array *value_param)
{
    if (!config_option_changable(option)) {
        cmd_mark_fail(cmd, rep_config_unsupported_err);
        return CORVUS_OK;
    }

    char value[value_param->str_len + 1];
    if (pos_to_str(value_param, value) != CORVUS_OK) {
        cmd_mark_fail(cmd, rep_err);
        return CORVUS_OK;
    }

    if (strcmp(option, "node") == 0) {
        // config set node host:port,host1:port1
        if (config_add("node", value) != CORVUS_OK) {
            cmd_mark_fail(cmd, rep_config_parse_err);
            return CORVUS_OK;
        } else {
            slot_create_job(SLOT_RELOAD);
        }
    } else {
        if (config_add(option, value) != CORVUS_OK) {
            cmd_mark_fail(cmd, rep_config_parse_err);
            return CORVUS_OK;
        }
    }
    conn_add_data(cmd->client, (uint8_t*) rep_ok, strlen(rep_ok),
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);
    return CORVUS_OK;
}

int cmd_config_get(struct command *cmd, const char *option)
{
    if (strcmp(option, "requirepass") == 0) {
        cmd_mark_fail(cmd, rep_config_unsupported_err);
        return CORVUS_OK;
    }
    struct cvstr value = cvstr_new(1024);
    while (true) {
        int res = config_get(option, value.data, value.capacity);
        if (res == CORVUS_ERR) {
            cmd_mark_fail(cmd, rep_config_unsupported_err);
            cvstr_free(&value);
            return CORVUS_OK;
        }
        if (!cvstr_full(&value)) break;
        cvstr_reserve(&value, value.capacity * 2);
    }
    const size_t RESP_LEN = 100;
    struct cvstr packet = cvstr_new(value.capacity + RESP_LEN);
    int data_len = snprintf(packet.data, packet.capacity,
            "$%zu\r\n%s\r\n", strlen(value.data), value.data);
    conn_add_data(cmd->client, (uint8_t*) packet.data, data_len,
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);

    cvstr_free(&value);
    cvstr_free(&packet);
    return CORVUS_OK;
}

int cmd_config_rewrite(struct command *cmd)
{
    int res = config_rewrite();
    if (res == CORVUS_AGAIN) {
        LOG(INFO, "Config rewrite is already in progress");
        cmd_mark_fail(cmd, rep_in_progress);
    } else if (res == CORVUS_OK) {
        conn_add_data(cmd->client, (uint8_t*) rep_ok, strlen(rep_ok),
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
        CMD_INCREF(cmd);
        cmd_mark_done(cmd);
    } else {
        cmd_mark_fail(cmd, rep_err);
    }
    return CORVUS_OK;
}

int cmd_config(struct command *cmd, struct redis_data *data)
{
    ASSERT_TYPE(data, REP_ARRAY);
    ASSERT_ELEMENTS(data->elements >= 2, data);

    struct redis_data *op = &data->element[1];
    ASSERT_TYPE(op, REP_STRING);

    char type[op->pos.str_len + 1];
    if (pos_to_str(&op->pos, type) == CORVUS_ERR) {
        LOG(ERROR, "cmd_config: parse error");
        return CORVUS_ERR;
    }

    if (data->elements >= 3) {
        struct redis_data *opt = &data->element[2];
        ASSERT_TYPE(opt, REP_STRING);
        char option[opt->pos.str_len + 1];
        if (pos_to_str(&opt->pos, option) == CORVUS_ERR) {
            LOG(ERROR, "cmd_config: parse error");
            return CORVUS_ERR;
        }
        for (char *p = option; *p; p++) {
            *p = tolower(*p);
        }
        if (strcasecmp(type, "SET") == 0) {
            //config set <item> <val>
            ASSERT_ELEMENTS(data->elements == 4, data);
            return cmd_config_set(cmd, option, &data->element[3].pos);
        } else if (strcasecmp(type, "GET") == 0) {
            //config get <item>
            ASSERT_ELEMENTS(data->elements == 3, data);
            return cmd_config_get(cmd, option);
        } else {
            cmd_mark_fail(cmd, rep_config_err);
        }
    } else if (strcasecmp(type, "REWRITE") == 0) {
        return cmd_config_rewrite(cmd);
    } else {
        cmd_mark_fail(cmd, rep_config_err);
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

int cmd_time(struct command *cmd)
{
    struct timeval tm;
    if (gettimeofday(&tm, NULL) == -1) {
        LOG(ERROR, "cmd_time: %s", strerror(errno));
        return CORVUS_ERR;
    }
    char time_fmt[100];
    char time_sec[21];
    char time_us[21];
    int sec_len = snprintf(time_sec, sizeof(time_sec), "%ld", tm.tv_sec);
    int usec_len = snprintf(time_us, sizeof(time_us), "%ld", (long)tm.tv_usec);
    int size = snprintf(time_fmt, sizeof(time_fmt),
            "*2\r\n"
            "$%d\r\n"
            "%s\r\n"
            "$%d\r\n"
            "%s\r\n",
            sec_len, time_sec, usec_len, time_us);
    conn_add_data(cmd->client, (uint8_t*)time_fmt, size,
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);
    return CORVUS_OK;
}

int cmd_quit(struct command *cmd)
{
    conn_add_data(cmd->client, (uint8_t*)rep_ok, strlen(rep_ok),
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);
    return CORVUS_OK;
}

int cmd_select(struct command *cmd, struct redis_data *data)
{
    ASSERT_TYPE(data, REP_ARRAY);
    ASSERT_ELEMENTS(data->elements == 2, data);

    struct redis_data *db_data = &data->element[1];
    ASSERT_TYPE(db_data, REP_STRING);

    if (pos_is_zero(&db_data->pos) == CORVUS_OK) {
        conn_add_data(cmd->client, (uint8_t*)rep_ok, 5,
                      &cmd->rep_buf[0], &cmd->rep_buf[1]);
        CMD_INCREF(cmd);
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }
    cmd_mark_fail(cmd, rep_select_not_allowed);
    return CORVUS_OK;
}

static int cmd_parse_len(struct redis_data *data, int *result)
{
    ASSERT_TYPE(data, REP_STRING);
    char len_limit[data->pos.str_len + 1];
    if (pos_to_str(&data->pos, len_limit) == CORVUS_ERR) {
        LOG(ERROR, "parse_len: emptry arg string");
        return CORVUS_ERR;
    }

    *result = atoi(len_limit);
    if (*result <= 0) {
        return CORVUS_ERR;
    }

    return CORVUS_OK;
}

static int cmd_slowlog_entry_cmp(const void * lhs, const void * rhs)
{
    const struct slowlog_entry *e1 = *((struct slowlog_entry**)lhs);
    const struct slowlog_entry *e2 = *((struct slowlog_entry**)rhs);
    return e1->log_time < e2->log_time ? -1 :
           e1->log_time > e2->log_time ? 1 :
           e1->id < e2->id ? -1 :
           e1->id > e2->id ? 1 : 0;
}

int cmd_slowlog_get(struct command *cmd, struct redis_data *data)
{
    // For example 'slowlog get 128', element[2] is 128 here
    int len = config.slowlog_max_len;
    if (data->elements > 3) {
        LOG(DEBUG, "cmd_slowlog_get: too many arguments");
        return CORVUS_ERR;
    } else if (data->elements == 3) {
        int len_limit;
        struct redis_data *len_limit_data = &data->element[2];
        if (cmd_parse_len(len_limit_data, &len_limit) == CORVUS_ERR) {
            return CORVUS_ERR;
        }
        if (len_limit == 0) {
            LOG(ERROR, "cmd_slowlog_get: invalid len");
            return CORVUS_ERR;
        }
        if (len_limit < len) {
            len = len_limit;
        }
    }

    struct context *contexts = get_contexts();
    struct slowlog_entry *entries[len];
    int count = 0;
    size_t queue_len = contexts[0].slowlog.capacity;
    for (size_t i = 0; i != queue_len && count < len; i++) {
        for (size_t j = 0; j != config.thread && count < len; j++) {
            struct slowlog_queue *queue = &contexts[j].slowlog;
            // slowlog_get will lock mutex
            struct slowlog_entry *entry = slowlog_get(queue, i);
            if (entry) {
                entries[count++] = entry;
            }
        }
    }

    qsort(entries, count, sizeof(struct slowlog_entry*), cmd_slowlog_entry_cmp);

    // generate redis packet
    const char *hdr_fmt =
        "*5\r\n"
        ":%lld\r\n"  // id
        ":%lld\r\n"  // log time
        ":%lld\r\n"  // remote latency
        ":%lld\r\n"  // total latency
        "*%d\r\n";  // cmd arg len
    char buf[200];

    int size = snprintf(buf, sizeof buf, "*%d\r\n", count);
    conn_add_data(cmd->client, (uint8_t*)buf, size,
            &cmd->rep_buf[0], &cmd->rep_buf[1]);

    for (size_t i = 0; i != count; i++) {
        struct slowlog_entry *entry = entries[i];
        assert(entry->argc > 0);
        size = snprintf(buf, sizeof buf, hdr_fmt,
                entry->id, entry->log_time, entry->remote_latency, entry->total_latency, entry->argc);
        assert(size < 150);
        conn_add_data(cmd->client, (uint8_t*)buf, size, NULL, NULL);

        for (size_t j = 0; j != entry->argc; j++) {
            struct pos *arg = entry->argv + j;
            conn_add_data(cmd->client, arg->str, arg->len,
                    NULL, &cmd->rep_buf[1]);
        }
        slowlog_dec_ref(entry);
    }
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);

    return CORVUS_OK;
}

int cmd_slowlog_len(struct command *cmd)
{
    int len = 0;
    struct context *contexts = get_contexts();

    for (size_t i = 0; i != config.thread; i++) {
        struct slowlog_queue *queue = &contexts[i].slowlog;
        for (size_t j = 0; j != queue->capacity && len < config.slowlog_max_len; j++) {
            struct slowlog_entry *entry = slowlog_get(queue, j);
            if (entry) {
                len++;
                slowlog_dec_ref(entry);
            }
        }
    }

    char buf[30];
    int size = snprintf(buf, sizeof buf, ":%d\r\n", len);
    conn_add_data(cmd->client, (uint8_t*)buf, size,
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);

    return CORVUS_OK;
}

int cmd_slowlog_reset(struct command *cmd)
{
    struct context *contexts = get_contexts();
    for (size_t i = 0; i != config.thread; i++) {
        struct slowlog_queue *queue = &contexts[i].slowlog;
        for (size_t j = 0; j != queue->capacity; j++) {
            slowlog_set(queue, NULL);
        }
    }

    conn_add_data(cmd->client, (uint8_t*)rep_ok, strlen(rep_ok),
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
    CMD_INCREF(cmd);
    cmd_mark_done(cmd);

    return CORVUS_OK;
}

int cmd_slowlog(struct command *cmd, struct redis_data *data)
{
    ASSERT_TYPE(data, REP_ARRAY);
    ASSERT_ELEMENTS(data->elements >= 2, data);

    if (!slowlog_cmd_enabled()) {
        conn_add_data(cmd->client, (uint8_t*)rep_slowlog_not_enabled,
            strlen(rep_slowlog_not_enabled),
            &cmd->rep_buf[0], &cmd->rep_buf[1]);
        CMD_INCREF(cmd);
        cmd_mark_done(cmd);
        return CORVUS_OK;
    }

    struct redis_data *op = &data->element[1];
    ASSERT_TYPE(op, REP_STRING);

    char type[op->pos.str_len + 1];
    if (pos_to_str(&op->pos, type) == CORVUS_ERR) {
        LOG(ERROR, "cmd_slowlog: parse error");
        return CORVUS_ERR;
    }

    if (strcasecmp(type, "GET") == 0) {
        return cmd_slowlog_get(cmd, data);
    } else if (strcasecmp(type, "LEN") == 0) {
        return cmd_slowlog_len(cmd);
    } else if (strcasecmp(type, "RESET") == 0) {
        return cmd_slowlog_reset(cmd);
    }
    cmd_mark_fail(cmd, rep_parse_err);
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
        case CMD_TIME:
            return cmd_time(cmd);
        case CMD_CONFIG:
            return cmd_config(cmd, data);
        case CMD_QUIT:
            return cmd_quit(cmd);
        case CMD_SLOWLOG:
            return cmd_slowlog(cmd, data);
        case CMD_SELECT:
            return cmd_select(cmd, data);
        default:
            LOG(ERROR, "%s: unknown command type %d", __func__, cmd->cmd_type);
            return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// redis命令转发逻辑, 根据不同redis操作的type, 做不同的处理方式, 核心是在cmd_forward_basic函数
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
        // 根据redis指令不同的type, 做不同的处理方式, type可参见command.h第13行
        case CMD_BASIC:
            // 处理basic类型的redis指令, 直接传给server
            cmd->slot = cmd_get_slot(data);     // 计算key所在的slot
            return cmd_forward_basic(cmd);
        case CMD_COMPLEX:
            // 处理complex类型的redis指令, 根据不同的redis命令做不同的操作
            return cmd_forward_complex(cmd, data);
        case CMD_EXTRA:
            // 处理extra类型的redis指令
            return cmd_extra(cmd, data);
        case CMD_UNIMPL:
            // 处理unimpl类型的redis指令
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

// 解析客户端发送到corvus的redis请求, 转发请求
int cmd_parse_req(struct command *cmd, struct mbuf *buf)
{
    // 构造reader
    struct reader *r = &cmd->client->info->reader;
    reader_feed(r, buf);

    // 从reader中对请求内容进行分析
    if (parse(r, MODE_REQ) == CORVUS_ERR) {
        return CORVUS_ERR;
    }

    if (reader_ready(r)) {      // 解析完毕, 把解析内容填充到command对象中
        ASSERT_TYPE(&r->data, REP_ARRAY);
        ASSERT_ELEMENTS(r->data.elements >= 1, &r->data);

        cmd->keys = r->data.elements - 1;
        cmd->parse_done = true;
        cmd->cmd_count = 1;
        cmd->data.type = REP_UNKNOWN;

        memcpy(&cmd->req_buf[0], &r->start, sizeof(r->start));
        memset(&r->start, 0, sizeof(r->start));

        memcpy(&cmd->req_buf[1], &r->end, sizeof(r->end));
        memset(&r->end, 0, sizeof(r->end));

        if (cmd_parse_token(cmd, &r->data) == CORVUS_ERR) {
            redis_data_free(&r->data);
            cmd_mark_fail(cmd, rep_parse_err);
            return CORVUS_OK;
        }
        // 转发从客户端发到corvus的redis操作
        if (cmd_forward(cmd, &r->data) == CORVUS_ERR) {
            redis_data_free(&r->data);
            cmd_mark_fail(cmd, rep_forward_err);
            return CORVUS_OK;
        }

        if (!(slowlog_cmd_enabled() && slowlog_type_need_log(cmd))) {
            redis_data_free(&r->data);
            return CORVUS_OK;
        }
        cmd->data = r->data;
        memset(&r->data, 0, sizeof(struct redis_data));
        r->data.type = REP_UNKNOWN;  // avoid double free in conn_free
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
        n = snprintf(iov->buf, sizeof(iov->buf), fmt, cmd->keys);
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
    n = snprintf(iov->buf, sizeof(iov->buf), fmt, count);
    cmd_iov_add(iov, iov->buf, n, NULL);
}

// 标记command对象的执行结果, fail=0表示成功执行, 1表示执行失败
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
        // In some cases cmd->cmd_fail is true though,
        // cmd->parent->cmd_fail is not. But since `cmd_fail`
        // implys a not null `fail_reason` (and maybe some other rules),
        // now we keep it unchanged.
        // if (fail) {
        //     cmd->parent->cmd_fail = true;
        // }
    }

    if (root != NULL && conn_register(root->client) == CORVUS_ERR) {
        LOG(ERROR, "fail to reregister client %d", root->client->fd);
        client_eof(root->client);
    }
}

// 初始化一个dict, key是redis的命令名称, value是cmd_item
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

// 构建一个command对象
struct command *cmd_create(struct context *ctx)
{
    struct command *cmd;
    // 检查free_cmdq队列是否为空
    // 1. 如果不为空, 则获取对首元素cmd
    // 2. 如果为空, 则向内存申请空间用于创建command对象
    // 3. 初始化command对象
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

// 读取redis实例返回的数据, 并根据redis协议进行解析, 把结果存到command对象中
int cmd_read_rep(struct command *cmd, struct connection *server)
{
    int rsize, status;
    struct mbuf *buf;

    while (1) {
        buf = conn_get_buf(server, true, false);    // 获取缓冲区
        rsize = mbuf_read_size(buf);        // 获取现在缓冲区的数据大小

        if (rsize <= 0) {
            // 缓冲区中没有数据, 把返回的数据从socket套接字中读取到缓冲区
            status = conn_read(server, buf);
            if (status != CORVUS_OK) return status;
        }

        // 根据redis协议, 从缓冲区解析redis返回的数据到command对象中
        if (cmd_parse_rep(cmd, buf) == CORVUS_ERR) return CORVUS_ERR;
        // 判断是否已经读取完毕
        if (reader_ready(&server->info->reader)) break;
    }

    return CORVUS_OK;
}

// 构造corvus发送到redis实例的数据
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

// 从command对象中获取redis返回值的类型, 主要是为了选出两种重定向请求以及错误:
// MOVED, ASK, CLUSTERDOWN
// 并把返回类型存储到info中
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

// 标记该command对象已经成功执行
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
    long long remote_latency, total_latency;

    ATOMIC_INC(ctx->stats.completed_commands, 1);

    total_latency = end_time - cmd->parse_time;

    ATOMIC_INC(ctx->stats.total_latency, total_latency);
    ATOMIC_SET(ctx->last_command_latency, total_latency);

    remote_latency = cmd->rep_time[1] - cmd->rep_time[0];

    if (slowlog_need_log(cmd, total_latency)) {
        if (slowlog_statsd_enabled()) {
            slowlog_add_count(cmd);
        }
        if (slowlog_cmd_enabled()) {
            struct slowlog_entry *entry = slowlog_create_entry(cmd,
                remote_latency / 1000, total_latency / 1000);
            slowlog_set(&cmd->ctx->slowlog, entry);
            entry = slowlog_create_sub_entry(cmd, total_latency / 1000);
            if (entry) {
                slowlog_set(&cmd->ctx->slowlog, entry);
            }
        }
    }

    ATOMIC_INC(ctx->stats.remote_latency, remote_latency);
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

    // iov_base存储的是指向缓冲区buf的指针, 这个buf用来存放将要发送出去的数据
    iov->data[iov->len].iov_base = buf;
    // iov_len存放的是需要发送的数据对应的长度
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
    cv_free(iov->data);
    cv_free(iov->buf_ptr);

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

    // When cmd->prefix is not NULL it's a sub command,
    // cmd->data of sub command is a weak reference
    // and should never be deallocated.
    if (cmd->data.type != REP_UNKNOWN && cmd->prefix == NULL) {
        redis_data_free(&cmd->data);
        cmd->data.type = REP_UNKNOWN;
    }

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

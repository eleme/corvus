#include <assert.h>
#include <string.h>
#include "alloc.h"
#include "corvus.h"
#include "slowlog.h"
#include "logging.h"

// use 32bit to guarantee slowlog id will not overflow in slowlog_entry.id
static uint32_t slowlog_id = 0;
static inline size_t min(size_t a, size_t b) { return a < b ? a : b; }

extern const size_t CMD_NUM;

struct dict slow_counts;  // node dsn => slow cmd counts
uint32_t *counts_sum;

const int multi_key_cmd[] = {CMD_MGET, CMD_MSET, CMD_DEL, CMD_EXISTS};
const size_t MULTI_KEY_CMD_NUM = sizeof multi_key_cmd / sizeof(int);
uint32_t multi_key_cmd_counts[4];  // for mget, mset, del, exists


int slowlog_init(struct slowlog_queue *slowlog)
{
    size_t queue_len = 1 + (config.slowlog_max_len - 1) / config.thread;  // round up
    slowlog->capacity = queue_len;
    slowlog->entries = cv_calloc(queue_len, sizeof(struct slowlog_entry));
    slowlog->entry_locks = cv_malloc(queue_len * sizeof(pthread_mutex_t));
    slowlog->curr = 0;
    int err;
    for (size_t i = 0; i != queue_len; i++) {
        if ((err = pthread_mutex_init(slowlog->entry_locks + i, NULL)) != 0) {
            LOG(ERROR, "pthread_mutex_init: %s", strerror(err));
            return CORVUS_ERR;
        }
    }
    return CORVUS_OK;
}

// Should be called only after all worker threads have stopped
void slowlog_free(struct slowlog_queue *slowlog)
{
    for (size_t i = 0; i != slowlog->capacity; i++) {
        // no other worker threads will read entry queue any more
        if (slowlog->entries[i] != NULL) {
            slowlog_dec_ref(slowlog->entries[i]);
        }
        pthread_mutex_destroy(slowlog->entry_locks + i);
    }
    cv_free(slowlog->entries);
    cv_free(slowlog->entry_locks);
}

struct slowlog_entry *slowlog_create_entry(struct command *cmd, int64_t latency)
{
    struct slowlog_entry *entry = cv_calloc(1, sizeof(struct slowlog_entry));
    entry->id = ATOMIC_INC(slowlog_id, 1);
    entry->log_time = time(NULL);  // redis also uses time(NULL);
    entry->latency = latency;
    entry->refcount = 1;

    const char *bytes_fmt = "(%zd bytes)";
    char bytes_buf[19];  // strlen("4294967295") + strlen("( bytes)") + 1

    assert(cmd->data.elements > 0);
    size_t argc = min(cmd->data.elements, SLOWLOG_ENTRY_MAX_ARGC);
    entry->argc = argc;
    size_t i = 0;

    // for sub cmd
    if (cmd->prefix) {
        const char *c = cmd_extract_prefix(cmd->prefix);
        assert(c != NULL);
        entry->argv[i].len = strlen(c);
        entry->argv[i++].str = (uint8_t*)c;
        entry->argc = min(argc + 1, SLOWLOG_ENTRY_MAX_ARGC);
        argc = min(argc, SLOWLOG_ENTRY_MAX_ARGC - 1);
    }
    // use the last argument to record total argument counts
    if (cmd->data.elements > argc) {
        argc--;
        const size_t tail_max_len = 64;
        char tail_buf[tail_max_len];
        uint8_t *buf = cv_malloc(tail_max_len);
        int len = snprintf(tail_buf, tail_max_len, "(%zd arguments in total)", cmd->data.elements);
        len = snprintf((char*)buf, tail_max_len, "$%d\r\n%s\r\n", len, tail_buf);
        entry->argv[SLOWLOG_ENTRY_MAX_ARGC - 1].str = buf;
        entry->argv[SLOWLOG_ENTRY_MAX_ARGC - 1].len = len;
    }
    size_t max_len = SLOWLOG_ENTRY_MAX_STRING;
    for (size_t j = 0; j != argc; i++, j++) {
        struct redis_data *arg = cmd->data.element + j;
        assert(arg->type == REP_STRING);
        size_t real_len = mbuf_range_len(arg->buf);
        size_t len = min(real_len, max_len);
        uint8_t *buf = cv_malloc(len);  // will be freed by the last owner
        if (real_len > max_len) {
            size_t str_len = SLOWLOG_MAX_ARG_LEN;
            int hdr_len = snprintf((char*)buf, len, "$%zd\r\n", str_len);
            pos_to_str_with_limit(&arg->pos, buf + hdr_len, str_len);
            int postfix_len = snprintf(bytes_buf, sizeof bytes_buf, bytes_fmt, arg->pos.str_len);
            memcpy(buf + max_len - 2 - postfix_len, bytes_buf, postfix_len);
            memcpy(buf + max_len - 2, "\r\n", 2);
        } else {
            mbuf_range_copy(buf, arg->buf, len);
        }
        entry->argv[i].str = buf;
        entry->argv[i].len = len;
    }

    return entry;
}

void slowlog_dec_ref(struct slowlog_entry *entry)
{
    int refcount = ATOMIC_DEC(entry->refcount, 1);
    assert(refcount >= 0);
    if (refcount > 0) return;

    for (size_t i = 0; i != entry->argc; i++) {
        uint8_t *buf = entry->argv[i].str;
        cv_free(buf);
    }
    cv_free(entry);
}

void slowlog_set(struct slowlog_queue *queue, struct slowlog_entry *entry)
{
    size_t curr = ATOMIC_GET(queue->curr);
    pthread_mutex_lock(queue->entry_locks + curr);
    struct slowlog_entry *old_entry = queue->entries[curr];
    queue->entries[curr] = entry;
    pthread_mutex_unlock(queue->entry_locks + curr);
    ATOMIC_SET(queue->curr, (curr + 1) % queue->capacity);
    if (old_entry != NULL) {
        slowlog_dec_ref(old_entry);
    }
}

struct slowlog_entry *slowlog_get(struct slowlog_queue *queue, size_t index)
{
    pthread_mutex_lock(queue->entry_locks + index);
    struct slowlog_entry *entry = queue->entries[index];
    if (entry == NULL) {
        pthread_mutex_unlock(queue->entry_locks + index);
        return NULL;
    }
    ATOMIC_INC(entry->refcount, 1);
    pthread_mutex_unlock(queue->entry_locks + index);
    return entry;
}

bool slowlog_cmd_enabled()
{
    return config.slowlog_max_len > 0
        && config.slowlog_log_slower_than >= 0;
}

bool slowlog_statsd_enabled()
{
    return config.slowlog_log_slower_than >= 0
        && config.slowlog_statsd_enabled;
}

bool slowlog_type_need_log(struct command *cmd)
{
    return cmd->request_type != CMD_EXTRA
        && cmd->request_type != CMD_UNIMPL
        && cmd->reply_type != REP_ERROR;
}

bool slowlog_need_log(struct command *cmd, long long latency)
{
    return config.slowlog_log_slower_than >= 0
        && slowlog_type_need_log(cmd)
        && latency > config.slowlog_log_slower_than * 1000;
}

void slowlog_init_stats()
{
    dict_init(&slow_counts);
    counts_sum = cv_calloc(CMD_NUM, sizeof(uint32_t));
}

void slowlog_free_stats()
{
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slow_counts, &iter) {
        cv_free(iter.value);
    }

    dict_free(&slow_counts);
    cv_free(counts_sum);
}

static uint32_t *slowlog_multi_key_cmd_count(int cmd_type)
{
    switch (cmd_type) {
        case CMD_MGET: return &multi_key_cmd_counts[0];
        case CMD_MSET: return &multi_key_cmd_counts[1];
        case CMD_DEL: return &multi_key_cmd_counts[2];
        case CMD_EXISTS: return &multi_key_cmd_counts[3];
        default: return NULL;
    }
}

void slowlog_add_count(struct command *cmd)
{
    // Since serving multiple keys command is usually achieved by multiple redis server
    // and they don't have a corresponding server(cmd->server),
    // we only record the total count of them.
    uint32_t *count = slowlog_multi_key_cmd_count(cmd->cmd_type);
    if (count) {
        ATOMIC_INC(*count, 1);
    } else {
        assert(cmd->server != NULL);
        ATOMIC_INC(cmd->server->info->slow_cmd_counts[cmd->cmd_type], 1);
    }
}

// Collect slow command count into `slow_counts` and `counts_sum`.
void slowlog_prepare_stats(struct context *contexts)
{
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slow_counts, &iter) {
        memset(iter.value, 0, CMD_NUM * sizeof(uint32_t));
    }
    memset(counts_sum, 0, CMD_NUM * sizeof(uint32_t));

    struct connection *server;

    for (size_t i = 0; i != config.thread; i++) {
        TAILQ_FOREACH(server, &contexts[i].servers, next) {
            // Note that server will not be freed until corvus stop,
            // so we don't need to copy dsn.
            const char *dsn = server->info->dsn;
            uint32_t *node_counts = NULL;
            for (size_t j = 0; j != CMD_NUM; j++) {
                uint32_t count = ATOMIC_IGET(server->info->slow_cmd_counts[j], 0);
                if (count == 0) continue;

                if (!node_counts) {
                    node_counts = (uint32_t*)dict_get(&slow_counts, dsn);
                    if (!node_counts) {
                        node_counts = cv_calloc(CMD_NUM, sizeof(uint32_t));
                        dict_set(&slow_counts, dsn, node_counts);
                    }
                }
                node_counts[j] += count;
                counts_sum[j] += count;
            }
        }
    }

    for (size_t i = 0; i != MULTI_KEY_CMD_NUM; i++) {
        int cmd_type = multi_key_cmd[i];
        uint32_t *count = slowlog_multi_key_cmd_count(cmd_type);
        counts_sum[cmd_type] = ATOMIC_IGET(*count, 0);
    }
}

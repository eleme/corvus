#include <assert.h>
#include <string.h>
#include "alloc.h"
#include "corvus.h"
#include "slowlog.h"
#include "logging.h"

// use 32bit to guarantee slowlog id will not overflow in slowlog_entry.id
static uint32_t slowlog_id = 0;
static inline size_t min(size_t a, size_t b) { return a < b ? a : b; }


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

bool slowlog_enabled()
{
    return config.slowlog_max_len > 0
        && config.slowlog_log_slower_than >= 0;
}

bool slowlog_type_need_log(struct command *cmd)
{
    return cmd->request_type != CMD_EXTRA
        && cmd->request_type != CMD_UNIMPL;
}

bool slowlog_need_log(struct command *cmd, long long latency)
{
    return slowlog_enabled()
        && slowlog_type_need_log(cmd)
        && latency > config.slowlog_log_slower_than * 1000;
}

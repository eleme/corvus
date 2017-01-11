#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/queue.h>
#include <pthread.h>
#include <errno.h>
#include "corvus.h"
#include "hash.h"
#include "slot.h"
#include "socket.h"
#include "logging.h"
#include "alloc.h"
#include "stats.h"

static const char SLOTS_CMD[] = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";

static int8_t in_progress = 0;
static pthread_mutex_t job_mutex;
static pthread_cond_t signal_cond;

static int slot_job = SLOT_UPDATE_UNKNOWN;

static struct {
    pthread_rwlock_t lock;
    struct node_info *data[REDIS_CLUSTER_SLOTS];
    struct dict free_nodes;
    struct dict node_map;
} slot_map = {.lock = PTHREAD_RWLOCK_INITIALIZER};

static struct {
    pthread_rwlock_t lock;
    struct address nodes[MAX_NODE_LIST];
    int len;
} node_list = {.lock = PTHREAD_RWLOCK_INITIALIZER};

static inline void node_list_init()
{
    struct node_conf *node = config_get_node();
    pthread_rwlock_wrlock(&node_list.lock);
    node_list.len = 0;
    for (int i = 0; i < MIN(node->len, MAX_NODE_LIST); i++) {
        memcpy(&node_list.nodes[i], &node->addr[i], sizeof(struct address));
        node_list.len++;
    }
    pthread_rwlock_unlock(&node_list.lock);
    config_node_dec_ref(node);
}

static inline void node_list_add(struct node_info *node)
{
    pthread_rwlock_wrlock(&node_list.lock);
    for (size_t i = 0; i < node->index && node_list.len < MAX_NODE_LIST; i++) {
        memcpy(&node_list.nodes[node_list.len++], &node->nodes[i],
                sizeof(struct address));
    }
    pthread_rwlock_unlock(&node_list.lock);
}

static inline void node_map_free(struct dict *map)
{
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(map, &iter) {
        struct node_info *n = iter.value;
        if (n->refcount <= 0) {
            cv_free(iter.value);
        }
    }
}

void node_desc_add(struct node_desc *b, uint8_t *start, uint8_t *end, bool partial)
{
    if (b->len <= b->index) {
        b->len = (b->len <= 0) ? 16 : b->len << 1;
        b->parts = cv_realloc(b->parts, sizeof(struct desc_part) * b->len);
        memset(b->parts + b->index, 0,
                sizeof(struct desc_part) * (b->len - b->index));
    }
    struct desc_part *d = &b->parts[b->index];
    memcpy(d->data + d->len, start, end - start);
    d->len += end - start;
    if (!partial) {
        b->index++;
    }
}

int split_node_description(struct node_desc *desc, struct pos_array *pos_array)
{
    int i, desc_count = 0;
    for(i = 0; i < pos_array->pos_len; i++) {
        struct pos *pos = &pos_array->items[i];
        uint8_t *p = pos->str, *s = p;
        while (p - pos->str < pos->len) {
            if (*p == ' ' || *p == '\n') {
                node_desc_add(&desc[desc_count], s, p, false);
                s = p + 1;
            }
            if (*(p++) == '\n') {
                desc_count++;
            }
        }
        if (p > s) {
            node_desc_add(&desc[desc_count], s, p, true);
        }
    }
    return desc_count;
}

int parse_slots()
{
    char *p;
    int start, stop, slot_count = 0;
    struct node_info *node;

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slot_map.node_map, &iter) {
        struct node_info *n = iter.value;

        // the node has no slots
        if (n->spec_length <= 0) {
            cv_free(n);
            continue;
        }

        node_list_add(n);

        for (int i = 8; i < n->spec_length + 8; i++) {
            if (n->slot_spec[i].data[0] == '[') {
                continue;
            }
            if ((p = strchr(n->slot_spec[i].data, '-')) != NULL) {
                *p = '\0';
                start = atoi(n->slot_spec[i].data);
                stop = atoi(p + 1);
            } else {
                start = stop = atoi(n->slot_spec[i].data);
            }
            n->refcount += stop - start + 1;
            while (start <= stop) {
                slot_count++;
                node = ATOMIC_IGET(slot_map.data[start++], n);
                if (node == NULL) {
                    continue;
                }
                node->refcount--;
                snprintf(node->name, sizeof(node->name), "%p", node);
                struct node_info *m = dict_get(&slot_map.free_nodes, node->name);
                if (m == NULL) {
                    dict_set(&slot_map.free_nodes, node->name, node);
                }
            }
        }
        n->spec_length = 0;
        n->slot_spec = NULL;
    }

    pthread_rwlock_wrlock(&slot_map.lock);
    node_map_free(&slot_map.free_nodes);
    pthread_rwlock_unlock(&slot_map.lock);

    dict_clear(&slot_map.free_nodes);
    return slot_count;
}

int parse_cluster_nodes(struct redis_data *data)
{
    ASSERT_TYPE(data, REP_STRING);

    struct node_desc desc[REDIS_CLUSTER_SLOTS];
    memset(desc, 0, sizeof(desc));
    int slot_count = 0, node_count = split_node_description(desc, &data->pos);
    if (node_count <= 0) {
        LOG(ERROR, "fail to parse cluster nodes infomation");
        goto end;
    }

    for (int i = 0; i < node_count; i++) {
        struct node_desc *d = &desc[i];
        if (d->index < 8) {
            node_map_free(&slot_map.node_map);
            goto end;
        }
        bool is_master = d->parts[3].data[0] == '-';
        char *name = is_master ? d->parts[0].data : d->parts[3].data;
        struct node_info *node = dict_get(&slot_map.node_map, name);
        if (node == NULL) {
            node = cv_calloc(1, sizeof(struct node_info));
            node->index = 1;
            strcpy(node->name, name);
            dict_set(&slot_map.node_map, node->name, node);
        }
        if (is_master) {
            socket_parse_addr(d->parts[1].data, &node->nodes[0]);
            node->slot_spec = d->parts;
            node->spec_length = d->index - 8;
        } else if (node->index <= MAX_SLAVE_NODES && (
                    strcasecmp(d->parts[2].data, "slave") == 0 ||
                    strcasecmp(d->parts[2].data, "myself,slave") == 0)) {
            socket_parse_addr(d->parts[1].data, &node->nodes[node->index++]);
        }
    }

    slot_count = parse_slots();

end:
    dict_clear(&slot_map.node_map);
    for (int i = 0; i < node_count; i++) {
        cv_free(desc[i].parts);
    }
    return slot_count;
}

int slot_parse_data(struct reader *r, struct mbuf *buf, int *count)
{
    reader_feed(r, buf);
    if (parse(r, MODE_REQ) == -1) {
        LOG(ERROR, "slot_map: parse cluster slots error");
        return CORVUS_ERR;
    }
    if (reader_ready(r)) {
        *count = parse_cluster_nodes(&r->data);
        redis_data_free(&r->data);
    }
    return CORVUS_OK;
}

int slot_read_data(struct connection *server, int *count)
{
    int n, rsize;
    struct mbuf *buf;
    struct reader *r = &server->info->reader;

    while (1) {
        buf = conn_get_buf(server, true, false);
        rsize = mbuf_read_size(buf);

        if (rsize > 0) {
            if (slot_parse_data(r, buf, count) == CORVUS_ERR) return CORVUS_ERR;
            if (reader_ready(r)) return CORVUS_OK;
            continue;
        }

        n = socket_read(server->fd, buf);
        if (n == 0 || n == CORVUS_ERR || n == CORVUS_AGAIN) return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int do_update_slot_map(struct connection *server)
{
    if (conn_connect(server) == CORVUS_ERR) return CORVUS_ERR;
    if (server->info->status != CONNECTED) {
        LOG(ERROR, "update slot map, server not connected: %s", strerror(errno));
        return CORVUS_ERR;
    }

    struct iovec iov;
    iov.iov_base = (void*)SLOTS_CMD;
    iov.iov_len = strlen(SLOTS_CMD);

    if (socket_write(server->fd, &iov, 1) <= 0) {
        LOG(ERROR, "update slot map, socket write: %s", strerror(errno));
        return CORVUS_ERR;
    }

    int count = -1;
    LOG(INFO, "updating slot map using %s:%d",
            server->info->addr.ip, server->info->addr.port);
    if(slot_read_data(server, &count) != CORVUS_OK) {
        LOG(ERROR, "update slot map, cmd read error");
        return CORVUS_ERR;
    }
    return count;
}

void slot_map_update(struct context *ctx)
{
    int i, count = 0;
    struct address node;

    struct connection *server = conn_create(ctx);
    server->info = conn_info_create(ctx);

    if (node_list.len <= 0) {
        node_list_init();
    }

    struct address nodes[MAX_NODE_LIST];
    memcpy(nodes, node_list.nodes, sizeof(node_list.nodes));
    int len = node_list.len;
    node_list.len = 0;

    for (i = len; i > 0; i--) {
        int r = rand_r(&ctx->seed) % i;
        node = nodes[r];
        nodes[r] = nodes[i - 1];

        server->fd = socket_create_stream();
        if (server->fd == -1) continue;

        if (socket_set_timeout(server->fd, 5) == CORVUS_ERR) {
            close(server->fd);
            continue;
        }

        memcpy(&server->info->addr, &node, sizeof(struct address));

        count = do_update_slot_map(server);
        if (count < REDIS_CLUSTER_SLOTS) {
            conn_free(server);
            conn_buf_free(server);
            continue;
        }
        break;
    }
    conn_free(server);
    conn_buf_free(server);
    conn_recycle(ctx, server);

    if (count == CORVUS_ERR) {
        LOG(WARN, "can not update slot map");
    } else {
        LOG(INFO, "slot map updated: corverd %d slots", count);
    }
}

void do_job(struct context *ctx, int job)
{
    switch (job) {
        case SLOT_UPDATE:
            slot_map_update(ctx);
            break;
        case SLOT_UPDATER_QUIT:
            ctx->state = CTX_QUIT;
            break;
    }
}

void *slot_manager(void *data)
{
    int job;
    struct context *ctx = data;

    pthread_mutex_lock(&job_mutex);
    in_progress = 0;

    while (ctx->state != CTX_QUIT) {
        if (slot_job == SLOT_UPDATE_UNKNOWN) {
            pthread_cond_wait(&signal_cond, &job_mutex);
            continue;
        }

        job = slot_job;
        slot_job = SLOT_UPDATE_UNKNOWN;

        pthread_mutex_unlock(&job_mutex);

        do_job(ctx, job);
        usleep(100000);

        pthread_mutex_lock(&job_mutex);
        in_progress = 0;
    }
    pthread_mutex_unlock(&job_mutex);

    for (int i = 0; i < REDIS_CLUSTER_SLOTS; i++) {
        struct node_info *n = ATOMIC_IGET(slot_map.data[i], NULL);
        if (n == NULL) {
            continue;
        }
        n->refcount--;
        if (n->refcount <= 0) {
            cv_free(n);
        }
    }
    dict_free(&slot_map.free_nodes);
    dict_free(&slot_map.node_map);

    pthread_rwlock_destroy(&node_list.lock);
    pthread_rwlock_destroy(&slot_map.lock);
    pthread_mutex_destroy(&job_mutex);

    LOG(DEBUG, "slot map update thread quiting");
    return NULL;
}

uint16_t slot_get(struct pos_array *pos)
{
    uint32_t s, len;
    uint8_t *str;
    int j, h, found = 0, found_s = 0;
    struct pos *q, *p;

    struct pos temp_pos[pos->pos_len];
    struct pos_array arr;
    memset(&arr, 0, sizeof(arr));
    arr.items = temp_pos;

    for (j = 0, h = 0; h < pos->pos_len; h++) {
        p = &pos->items[h];
        len = p->len;
        str = p->str;

        q = &arr.items[j];
        q->len = len;
        q->str = str;

        for (s = 0; s < len; s++) {
            if (str[s] == '}' && found_s) {
                found = 1;
                q->len -= len - s;
                break;
            }

            if (str[s] == '{' && !found_s) {
                if (s == len - 1) {
                    if (h + 1 >= pos->pos_len) goto end;
                    if ((p + 1)->len <= 0 || (p + 1)->str[0] == '}') goto end;
                    found_s = 1;
                    j = -1;
                    arr.pos_len = -1;
                } else {
                    q->str += s + 1;
                    q->len -= s + 1;
                    if (q->str[0] == '}') goto end;
                    found_s = 1;
                }
            }
        }

        if (found_s) {
            j++;
            arr.pos_len++;
        }
        if (found) break;
    }
end:
    if (found) pos = &arr;
    return crc16(pos) & 0x3FFF;
}

bool slot_get_node_addr(uint16_t slot, struct node_info *info)
{
    bool hit = false;

    pthread_rwlock_rdlock(&slot_map.lock);
    struct node_info *n = ATOMIC_GET(slot_map.data[slot]);
    if (n != NULL) {
        hit = true;
        memcpy(info, n, sizeof(struct node_info));
    }
    pthread_rwlock_unlock(&slot_map.lock);

    return hit;
}

void node_list_get(char *dest)
{
    int i, pos = 0;
    pthread_rwlock_rdlock(&node_list.lock);
    for (i = 0; i < node_list.len; i++) {
        if (i > 0) {
            dest[pos++] = ',';
        }
        pos += snprintf(dest + pos, ADDRESS_LEN, "%s:%d",
                node_list.nodes[i].ip, node_list.nodes[i].port);
    }
    pthread_rwlock_unlock(&node_list.lock);
}

void slot_create_job(int type)
{
    incr_slot_update_counter();
    pthread_mutex_lock(&job_mutex);
    if (!in_progress || type == SLOT_UPDATER_QUIT) {
        in_progress = 1;
        slot_job = type;
        pthread_cond_signal(&signal_cond);
    }
    pthread_mutex_unlock(&job_mutex);
}

int slot_start_manager(struct context *ctx)
{
    int err;
    memset(slot_map.data, 0, sizeof(struct node_info*) * REDIS_CLUSTER_SLOTS);
    dict_init(&slot_map.free_nodes);
    dict_init(&slot_map.node_map);
    node_list_init();

    if ((err = pthread_mutex_init(&job_mutex, NULL)) != 0) {
        LOG(ERROR, "pthread_mutex_init: %s", strerror(err));
        return CORVUS_ERR;
    }
    if ((err = pthread_cond_init(&signal_cond, NULL)) != 0) {
        LOG(ERROR, "pthread_cond_init: %s", strerror(err));
        return CORVUS_ERR;
    }

    LOG(INFO, "starting slot manager thread");
    return thread_spawn(ctx, slot_manager);
}

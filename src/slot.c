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

#define MAX_UPDATE_NODES 16
#define SLAVE_NODES 4

static struct node_info *slot_map[REDIS_CLUSTER_SLOTS];
static const char SLOTS_CMD[] = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";

static int8_t in_progress = 0;
static pthread_mutex_t job_mutex;
static pthread_cond_t signal_cond;

static pthread_rwlock_t slot_map_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t addr_list_lock = PTHREAD_RWLOCK_INITIALIZER;

static int slot_job = SLOT_UPDATE_UNKNOWN;

static struct {
    struct address nodes[MAX_UPDATE_NODES];
    int len;
} node_list;

static struct {
    char addrs[ADDR_LIST_MAX];
    int bytes;
} addr_list;

static struct {
    struct node_info nodes[REDIS_CLUSTER_SLOTS];
    struct dict map;
    int idx;
} node_store;

static inline void node_list_init()
{
    memset(&node_list, 0, sizeof(node_list));
    for (int i = 0; i < MIN(config.node.len, MAX_UPDATE_NODES); i++) {
        memcpy(&node_list.nodes[i], &config.node.addr[i], sizeof(struct address));
        node_list.len++;
    }
}

void slot_map_clear()
{
    pthread_rwlock_wrlock(&slot_map_lock);
    memset(slot_map, 0, sizeof(slot_map));
    pthread_rwlock_unlock(&slot_map_lock);

    pthread_rwlock_wrlock(&addr_list_lock);
    memset(&addr_list, 0, sizeof(addr_list));
    pthread_rwlock_unlock(&addr_list_lock);

    struct node_info *node_info;

    node_list.len = 0;

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&node_store.map, &iter) {
        node_info = (struct node_info *)iter.value;
        if (node_list.len >= MAX_UPDATE_NODES) break;
        memcpy(&node_list.nodes[node_list.len++], &node_info->nodes[0], sizeof(struct address));
    }
    node_store.idx = 0;
    dict_clear(&node_store.map);

    if (node_list.len <= 0) node_list_init();
}

void addr_add(char *ip, uint16_t port)
{
    int n;
    pthread_rwlock_wrlock(&addr_list_lock);
    if (addr_list.bytes > 0) {
        if (addr_list.bytes + DSN_LEN + 1 <= ADDR_LIST_MAX) {
            addr_list.addrs[addr_list.bytes++] = ',';
        }
    }
    if (ADDR_LIST_MAX - addr_list.bytes >= DSN_LEN) {
        n = snprintf(addr_list.addrs + addr_list.bytes, DSN_LEN, "%s:%d", ip, port);
        addr_list.bytes += n;
    }
    pthread_rwlock_unlock(&addr_list_lock);
}

struct node_info *node_info_get()
{
    if (node_store.idx >= REDIS_CLUSTER_SLOTS) {
        return NULL;
    }

    struct node_info *n = &node_store.nodes[node_store.idx++];
    n->index = 1;
    return n;
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

int parse_slots(struct dict *node_map)
{
    char *p;
    int start, stop, slot_count = 0;

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(node_map, &iter) {
        struct node_info *n = iter.value;
        for (size_t i = 0; i < n->index; i++) {
            addr_add(n->nodes[i].ip, n->nodes[i].port);
        }

        for (int i = 0; i < n->spec_length; i++) {
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
            while (start <= stop) {
                slot_count++;
                slot_map[start++] = n;
            }
        }
    }
    return slot_count;
}

int parse_cluster_nodes(struct redis_data *data)
{
    ASSERT_TYPE(data, REP_STRING);

    struct dict node_map;
    dict_init(&node_map);

    struct node_desc desc[REDIS_CLUSTER_SLOTS];
    memset(desc, 0, sizeof(desc));
    int slot_count = 0, node_count = split_node_description(desc, &data->pos);
    if (node_count <= 0) {
        LOG(ERROR, "fail to parse cluster nodes infomation");
        goto end;
    }

    for (int i = 0; i < node_count; i++) {
        struct node_desc *d = &desc[i];
        if (d->index <= 0) {
            goto end;
        }
        if (strcasecmp(d->parts[0].data, "vars") == 0) {
            continue;
        }
        if (d->index < 8) {
            goto end;
        }
        bool is_master = d->parts[3].data[0] == '-';
        char *name = is_master ? d->parts[0].data : d->parts[3].data;
        struct node_info *node = dict_get(&node_map, name);
        if (node == NULL) {
            node = node_info_get();
            if (node == NULL) {
                goto end;
            }
            strcpy(node->name, name);
            dict_set(&node_map, node->name, node);
        }
        if (node->len <= node->index) {
            node->len = (node->len == 0) ? 4 : node->len << 1;
            node->nodes = cv_realloc(node->nodes, node->len * sizeof(struct address));
        }
        socket_parse_addr(d->parts[1].data, &node->nodes[is_master ? 0 : node->index++]);
        if (is_master) {
            node->slot_spec = &d->parts[8];
            node->spec_length = d->index - 8;
        }
    }

    slot_count = parse_slots(&node_map);

end:
    dict_free(&node_map);
    for (int i = 0; i < desc->index; i++) {
        cv_free(desc[i].parts);
    }
    return slot_count;
}

int slot_parse_data(struct reader *r, struct mbuf *buf, int *count)
{
    reader_feed(r, buf);
    while (buf->pos < buf->last) {
        if (parse(r, MODE_REQ) == -1) {
            LOG(ERROR, "slot_map: parse cluster slots error");
            return CORVUS_ERR;
        }

        if (reader_ready(r)) {
            *count = parse_cluster_nodes(&r->data);
            redis_data_free(&r->data);
            break;
        }
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
    struct address *node;

    struct connection *server = conn_create(ctx);
    server->info = conn_info_create(ctx);

    for (i = 0; i < node_list.len; i++) {
        node = &node_list.nodes[i];
        server->fd = socket_create_stream();
        if (server->fd == -1) continue;

        if (socket_set_timeout(server->fd, 5) == CORVUS_ERR) {
            close(server->fd);
            continue;
        }

        memcpy(&server->info->addr, node, sizeof(struct address));

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
    slot_map_clear();

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

    dict_free(&node_store.map);
    for (int i = 0; i < REDIS_CLUSTER_SLOTS; i++) {
        cv_free(node_store.nodes[i].nodes);
    }

    pthread_rwlock_destroy(&slot_map_lock);
    pthread_rwlock_destroy(&addr_list_lock);
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

bool slot_get_node_addr(struct context *ctx, uint16_t slot, struct address *addr,
        struct address *slave)
{
    bool res = false;
    struct node_info *info;
    pthread_rwlock_rdlock(&slot_map_lock);
    info = slot_map[slot];
    if (info != NULL) {
        memcpy(addr, &info->nodes[0], sizeof(struct address));
        if (config.readslave && info->index > 1) {
            int r = rand_r(&ctx->seed);
            if (!config.readmasterslave || r % info->index != 0) {
                int i = r % (info->index - 1);
                memcpy(slave, &info->nodes[++i], sizeof(struct address));
            }
        }
        res = true;
    }
    pthread_rwlock_unlock(&slot_map_lock);
    return res;
}

void slot_get_addr_list(char *dest)
{
    pthread_rwlock_rdlock(&addr_list_lock);
    memcpy(dest, addr_list.addrs, addr_list.bytes + 1);
    pthread_rwlock_unlock(&addr_list_lock);
}

void slot_create_job(int type)
{
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
    memset(slot_map, 0, sizeof(struct node_info*) * REDIS_CLUSTER_SLOTS);
    memset(&node_store, 0, sizeof(node_store));
    dict_init(&node_store.map);
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

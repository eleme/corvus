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

#define MAX_UPDATE_NODES 16

struct map_item {
    uint32_t hash;
    struct node_info *node;
};

enum {
    SLOT_MAP_NEW,
    SLOT_MAP_DIRTY,
};

extern void context_free(struct context *ctx);

static struct node_info *slot_map[REDIS_CLUSTER_SLOTS];
static const char SLOTS_CMD[] = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";

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

void node_list_init()
{
    memset(&node_list, 0, sizeof(node_list));
    for (int i = 0; i < config.node.len; i++, node_list.len++) {
        if (node_list.len >= MAX_UPDATE_NODES) break;
        memcpy(&node_list.nodes[i], &config.node.addr[i], sizeof(struct address));
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
        memcpy(&node_list.nodes[node_list.len++], &node_info->master, sizeof(struct address));
    }
    node_store.idx = 0;
    dict_clear(&node_store.map);
    memset(node_store.nodes, 0, sizeof(node_store.nodes));

    if (node_list.len <= 0) node_list_init();
}

void addr_add(char *host, uint16_t port)
{
    int n;
    pthread_rwlock_wrlock(&addr_list_lock);
    if (addr_list.bytes > 0) {
        if (addr_list.bytes + DSN_MAX + 1 <= ADDR_LIST_MAX) {
            addr_list.addrs[addr_list.bytes++] = ',';
        }
    }
    if (ADDR_LIST_MAX - addr_list.bytes >= DSN_MAX) {
        n = snprintf(addr_list.addrs + addr_list.bytes, DSN_MAX, "%s:%d", host, port);
        addr_list.bytes += n;
    }
    pthread_rwlock_unlock(&addr_list_lock);
}

struct node_info *node_info_get(struct address *addr)
{
    if (node_store.idx >= REDIS_CLUSTER_SLOTS) return NULL;
    struct node_info *node = &node_store.nodes[node_store.idx++];
    memset(node, 0, sizeof(struct node_info));

    addr_add(addr->host, addr->port);

    memcpy(&node->master, addr, sizeof(struct address));
    return node;
}

struct node_info *node_info_find(struct redis_data *data)
{
    struct node_info *node = NULL;

    if (data->elements != 2) return NULL;

    struct pos_array *p = &data->element[0].pos;
    if (p->str_len <= 0) return NULL;

    char hostname[p->str_len + 1];
    if (pos_to_str(p, hostname) == CORVUS_ERR) return NULL;

    uint16_t port = data->element[1].integer;

    struct address addr;
    socket_address_init(&addr, hostname, p->str_len, port);

    char key[DSN_MAX];
    socket_get_key(&addr, key);

    node = dict_get(&node_store.map, key);
    if (node == NULL) {
        node = node_info_get(&addr);
        if (node == NULL) return NULL;
        dict_set(&node_store.map, key, node);
    }
    return node;
}

int node_info_add_addr(struct redis_data *data)
{
    if (data->elements != 2) return -1;

    struct pos_array *p = &data->element[0].pos;
    if (p->str_len <= 0) return -1;

    char hostname[p->str_len + 1];
    if (pos_to_str(p, hostname) == CORVUS_ERR) return -1;

    uint16_t port = data->element[1].integer;
    struct address addr;
    socket_address_init(&addr, hostname, p->str_len, port);

    addr_add(addr.host, addr.port);
    return 0;
}

int parse_slots(struct redis_data *data)
{
    size_t i, h;
    long long j;
    int count = 0;
    struct redis_data *d;
    struct node_info *node;

    if (data->elements <= 0) return -1;

    for (i = 0; i < data->elements; i++) {
        d = &data->element[i];

        if (d->elements < 3) return -1;

        node = node_info_find(&d->element[2]);
        if (node == NULL) return -1;

        for (j = d->element[0].integer; j < d->element[1].integer + 1; j++) {
            count++;
            slot_map[j] = node;
        }

        if (!node->dsn_added && d->elements - 3 > 0) {
            for (h = 3; h < d->elements; h++) {
                if (node_info_add_addr(&d->element[h]) == -1) return -1;
            }
            node->dsn_added = 1;
        }
    }

    return count;
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
            *count = parse_slots(&r->data);
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
    struct reader r;
    reader_init(&r);

    while (1) {
        buf = conn_get_buf(server);
        rsize = mbuf_read_size(buf);

        if (rsize > 0) {
            if (slot_parse_data(&r, buf, count) == CORVUS_ERR) return CORVUS_ERR;
            if (reader_ready(&r)) return CORVUS_OK;
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
    if (server->status != CONNECTED) {
        LOG(ERROR, "update slot map, server not connected: %s", strerror(errno));
        return -1;
    }

    struct iovec iov;
    iov.iov_base = (void*)SLOTS_CMD;
    iov.iov_len = strlen(SLOTS_CMD);

    if (socket_write(server->fd, &iov, 1) <= 0) {
        LOG(ERROR, "update slot map, socket write: %s", strerror(errno));
        return -1;
    }

    int count = -1;
    LOG(INFO, "updating slot map using %s:%d", server->addr.host, server->addr.port);
    if(slot_read_data(server, &count) != CORVUS_OK) {
        LOG(ERROR, "update slot map, cmd read error");
        return -1;
    }
    return count;
}

void slot_map_update(struct context *ctx)
{
    int i, count = 0;
    struct address *node;
    struct connection server;

    conn_init(&server, ctx);

    for (i = 0; i < node_list.len; i++) {
        node = &node_list.nodes[i];
        server.fd = socket_create_stream();
        if (server.fd == -1) continue;

        if (socket_set_timeout(server.fd, 5) == CORVUS_ERR) {
            close(server.fd);
            continue;
        }

        memcpy(&server.addr, node, sizeof(struct address));

        count = do_update_slot_map(&server);
        if (count < REDIS_CLUSTER_SLOTS) {
            conn_free(&server);
            conn_buf_free(&server);
            continue;
        }
        break;
    }
    conn_free(&server);
    conn_buf_free(&server);

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

void *slot_map_updater(void *data)
{
    int job;
    struct context *ctx = data;

    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

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

    pthread_rwlock_destroy(&slot_map_lock);
    pthread_rwlock_destroy(&addr_list_lock);
    pthread_mutex_destroy(&job_mutex);
    context_free(ctx);

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

int slot_get_node_addr(uint16_t slot, struct address *addr)
{
    int res = 0;
    struct node_info *info;
    pthread_rwlock_rdlock(&slot_map_lock);
    info = slot_map[slot];
    if (info != NULL) {
        memcpy(addr, &info->master, sizeof(struct address));
        res = 1;
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

int slot_init_updater(struct context *ctx)
{
    pthread_t thread;
    pthread_attr_t attr;
    size_t stacksize;

    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    /* Set the stack size as by default it may be small in some system */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    memset(slot_map, 0, sizeof(struct node_info*) * REDIS_CLUSTER_SLOTS);
    memset(&node_store, 0, sizeof(node_store));
    dict_init(&node_store.map);

    node_list_init();

    pthread_mutex_init(&job_mutex, NULL);
    pthread_cond_init(&signal_cond, NULL);

    if (pthread_create(&thread, &attr, slot_map_updater, (void*)ctx) != 0) {
        LOG(ERROR, "can't initialize slot updating thread");
        return -1;
    }
    LOG(INFO, "starting slot updating thread");

    ctx->thread = thread;
    ctx->started = true;
    ctx->role = THREAD_SLOT_UPDATER;
    return 0;
}

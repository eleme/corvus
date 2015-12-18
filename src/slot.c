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

static int slot_map_status;
static pthread_mutex_t job_mutex;
static pthread_cond_t signal_cond;

static pthread_rwlock_t slot_map_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t addr_list_lock = PTHREAD_RWLOCK_INITIALIZER;

static int slot_job = SLOT_UPDATE_UNKNOWN;

static struct {
    struct node_info nodes[MAX_UPDATE_NODES];
    int idx;
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
    socket_get_addr(hostname, p->str_len, port, &addr);

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
    socket_get_addr(hostname, p->str_len, port, &addr);

    addr_add(addr.host, addr.port);
    return 0;
}

int parse_slots_data(struct redis_data *data)
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

void slot_map_clear()
{
    pthread_rwlock_wrlock(&slot_map_lock);
    memset(slot_map, 0, sizeof(slot_map));
    pthread_rwlock_unlock(&slot_map_lock);

    pthread_rwlock_wrlock(&addr_list_lock);
    memset(&addr_list, 0, sizeof(addr_list));
    pthread_rwlock_unlock(&addr_list_lock);

    struct node_info *node_info;

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&node_store.map, &iter) {
        node_info = (struct node_info *)iter.value;
        if (node_list.idx >= MAX_UPDATE_NODES) break;
        memcpy(&node_list.nodes[node_list.idx++], node_info, sizeof(struct node_info));
    }
    node_store.idx = 0;
    dict_clear(&node_store.map);
    memset(node_store.nodes, 0, sizeof(node_store.nodes));
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

    struct command *cmd = cmd_create(server->ctx);
    cmd->server = server;
    if(cmd_read_reply(cmd, server) != CORVUS_OK) {
        LOG(ERROR, "update slot map, cmd read error");
        cmd_free(cmd);
        return -1;
    }

    LOG(INFO, "updating slot map using %s:%d", server->addr.host, server->addr.port);
    int count = parse_slots_data(&cmd->rep_data);
    cmd_free(cmd);
    mbuf_destroy(server->ctx);
    return count;
}

void slot_map_init(struct context *ctx)
{
    int i, port, count = 0;
    char *addr;
    struct connection server;
    struct address address;
    struct node_conf *conf = ctx->node_conf;

    conn_init(&server, ctx);

    for (i = 0; i < conf->len; i++) {
        addr = conf->nodes[i];
        port = socket_parse_addr(addr, &address);
        if (port == -1) continue;

        server.fd = socket_create_stream();
        if (server.fd == -1) continue;

        if (socket_set_timeout(server.fd, 5) == CORVUS_ERR) {
            close(server.fd);
            continue;
        }

        memcpy(&server.addr, &address, sizeof(struct address));
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
        LOG(WARN, "can not init slot map");
    } else {
        LOG(INFO, "slot map inited: covered %d slots", count);
    }
}

void slot_map_update(struct context *ctx)
{
    int i, count = 0;
    struct node_info *node;
    struct connection server;

    conn_init(&server, ctx);

    for (i = 0; i < node_list.idx; i++) {
        node = &node_list.nodes[i];
        server.fd = socket_create_stream();
        if (server.fd == -1) continue;

        if (socket_set_timeout(server.fd, 5) == CORVUS_ERR) {
            close(server.fd);
            continue;
        }

        memcpy(&server.addr, &node->master, sizeof(struct address));

        count = do_update_slot_map(&server);
        if (count < REDIS_CLUSTER_SLOTS) {
            conn_free(&server);
            conn_buf_free(&server);
            continue;
        }
        break;
    }
    node_list.idx = 0;

    conn_free(&server);
    conn_buf_free(&server);

    if (count == CORVUS_ERR) {
        LOG(WARN, "can not update slot map");
    } else {
        LOG(INFO, "slot map updated: corverd %d slots", count);
    }
}

void do_update(struct context *ctx, int job)
{
    slot_map_clear();

    if (job == SLOT_UPDATE && node_list.idx <= 0) {
        job = SLOT_UPDATE_INIT;
    }

    switch (job) {
        case SLOT_UPDATE_INIT:
            slot_map_init(ctx);
            break;
        case SLOT_UPDATE:
            slot_map_update(ctx);
            break;
        case SLOT_UPDATER_QUIT:
            ctx->quit = 1;
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
    slot_map_status = SLOT_MAP_NEW;

    while (!ctx->quit) {
        if (slot_job == SLOT_UPDATE_UNKNOWN) {
            pthread_cond_wait(&signal_cond, &job_mutex);
            continue;
        }

        job = slot_job;
        slot_job = SLOT_UPDATE_UNKNOWN;

        pthread_mutex_unlock(&job_mutex);

        do_update(ctx, job);

        pthread_mutex_lock(&job_mutex);
        slot_map_status = SLOT_MAP_NEW;
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
    uint16_t hash;
    int h, found = 0, found_s = 0, pos_len = pos->pos_len;
    int tag_start = -1, tag_end = -1;
    struct pos start_pos, end_pos;
    struct pos *p, *end = NULL, *start = pos->items, *items = pos->items;

    for (h = 0; h < pos->pos_len; h++) {
        p = &pos->items[h];
        len = p->len;
        str = p->str;

        for (s = 0; s < len; s++) {
            if (str[s] == '}' && found_s) {
                tag_end = h;
                memcpy(&end_pos, p, sizeof(end_pos));

                p->len -= len - s;
                found = 1;
                end = p;
                break;
            }

            if (str[s] == '{' && !found_s) {
                if (s == len - 1) {
                    if (h + 1 >= pos->pos_len) goto end;
                    if ((p+1)->len <= 0 || (p+1)->str[0] == '}') goto end;
                    start = p + 1;
                    found_s = 1;
                } else {
                    start = p;

                    tag_start = h;
                    memcpy(&start_pos, p, sizeof(start_pos));

                    p->str += s + 1;
                    p->len -= s + 1;
                    if (p->str[0] == '}') goto end;
                    found_s = 1;
                }
            }
        }
        if (found) break;
    }
end:
    if (found) {
        pos->items = start;
        pos->pos_len = end - start + 1;
    } else if (tag_start != -1) {
        memcpy(&pos->items[tag_start], &start_pos, sizeof(start_pos));
        tag_start = -1;
    }

    hash = crc16(pos) & 0x3FFF;

    if (found) {
        pos->items = items;
        pos->pos_len = pos_len;

        if (tag_end == tag_start) tag_end = -1;
        if (tag_end != -1) {
            memcpy(&pos->items[tag_end], &end_pos, sizeof(end_pos));
            tag_end = -1;
        }
        if (tag_start != -1) {
            memcpy(&pos->items[tag_start], &start_pos, sizeof(start_pos));
            tag_start = -1;
        }
    }
    return hash;
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
    if (slot_map_status == SLOT_MAP_NEW) {
        slot_map_status = SLOT_MAP_DIRTY;
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
    memset(&node_list, 0, sizeof(node_list));
    memset(&node_store, 0, sizeof(node_store));
    dict_init(&node_store.map);

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

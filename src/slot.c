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

#define ADDR_LIST_CHUNK 128
#define ADDR_NAME_LEN (HOST_NAME_MAX + 8)

struct job {
    STAILQ_ENTRY(job) next;
    int type;
    void *arg;
};

enum {
    SLOT_MAP_NEW,
    SLOT_MAP_DIRTY,
};

STAILQ_HEAD(job_queue, job);

extern void context_init(struct context *ctx, bool syslog, int log_level);
extern void context_free(struct context *ctx);

static struct node_info *slot_map[REDIS_CLUSTER_SLOTS];
static const char SLOTS_CMD[] = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";

static int slot_map_status;
static pthread_mutex_t job_queue_mutex;
static pthread_cond_t signal_cond;
static struct job_queue job_queue;

static pthread_rwlock_t slot_map_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t addr_list_lock = PTHREAD_RWLOCK_INITIALIZER;

static struct node_list node_list;

static struct {
    char *addrs;
    int bytes;
    int max_size;
} addr_list = {NULL, 0, 0};

static void addr_add(char *host, uint16_t port)
{
    int n;
    pthread_rwlock_wrlock(&addr_list_lock);
    if (addr_list.bytes > 0) {
        addr_list.addrs[addr_list.bytes++] = ',';
    }
    if (addr_list.max_size - addr_list.bytes < ADDR_NAME_LEN) {
        addr_list.max_size += ADDR_LIST_CHUNK * ADDR_NAME_LEN;
        addr_list.addrs = realloc(addr_list.addrs, addr_list.max_size);
    }
    n = snprintf(addr_list.addrs + addr_list.bytes, ADDR_NAME_LEN, "%s:%d", host, port);
    addr_list.bytes += n;
    pthread_rwlock_unlock(&addr_list_lock);
}

static void addr_list_clear()
{
    pthread_rwlock_wrlock(&addr_list_lock);
    if (addr_list.addrs != NULL) {
        memset(addr_list.addrs, 0, addr_list.max_size);
    }
    addr_list.bytes = 0;
    pthread_rwlock_unlock(&addr_list_lock);
}

static void node_list_free()
{
    struct node_info *node;
    while(!LIST_EMPTY(&node_list)) {
        node = LIST_FIRST(&node_list);
        LIST_REMOVE(node, next);
        if (node->slaves != NULL) free(node->slaves);
        free(node);
    }
}

static struct node_info *node_info_create(struct address *addr)
{
    struct node_info *node = malloc(sizeof(struct node_info));
    node->id = -1;
    node->slave_count = 0;
    node->slaves = NULL;

    addr_add(addr->host, addr->port);

    memcpy(&node->master, addr, sizeof(struct address));
    return node;
}

static struct node_info *node_info_find(struct context *ctx, struct redis_data *data)
{
    struct node_info *node;
    char *key;

    if (data->elements != 2) return NULL;

    struct pos_array *p = data->element[0]->pos;
    if (p->str_len <= 0) return NULL;

    char hostname[p->str_len + 1];
    if (pos_to_str(p, hostname) == CORVUS_ERR) return NULL;

    uint16_t port = data->element[1]->integer;

    struct address addr;
    socket_get_addr(hostname, p->str_len, port, &addr);

    key = socket_get_key(&addr);
    node = hash_get(ctx->server_table, key);
    if (node != NULL) {
        free(key);
    } else {
        node = node_info_create(&addr);
        hash_set(ctx->server_table, key, (void*)node);
    }
    return node;
}

static int node_info_add_slave(struct node_info *node, struct redis_data *data)
{
    struct address *addr = &node->slaves[node->slave_count++];

    if (data->elements != 2) return -1;

    struct pos_array *p = data->element[0]->pos;
    if (p->str_len <= 0) return CORVUS_ERR;

    char hostname[p->str_len + 1];
    if (pos_to_str(p, hostname) == CORVUS_ERR) return CORVUS_ERR;

    uint16_t port = data->element[1]->integer;
    socket_get_addr(hostname, p->str_len, port, addr);

    addr_add(addr->host, addr->port);
    return 0;
}

static int parse_slots_data(struct context *ctx, struct redis_data *data)
{
    size_t i, h;
    long long j;
    int count = 0;
    struct redis_data *d;
    struct node_info *node;

    if (data->elements <= 0) return -1;

    for (i = 0; i < data->elements; i++) {
        d = data->element[i];

        if (d->elements < 3) return -1;

        node = node_info_find(ctx, d->element[2]);
        if (node == NULL) return -1;

        for (j = d->element[0]->integer; j < d->element[1]->integer + 1; j++) {
            count++;
            slot_map[j] = node;
        }

        if (node->slaves == NULL && d->elements - 3 > 0) {
            node->slaves = malloc(sizeof(struct address) * (d->elements - 3));
            for (h = 3; h < d->elements; h++) {
                if (node_info_add_slave(node, d->element[h]) == -1) return -1;
            }
        }
    }

    return count;
}

static void slot_map_clear(struct context *ctx)
{
    pthread_rwlock_wrlock(&slot_map_lock);
    memset(slot_map, 0, sizeof(slot_map));
    pthread_rwlock_unlock(&slot_map_lock);

    addr_list_clear();

    struct node_info *node;
    hash_each(ctx->server_table, {
        free((void *)key);
        node = (struct node_info *)val;
        LIST_INSERT_HEAD(&node_list, node, next);
    });

    hash_clear(ctx->server_table);
}

static int do_update_slot_map(struct connection *server)
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
        conn_free(server);
        return -1;
    }

    struct command *cmd = cmd_create(server->ctx);
    cmd->server = server;
    if(cmd_read_reply(cmd, server) != CORVUS_OK) {
        LOG(ERROR, "update slot map, cmd read error");
        return -1;
    }

    LOG(INFO, "updating slot map using %s:%d", server->addr.host, server->addr.port);
    int count = parse_slots_data(server->ctx, cmd->rep_data);
    cmd_free(cmd);
    return count;
}

static void slot_map_init(struct context *ctx)
{
    int i, port, count = 0;
    char *addr;
    struct connection server;
    struct address address;
    struct node_conf *conf = ctx->node_conf;

    for (i = 0; i < conf->len; i++) {
        addr = conf->nodes[i];
        port = socket_parse_addr(addr, &address);
        if (port == -1) continue;

        conn_init(&server, ctx);
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
            continue;
        }
        break;
    }
    conn_free(&server);
    if (count == CORVUS_ERR) {
        LOG(WARN, "can not init slot map");
    } else {
        LOG(INFO, "slot map inited: covered %d slots", count);
    }
}

static void slot_map_update(struct context *ctx)
{
    int count = 0;
    struct node_info *node;
    struct connection server;

    conn_init(&server, ctx);

    LIST_FOREACH(node, &node_list, next) {
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
            continue;
        }
        break;
    }

    node_list_free();
    conn_free(&server);

    if (count == CORVUS_ERR) {
        LOG(WARN, "can not update slot map");
    } else {
        LOG(INFO, "slot map updated: corverd %d slots", count);
    }
}

static void do_update(struct context *ctx, struct job *job)
{
    slot_map_clear(ctx);

    if (job->type == SLOT_UPDATE && LIST_EMPTY(&node_list)) {
        job->type = SLOT_UPDATE_INIT;
    }

    switch (job->type) {
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
    free(job);
}

void *slot_map_updater(void *data)
{
    struct job *job;
    struct context *ctx = data;

    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&job_queue_mutex);
    slot_map_status = SLOT_MAP_NEW;

    while (!ctx->quit) {
        if (STAILQ_EMPTY(&job_queue)) {
            pthread_cond_wait(&signal_cond, &job_queue_mutex);
            continue;
        }

        job = STAILQ_FIRST(&job_queue);
        STAILQ_REMOVE_HEAD(&job_queue, next);

        pthread_mutex_unlock(&job_queue_mutex);

        do_update(ctx, job);

        pthread_mutex_lock(&job_queue_mutex);
        slot_map_status = SLOT_MAP_NEW;
    }
    pthread_mutex_unlock(&job_queue_mutex);

    while (!STAILQ_EMPTY(&job_queue)) {
        job = STAILQ_FIRST(&job_queue);
        STAILQ_REMOVE_HEAD(&job_queue, next);
        free(job);
    }
    pthread_rwlock_wrlock(&addr_list_lock);
    if (addr_list.addrs != NULL) free(addr_list.addrs);
    addr_list.addrs = NULL;
    addr_list.bytes = 0;
    pthread_rwlock_unlock(&addr_list_lock);

    node_list_free();

    pthread_rwlock_destroy(&slot_map_lock);
    pthread_rwlock_destroy(&addr_list_lock);
    pthread_mutex_destroy(&job_queue_mutex);
    context_free(ctx);

    LOG(DEBUG, "slot map update thread quiting");
    return NULL;
}

uint16_t slot_get(struct pos_array *pos)
{
    uint32_t s, len, orig_len = 0;
    uint8_t *str, *orig_str = NULL;
    uint16_t hash;
    int h, found = 0, found_s = 0, pos_len = pos->pos_len;
    struct pos *changed_pos = NULL, *start = pos->items, *items = pos->items, *end, *p;

    for (h = 0; h < pos->pos_len; h++) {
        p = &pos->items[h];
        len = p->len;
        str = p->str;

        for (s = 0; s < len; s++) {
            if (str[s] == '}' && found_s) {
                p->len -= len - s;
                found = 1;
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
                    changed_pos = p;
                    orig_len = p->len;
                    orig_str = p->str;
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
        end = &pos->items[pos_len - 1];
        pos->items = start;
        pos->pos_len = end - start + 1;
        hash = crc16(pos) & 0x3FFF;
        pos->items = items;
        pos->pos_len = pos_len;
        return hash;
    }
    if (changed_pos != NULL) {
        changed_pos->len = orig_len;
        changed_pos->str = orig_str;
    }
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

void slot_get_addr_list(char **dest)
{
    pthread_rwlock_rdlock(&addr_list_lock);
    *dest = calloc(addr_list.bytes + 1, sizeof(char));
    if (addr_list.addrs != NULL) {
        memcpy(*dest, addr_list.addrs, addr_list.bytes + 1);
    } else {
        (*dest)[0] = '\0';
    }
    pthread_rwlock_unlock(&addr_list_lock);
}

void slot_create_job(int type, void *arg)
{
    if (slot_map_status == SLOT_MAP_DIRTY) return;

    struct job *job = malloc(sizeof(struct job));
    job->arg = arg;
    job->type = type;

    pthread_mutex_lock(&job_queue_mutex);
    slot_map_status = SLOT_MAP_DIRTY;
    STAILQ_INSERT_TAIL(&job_queue, job, next);
    pthread_cond_signal(&signal_cond);
    pthread_mutex_unlock(&job_queue_mutex);
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
    STAILQ_INIT(&job_queue);
    LIST_INIT(&node_list);

    pthread_mutex_init(&job_queue_mutex, NULL);
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

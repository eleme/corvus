#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/queue.h>
#include <pthread.h>
#include <netdb.h>
#include <errno.h>
#include "corvus.h"
#include "hash.h"
#include "slot.h"
#include "connection.h"
#include "socket.h"
#include "logging.h"

#define SERVER_LIST_LEN 1024
#define THREAD_STACK_SIZE (1024*1024*4)

struct server_list {
    struct connection *list;
    size_t len;
    int max_len;
};

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

static struct node_info *slot_map[REDIS_CLUSTER_SLOTS];
static const char SLOTS_CMD[] = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";

static struct context slot_map_ctx;
static int slot_map_status;
static pthread_t slot_update_thread;
static pthread_mutex_t job_queue_mutex;
static pthread_cond_t signal_cond;
static struct job_queue job_queue;

static hash_t *node_map;
static struct node_list node_list;

static struct node_info *node_info_create(struct sockaddr *addr)
{
    struct node_info *node = malloc(sizeof(struct node_info));
    node->id = -1;
    node->slave_count = 0;
    node->slaves = NULL;
    memcpy(&node->master, addr, sizeof(struct sockaddr));
    return node;
}

static struct node_info *node_info_find(struct redis_data *data)
{
    struct sockaddr addr;
    struct node_info *node;
    char *key;

    if (data->elements != 2) return NULL;

    char *hostname = pos_to_str(data->element[0]->pos);
    if (hostname == NULL) return NULL;

    uint16_t port = data->element[1]->integer;
    int status = socket_get_addr(hostname, port, &addr);
    free(hostname);

    if (status == -1) return NULL;

    key = socket_get_key(&addr);
    node = hash_get(node_map, key);
    if (node != NULL) {
        free(key);
    } else {
        node = node_info_create(&addr);
        hash_set(node_map, key, (void*)node);
    }
    return node;
}

static int node_info_add_slave(struct node_info *node, struct redis_data *data)
{
    struct sockaddr *addr = &node->slaves[node->slave_count++];

    if (data->elements != 2) return -1;

    char *hostname = pos_to_str(data->element[0]->pos);
    if (hostname == NULL) return -1;

    uint16_t port = data->element[1]->integer;
    int status = socket_get_addr(hostname, port, addr);
    free(hostname);
    if (status == -1) return -1;
    return 0;
}

static int parse_slots_data(struct redis_data *data)
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

        node = node_info_find(d->element[2]);
        if (node == NULL) return -1;

        for (j = d->element[0]->integer; j < d->element[1]->integer + 1; j++) {
            count += 1;
            slot_map[j] = node;
        }

        if (node->slaves == NULL && d->elements - 3 > 0) {
            node->slaves = malloc(sizeof(struct sockaddr) * (d->elements - 3));
            for (h = 3; h < d->elements; h++) {
                if (node_info_add_slave(node, d->element[h]) == -1) return -1;
            }
        }
    }

    return count;
}

static void slot_map_clear()
{
    LOG(DEBUG, "empty slot map");
    struct node_info *node;
    hash_each(node_map, {
        free((void *)key);
        node = (struct node_info *)val;
        LIST_INSERT_HEAD(&node_list, node, next);
    });

    hash_clear(node_map);
}

static int do_update_slot_map(struct connection *server, int use_addr)
{
    conn_connect(server, use_addr);
    if (server->status != CONNECTED) {
        LOG(ERROR, "update_slot_map, server not connected: %s", strerror(errno));
        return -1;
    }

    struct iovec iov;
    iov.iov_base = (void*)SLOTS_CMD;
    iov.iov_len = strlen(SLOTS_CMD);

    if (socket_write(server->fd, &iov, 1) <= 0) {
        LOG(ERROR, "update_slot_map: socket write: %s", strerror(errno));
        conn_free(server);
        return -1;
    }

    struct command *cmd = cmd_create(&slot_map_ctx);
    cmd->server = server;
    if(cmd_read_reply(cmd, server) != CORVUS_OK) {
        LOG(ERROR, "update_slot_map: cmd read error");
        return -1;
    }

    int count = parse_slots_data(cmd->rep_data);
    cmd_free(cmd);
    return count;
}


uint16_t slot_get(struct pos_array *pos)
{
    uint32_t s, len, orig_len;
    uint8_t *str, *orig_str;
    uint16_t hash;
    int h, found = 0, found_s = 0, pos_len = pos->pos_len;
    struct pos *changed_pos = NULL, *p, *start, *end, *items = pos->items;

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

struct node_info *slot_get_node_info(uint16_t slot)
{
    return slot_map[slot];
}

void slot_init_map(struct node_conf *conf)
{
    int port, count = 0;
    int i;
    struct connection server;
    char *hostname, *addr;
    for (i = 0; i < conf->len; i++) {
        addr = conf->nodes[i];
        port = socket_parse_addr(addr, &hostname);
        if (port == -1) continue;

        conn_init(&server, &slot_map_ctx);
        server.fd = socket_create_stream();
        if (server.fd == -1) continue;

        server.hostname = hostname;
        server.port = port;

        count = do_update_slot_map(&server, false);
        if (count < REDIS_CLUSTER_SLOTS) {
            conn_free(&server);
            continue;
        }
        break;
    }
    LOG(INFO, "slot map inited: covered %d slots", count);
}

void slot_map_update()
{
    LOG(DEBUG, "update slot map");
    int count = 0;
    struct node_info *node;
    struct sockaddr addr;
    struct connection server;
    conn_init(&server, &slot_map_ctx);

    LIST_FOREACH(node, &node_list, next) {
        server.fd = socket_create_stream();
        if (server.fd == -1) continue;
        memcpy(&server.addr, &node->master, sizeof(addr));
        count = do_update_slot_map(&server, true);
        if (count < REDIS_CLUSTER_SLOTS) {
            conn_free(&server);
            continue;
        }
        break;
    }

    while(!LIST_EMPTY(&node_list)) {
        node = LIST_FIRST(&node_list);
        LIST_REMOVE(node, next);
        if (node->slaves != NULL) free(node->slaves);
        free(node);
    }

    LOG(INFO, "slot map updated: corverd %d slots", count);
    conn_free(&server);
}

void do_update(struct job *job)
{
    slot_map_clear();

    switch (job->type) {
        case SLOT_UPDATE_INIT:
            slot_init_map(job->arg);
            break;
        case SLOT_UPDATE:
            slot_map_update();
            break;
    }
}

void *slot_map_updater()
{
    struct job *job;

    /* Make the thread killable at any time can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&job_queue_mutex);
    slot_map_status = SLOT_MAP_NEW;

    while (1) {
        if (STAILQ_EMPTY(&job_queue)) {
            pthread_cond_wait(&signal_cond, &job_queue_mutex);
            continue;
        }

        job = STAILQ_FIRST(&job_queue);
        STAILQ_REMOVE_HEAD(&job_queue, next);

        pthread_mutex_unlock(&job_queue_mutex);

        do_update(job);

        pthread_mutex_lock(&job_queue_mutex);
        slot_map_status = SLOT_MAP_NEW;
    }
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

int slot_init_updater(bool syslog, int log_level)
{
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;

    slot_map_ctx.syslog = syslog;
    slot_map_ctx.log_level = log_level;
    mbuf_init(&slot_map_ctx);
    log_init(&slot_map_ctx);
    STAILQ_INIT(&slot_map_ctx.free_cmdq);
    slot_map_ctx.nfree_cmdq = 0;

    memset(slot_map, 0, sizeof(struct node_info*) * REDIS_CLUSTER_SLOTS);
    STAILQ_INIT(&job_queue);
    node_map = hash_new();
    LIST_INIT(&node_list);

    pthread_mutex_init(&job_queue_mutex, NULL);
    pthread_cond_init(&signal_cond, NULL);

    /* Set the stack size as by default it may be small in some system */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    if (pthread_create(&thread, &attr, slot_map_updater, NULL) != 0) {
        LOG(ERROR, "can't initialize slot updating thread");
        return -1;
    }
    slot_update_thread = thread;
    return 0;
}

void slot_kill_updater()
{
    int err;
    if (pthread_cancel(slot_update_thread) == 0) {
        if ((err = pthread_join(slot_update_thread, NULL)) != 0) {
            LOG(ERROR, "slot update thread can not be joined: %s", strerror(err));
        } else {
            LOG(WARN, "slot update thread terminated");
        }
    }
}

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
/*
 * Polling is required when a preferred node is not available.
 * We need to poll cluster config to find out when node can be used.
 */
static int8_t polling_required = 0;
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
/*
There are some certain places where write operation
of node_list.len is not protected by lock.
This is ok and its reason is quite subtle:
(1) we assume that assignment for `int` is atomic,
    which rely on the alignment of node_list and
    that only one instruction is generated for the
    assignment statement.
(2) `len` is not protected but node_list.nodes is.
(3) its write operations are all in the thread managing slots
    and all of them are resetting node_list.len to zero.
(4) its read operation from other threads is only in node_list_get,
    inside which `<` operation is used instead of `!=`
    to determine the loop exit.
*/

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

static inline void node_list_replace(struct address *nodes, size_t len)
{
    pthread_rwlock_wrlock(&node_list.lock);
    node_list.len = len;
    // memcpy works for zero len
    memcpy(node_list.nodes, nodes, sizeof(struct address) * len);
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

/*
 * For every slot, set the preferred_nodes list
 * The nodes available for that slot are sorted such that the preferred nodes
 * come first in the list.
 */
static void sort_nodes()
{
    bool invalid_found = false;
    bool polling_changed =  false;
    struct node_conf *preferred_nodes = config_get_preferred_node();
    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slot_map.node_map, &iter) {
        struct node_info *n = iter.value;
        struct node sorted_nodes[MAX_SLAVE_NODES + 1];
        struct node remaining_nodes[MAX_SLAVE_NODES + 1];
        int sorted_size = 0;
        int remaining_size = 0;
        for (int i = 0; i < preferred_nodes->len; i++) {
            for (int j = 0; j < n->index; j++) {
                // Build list of nodes that are in the preferred list
                if (socket_cmp(&n->nodes[j].addr, &preferred_nodes->addr[i]) == 0) {
                    if (n->nodes[j].available) {
                        memcpy(&sorted_nodes[sorted_size], &n->nodes[j], sizeof(struct address));
                        sorted_size++;
                    } else {
                        // A preferred node is not valid, set polling_required flag so that the cluster config
                        // gets retrieved regulary in case preferred node becomes available again.
                        invalid_found = true;
                    }
                }
            }
        }

        // Add remaining nodes to list
        for (int i = 0; i < n->index; i++) {
            bool found = false;
            if (!n->nodes[i].available) continue;

            for (int j = 0; j < sorted_size; j++) {
                if (socket_cmp(&n->nodes[i].addr, &sorted_nodes[j].addr) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                memcpy(&remaining_nodes[remaining_size], &n->nodes[i], sizeof(struct address));
                remaining_size++;
            }
        }

        // Copy sorted_nodes back to node_info structure
        memcpy(&n->preferred_nodes[0], &sorted_nodes[0], sizeof(struct address) * sorted_size);
        // Add remaining nodes to the end of list
        memcpy(&n->preferred_nodes[sorted_size], &remaining_nodes[0], sizeof(struct address) * remaining_size);
    }

    if (invalid_found) {
        LOG(INFO, "Unreachable preferred node found, polling required.");
        // A preferred node has been found to be invalid, start polling for cluster config
        // Only lock mutex if required
        if (!polling_required) polling_changed = true;
    } else if (polling_required) {
        LOG(INFO, "Preferred nodes ok, polling stopped.");
        polling_changed = true;
    }

    if (polling_changed) {
       pthread_mutex_lock(&job_mutex);
       polling_required = !polling_required;
       pthread_mutex_unlock(&job_mutex);
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
    struct address tmp_nodes[MAX_NODE_LIST];
    size_t tmp_nodes_len = 0;

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slot_map.node_map, &iter) {
        struct node_info *n = iter.value;

        // the node has no slots
        if (n->spec_length <= 0) {
            cv_free(n);
            continue;
        }

        size_t num = MIN(n->index, MAX_NODE_LIST - tmp_nodes_len);
        if (num > 0) {
            memcpy(tmp_nodes + tmp_nodes_len, n->nodes, num * sizeof(struct address));
            tmp_nodes_len += num;
        }

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

    node_list_replace(tmp_nodes, tmp_nodes_len);

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
        /*
         * A node is considered available (can be sent commands) if it is connected to the cluster
         * and it's replying to PINGs
         */
        bool is_available = strcmp(d->parts[7].data, "connected") == 0 && d->parts[4].data[0] == '0';
        char *name = is_master ? d->parts[0].data : d->parts[3].data;
        struct node_info *node = dict_get(&slot_map.node_map, name);
        if (node == NULL) {
            node = cv_calloc(1, sizeof(struct node_info));
            node->index = 1;
            strcpy(node->name, name);
            dict_set(&slot_map.node_map, node->name, node);
        }
        if (is_master) {
            socket_parse_addr(d->parts[1].data, &node->nodes[0].addr);
            node->nodes[0].available = is_available;
            node->slot_spec = d->parts;
            node->spec_length = d->index - 8;
        } else if (node->index <= MAX_SLAVE_NODES && (
                    strcasecmp(d->parts[2].data, "slave") == 0 ||
                    strcasecmp(d->parts[2].data, "myself,slave") == 0)) {
            socket_parse_addr(d->parts[1].data, &node->nodes[node->index].addr);
            node->nodes[node->index].available = is_available;
            node->index++;
        }
    }

    /*
     * If readpreferred is set, nodes need to be sorted by preferrence.
     */
    if (config.readpreferred) sort_nodes();

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

void slot_map_update(struct context *ctx, bool reload)
{
    int i, count = 0;
    struct address node;

    struct connection *server = conn_create(ctx);
    server->info = conn_info_create(ctx);

    if (node_list.len <= 0) {
        node_list_init();
    }

    int len;
    struct address nodes[MAX_NODE_LIST];
    if (reload) {
        struct node_conf *node = config_get_node();
        len = node->len;
        memcpy(nodes, node->addr, len * sizeof(struct address));
        config_node_dec_ref(node);
    } else {
        memcpy(nodes, node_list.nodes, sizeof(node_list.nodes));
        len = node_list.len;
    }

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
        node_list.len = 0;  // clear it if we can't update slot map
        LOG(WARN, "can not update slot map");
    } else {
        LOG(INFO, "slot map updated: corverd %d slots", count);
    }
}

void do_job(struct context *ctx, int job)
{
    switch (job) {
        case SLOT_UPDATE:
            slot_map_update(ctx, false);
            break;
        case SLOT_RELOAD:
            slot_map_update(ctx, true);
            break;
        case SLOT_UPDATER_QUIT:
            ctx->state = CTX_QUIT;
            break;
    }
}

void *slot_manager(void *data)
{
    int job, ret;
    struct context *ctx = data;
    struct timespec timeToWait;

    pthread_mutex_lock(&job_mutex);
    in_progress = 0;

    while (ctx->state != CTX_QUIT) {
        if (slot_job == SLOT_UPDATE_UNKNOWN) {
            if (polling_required) {
                timeToWait.tv_sec = time(NULL) + config.polling_interval;
                ret = pthread_cond_timedwait(&signal_cond, &job_mutex, &timeToWait);
                /* If polling interval is over, retrieve cluster config */
                if (ret == ETIMEDOUT) {
                    slot_job = SLOT_UPDATE;
                }
            } else {
                pthread_cond_wait(&signal_cond, &job_mutex);
            }
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

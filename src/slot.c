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

// slot信息
static struct {
    pthread_rwlock_t lock;  // 读写锁, 一写多读
    // 存储slot和节点关系的列表, index是slot, 值是存储该slot的redis实例的节点信息
    struct node_info *data[REDIS_CLUSTER_SLOTS];
    struct dict free_nodes; // 空闲节点(没有用处, 待释放掉的节点)
    struct dict node_map;   // redis主节点映射
} slot_map = {.lock = PTHREAD_RWLOCK_INITIALIZER};

/*
 * 字典node_map:
 * 1. key是redis集群给节点的id
 * 2. value是结构体node_info的对象(node_info的结构在slot.h中定义)
 * 每一项都是一个主节点的相关信息(注意没有从节点信息)
 */

// Corvus所管理的所有redis节点
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

// 初始化node_list这个全局变量
static inline void node_list_init()
{
    struct node_conf *node = config_get_node(); // 从配置文件中读取redis节点
    pthread_rwlock_wrlock(&node_list.lock); // 获取读写锁
    node_list.len = 0;
    // 对每个节点进行初始化
    for (int i = 0; i < MIN(node->len, MAX_NODE_LIST); i++) {
        memcpy(&node_list.nodes[i], &node->addr[i], sizeof(struct address));
        node_list.len++;
    }
    pthread_rwlock_unlock(&node_list.lock);
    // 释放node
    config_node_dec_ref(node);
}

// 更新node_list(存储Corvus所管理的所有redis节点信息)
static inline void node_list_replace(struct address *nodes, size_t len)
{
    pthread_rwlock_wrlock(&node_list.lock); // 获取读写锁
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

// 解析slot属性, 获取每个redis节点存储的slot, 并更新到slot_map
int parse_slots()
{
    char *p;
    int start, stop, slot_count = 0;    // 记录更新了多少个slot
    struct node_info *node;
    struct address tmp_nodes[MAX_NODE_LIST];
    size_t tmp_nodes_len = 0;

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&slot_map.node_map, &iter) {   // 遍历node_map字典
        struct node_info *n = iter.value;

        // the node has no slots
        if (n->spec_length <= 0) {  // 如果主节点没有slot, 释放本节点信息
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
            // 循环删除掉slot对应的原redis节点信息
            while (start <= stop) {
                slot_count++;
                // 使用新节点信息替换掉该slot对应的原节点信息, 并返回原节点信息
                node = ATOMIC_IGET(slot_map.data[start++], n);
                if (node == NULL) {
                    continue;
                }
                node->refcount--;
                // 把原节点信息放入free_nodes, 等待清理
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
    node_map_free(&slot_map.free_nodes);    // 释放free_nodes
    pthread_rwlock_unlock(&slot_map.lock);

    // 更新Corvus管理的redis节点信息
    node_list_replace(tmp_nodes, tmp_nodes_len);

    dict_clear(&slot_map.free_nodes);
    return slot_count;
}

// 更新slot信息
// cluster nodes请求获取的结果格式如下:
// 1f7a0c3db882f4a367f499420b3f51de03c39779 10.0.20.79:7001 myself,slave a03dce1c14749432cc992d53ec8fa4daee8fe412 0 0 8 connected
// 每个节点一共有8项数据:
// 1. 节点ID
// 2. 节点地址
// 3. flag(master, slave, myself, fail?, fail, handshake, noaddr, noflags)
// 4. 主节点ID(如果本节点是主节点则为-)
// 5. 最近一次发送ping的时间(毫秒时间戳), 0表示没发过
// 6. 最近一次收到pong的时间(毫秒时间戳)
// 7. 节点的epoch值
// 8. 集群总线使用的链接的状态(connected, disconnected)
// 9. 本节点存储slot槽位的范围(只有主节点才有这个值)
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
        if (d->index < 8) { // 判断返回值是否合法
            node_map_free(&slot_map.node_map);
            goto end;
        }
        // 判断节点是否是master
        bool is_master = d->parts[3].data[0] == '-';
        // 获取主节点ID
        char *name = is_master ? d->parts[0].data : d->parts[3].data;
        // 从node_map中获取主节点信息
        struct node_info *node = dict_get(&slot_map.node_map, name);
        if (node == NULL) {
            node = cv_calloc(1, sizeof(struct node_info));
            node->index = 1;
            strcpy(node->name, name);
            dict_set(&slot_map.node_map, node->name, node);
        }
        if (is_master) {    // 如果本节点是主节点
            // 给node_info实例中的节点地址列表的表头插入主节点地址
            socket_parse_addr(d->parts[1].data, &node->nodes[0]);
            node->slot_spec = d->parts;
            node->spec_length = d->index - 8;   // slot信息参数的个数
        } else if (node->index <= MAX_SLAVE_NODES && (
                    strcasecmp(d->parts[2].data, "slave") == 0 ||
                    strcasecmp(d->parts[2].data, "myself,slave") == 0)) {
            // 本节点是从节点
            // 给node_info实例中的节点地址列表append从节点地址
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

// 解析响应数据, 并更新slot信息
int slot_parse_data(struct reader *r, struct mbuf *buf, int *count)
{
    reader_feed(r, buf);    // 从缓冲区读取数据
    if (parse(r, MODE_REQ) == -1) {     // 使用redis协议解析数据
        LOG(ERROR, "slot_map: parse cluster slots error");
        return CORVUS_ERR;
    }
    if (reader_ready(r)) {      // 判断是否解析完毕
        *count = parse_cluster_nodes(&r->data);     // 更新slot信息
        redis_data_free(&r->data);
    }
    return CORVUS_OK;
}

// 读取并解析redis返回的请求, 最后再更新slot信息
int slot_read_data(struct connection *server, int *count)
{
    int n, rsize;
    struct mbuf *buf;
    struct reader *r = &server->info->reader;

    while (1) {
        buf = conn_get_buf(server, true, false);    // 获取缓冲区
        rsize = mbuf_read_size(buf);    // 缓冲区是否还有没处理的数据

        if (rsize > 0) {    // 如果有, 就解析数据, 更新slot信息并返回
            if (slot_parse_data(r, buf, count) == CORVUS_ERR) return CORVUS_ERR;
            if (reader_ready(r)) return CORVUS_OK;
            continue;
        }

        // 从套接字中读取信息到缓冲区
        n = socket_read(server->fd, buf);
        if (n == 0 || n == CORVUS_ERR || n == CORVUS_AGAIN) return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// 更新slot信息
// 1. 与提供的redis实例建立连接
// 2. 构造"cluster nodes"请求
// 3. 发送请求
// 4. 读取和解析response, 并更新slot信息
int do_update_slot_map(struct connection *server)
{
    // 与redis实例建立连接
    if (conn_connect(server) == CORVUS_ERR) return CORVUS_ERR;
    if (server->info->status != CONNECTED) {
        LOG(ERROR, "update slot map, server not connected: %s", strerror(errno));
        return CORVUS_ERR;
    }

    // 构造请求体, 用于向redis发送请求
    struct iovec iov;
    iov.iov_base = (void*)SLOTS_CMD;    // 构造"cluster nodes"请求
    iov.iov_len = strlen(SLOTS_CMD);    // 记录请求体长度

    // 向redis发送请求
    if (socket_write(server->fd, &iov, 1) <= 0) {
        LOG(ERROR, "update slot map, socket write: %s", strerror(errno));
        return CORVUS_ERR;
    }

    int count = -1;
    LOG(INFO, "updating slot map using %s:%d",
            server->info->addr.ip, server->info->addr.port);
    // 读取并解析请求, 更新slot信息
    if(slot_read_data(server, &count) != CORVUS_OK) {
        LOG(ERROR, "update slot map, cmd read error");
        return CORVUS_ERR;
    }
    return count;
}

// 更新slot映射
void slot_map_update(struct context *ctx, bool reload)
{
    int i, count = 0;
    struct address node;

    // 创建新的连接
    struct connection *server = conn_create(ctx);
    // 初始化conn_info
    server->info = conn_info_create(ctx);

    // 如果node_list没有初始化, 先初始化
    if (node_list.len <= 0) {
        node_list_init();
    }

    int len;
    struct address nodes[MAX_NODE_LIST];
    // 是否重载, 如果需要重载, 则重新从配置文件中读取redis节点
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
        // 随机选取redis节点
        int r = rand_r(&ctx->seed) % i;
        node = nodes[r];
        nodes[r] = nodes[i - 1];

        // 创建新的套接字用于连接redis实例
        server->fd = socket_create_stream();
        if (server->fd == -1) continue;

        // 设置超时时间
        if (socket_set_timeout(server->fd, 5) == CORVUS_ERR) {
            close(server->fd);
            continue;
        }

        // 将随机选取的节点地址拷贝到连接对象中
        memcpy(&server->info->addr, &node, sizeof(struct address));

        // 更新slot信息
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

// slot相关操作逻辑
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

// slot manager线程真正执行的函数, 该函数通过slot_job变量来进行更新slot的操作
// 在函数刚执行的时候, slot_job为SLOT_UPDATE_UNKNOWN, 它会释放互斥锁job_mutex, 同时会等待信号signal_cond, block在386行
// 当slot_job被slot_create_job函数更新后, signal_cond被获取到, 线程会获取互斥锁job_mutex, 然后会执行到391行之后的东西, 对slot进行操作
void *slot_manager(void *data)
{
    int job;
    struct context *ctx = data;

    pthread_mutex_lock(&job_mutex);
    in_progress = 0;

    // context的state不是退出状态的时候执行
    while (ctx->state != CTX_QUIT) {
        if (slot_job == SLOT_UPDATE_UNKNOWN) {
            // slot_job没有变化的时候, 该线程会block在这里
            pthread_cond_wait(&signal_cond, &job_mutex);
            continue;
        }

        // 该线程获取到signal_cond, 执行到这里之前会获取互斥锁job_mutex
        job = slot_job;     // 获取当前更新后的slot_job
        slot_job = SLOT_UPDATE_UNKNOWN;     // 把slot_job重置

        pthread_mutex_unlock(&job_mutex);   // 解锁

        // 真正的更新slot逻辑
        do_job(ctx, job);
        // 睡眠100ms
        usleep(100000);

        pthread_mutex_lock(&job_mutex);     // 加锁
        in_progress = 0;                    // 重置in_progress变量(slot_create_job在更新slot_job变量的时候会更新这个变量)
    }
    pthread_mutex_unlock(&job_mutex);

    // context的state是退出状态, 需要销毁相关资源
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

// 更新slot_job变量
void slot_create_job(int type)
{
    // 更新slot更新计数器到statsd打点
    incr_slot_update_counter();
    // 获取互斥锁job_mutex
    pthread_mutex_lock(&job_mutex);
    // 检查in_progress是否为0(该变量表示slot更新逻辑是否在进行中)
    if (!in_progress || type == SLOT_UPDATER_QUIT) {
        in_progress = 1;
        slot_job = type;
        // 释放信号锁, 用来触发slot manager线程工作
        pthread_cond_signal(&signal_cond);
    }
    // 释放互斥锁
    pthread_mutex_unlock(&job_mutex);
}

// 创建slot manager线程, 并启动该线程
int slot_start_manager(struct context *ctx)
{
    int err;
    // 初始化slot_map.data
    memset(slot_map.data, 0, sizeof(struct node_info*) * REDIS_CLUSTER_SLOTS);
    // 初始化slot_map.free_nodes
    dict_init(&slot_map.free_nodes);
    // 初始化slot_map.node_map
    dict_init(&slot_map.node_map);
    // 初始化node_list
    node_list_init();

    // 初始化互斥锁job_mutex
    if ((err = pthread_mutex_init(&job_mutex, NULL)) != 0) {
        LOG(ERROR, "pthread_mutex_init: %s", strerror(err));
        return CORVUS_ERR;
    }
    // 初始化信号锁signal_cond
    if ((err = pthread_cond_init(&signal_cond, NULL)) != 0) {
        LOG(ERROR, "pthread_cond_init: %s", strerror(err));
        return CORVUS_ERR;
    }

    LOG(INFO, "starting slot manager thread");
    // 创建线程并执行
    return thread_spawn(ctx, slot_manager);
}

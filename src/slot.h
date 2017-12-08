#ifndef SLOT_H
#define SLOT_H

#include <stdint.h>
#include "parser.h"
#include "socket.h"

#define MAX_DESC_PART_LEN 64
#define MAX_NODE_LIST 16
#define MAX_SLAVE_NODES 64
#define REDIS_CLUSTER_SLOTS 16384

struct context;

enum {
    SLOT_UPDATE_UNKNOWN,
    SLOT_UPDATE_INIT,
    SLOT_UPDATE,
    SLOT_RELOAD,
    SLOT_UPDATER_QUIT,
};

struct desc_part {
    char data[MAX_DESC_PART_LEN];
    uint8_t len;
};

struct node_desc {
    struct desc_part *parts;
    uint16_t index, len;
};

// redis主节点信息
struct node_info {
    char name[64];  // 节点id
    // contains master and slaves of one shard
    // 一个shard中所有节点的地址, 主节点地址在列表中的第一个, 后面都是从节点地址
    struct address nodes[MAX_SLAVE_NODES + 1];
    size_t index;  // length of `nodes` above
    int refcount;   // 记录有多少个slot存储在本节点上
    // for parsing slots of a master node
    // 以下两个属性只有主节点才有
    struct desc_part *slot_spec;    // 本节点的信息
    int spec_length;    // 上述slot_spec中, 描述slot的参数的个数
};

uint16_t slot_get(struct pos_array *pos);
void node_list_get(char *dest);
bool slot_get_node_addr(uint16_t slot, struct node_info *info);
void slot_create_job(int type);
int slot_start_manager(struct context *ctx);

#endif /* end of include guard: SLOT_H */

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

struct node {
    struct address addr;
    bool available;
};

struct node_info {
    char name[64];
    // contains preferred node (if exists), master and slaves of one shard
    struct node nodes[MAX_SLAVE_NODES + 1];
    size_t index;  // length of `nodes` above
    struct node preferred_nodes[MAX_SLAVE_NODES + 1];
    size_t preferred_index;  // length of `preferred_nodes` above
    int refcount;
    // for parsing slots of a master node
    struct desc_part *slot_spec;
    int spec_length;
};

uint16_t slot_get(struct pos_array *pos);
void node_list_get(char *dest);
bool slot_get_node_addr(uint16_t slot, struct node_info *info);
void slot_create_job(int type);
int slot_start_manager(struct context *ctx);

#endif /* end of include guard: SLOT_H */

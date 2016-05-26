#ifndef __SLOT_H
#define __SLOT_H

#include <stdint.h>
#include "parser.h"
#include "socket.h"

#define MAX_DESC_PART_LEN 64
#define REDIS_CLUSTER_SLOTS 16384

struct context;

enum {
    SLOT_UPDATE_UNKNOWN,
    SLOT_UPDATE_INIT,
    SLOT_UPDATE,
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

struct node_info {
    char name[64];
    struct address *nodes;
    size_t index;
    size_t len;
    bool slave_added;
    struct desc_part *slot_spec;
    int spec_length;
};

uint16_t slot_get(struct pos_array *pos);
void slot_get_addr_list(char *dest);
bool slot_get_node_addr(struct context *ctx, uint16_t slot, struct address *addr,
        struct address *slave);
void slot_create_job(int type);
int slot_start_manager(struct context *ctx);

#endif /* end of include guard: __SLOT_H */

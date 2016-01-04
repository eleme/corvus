#ifndef __SLOT_H
#define __SLOT_H

#include <stdint.h>
#include "parser.h"
#include "socket.h"

#define REDIS_CLUSTER_SLOTS 16384

struct context;

enum {
    SLOT_UPDATE_UNKNOWN,
    SLOT_UPDATE_INIT,
    SLOT_UPDATE,
    SLOT_UPDATER_QUIT,
};

struct node_info {
    struct address master;
    int8_t dsn_added;
};

uint16_t slot_get(struct pos_array *pos);
void slot_get_addr_list(char *dest);
int slot_get_node_addr(uint16_t slot, struct address *addr);
void slot_create_job(int type);
int slot_init_updater(struct context *ctx);

#endif /* end of include guard: __SLOT_H */

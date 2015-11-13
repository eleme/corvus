#ifndef __SLOT_H
#define __SLOT_H

#include <stdint.h>
#include "corvus.h"
#include "parser.h"
#include "socket.h"

#define REDIS_CLUSTER_SLOTS 16384

struct context;

enum {
    SLOT_UPDATE_INIT,
    SLOT_UPDATE,
    SLOT_UPDATER_QUIT,
};

struct node_info {
    LIST_ENTRY(node_info) next;
    int id;
    struct address master;
    size_t slave_count;
    struct address *slaves;
};

LIST_HEAD(node_list, node_info);

uint16_t slot_get(struct pos_array *pos);
struct node_info *slot_get_node_info(uint16_t slot);
void slot_create_job(int type, void *arg);
void slot_kill_updater();
int slot_init_updater(struct context *ctx);

#endif /* end of include guard: __SLOT_H */

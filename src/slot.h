#ifndef __SLOT_H
#define __SLOT_H

#include <stdint.h>
#include <sys/queue.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "parser.h"

struct context;

enum {
    SLOT_UPDATE_INIT,
    SLOT_UPDATE,
};

struct node_info {
    LIST_ENTRY(node_info) next;
    int id;
    struct sockaddr master;
    size_t slave_count;
    struct sockaddr *slaves;
};

LIST_HEAD(node_list, node_info);

uint16_t slot_get(struct pos_array *pos);
struct node_info *slot_get_node_info(uint16_t slot);
void slot_create_job(int type, void *arg);
void slot_kill_updater();
int slot_init_updater(bool syslog, int log_level);

#endif /* end of include guard: __SLOT_H */

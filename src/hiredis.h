#ifndef __HIREDIS_H
#define __HIREDIS_H
#include <stdio.h> /* for size_t */
#include <stdint.h>
#include "../deps/read.h"

struct str {
    uint32_t len;
    char *str;
};

struct array_task {
    int size;
    int item_len;
    struct str *items;
};

/* Public API for the protocol parser. */
redisReader *hiredis_init();
int hiredis_feed_data(redisReader *r, uint8_t *buf, uint32_t len);
int hiredis_get_result(redisReader *r, void **result);
void hiredis_free_task(struct array_task *task);

#endif

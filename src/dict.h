#ifndef __DICT_H
#define __DICT_H

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>

#define DICT_ITER_INITIALIZER {NULL, NULL, 0}
#define DICT_FOREACH(map, iter) \
    while (dict_next(map, iter) != NULL)

struct bucket {
    void *data;
    const char *key;
    uint32_t hash;
    bool deleted;
    bool setted;
};

struct dict {
    struct bucket *buckets;
    uint32_t capacity;
    uint32_t length;
    uint32_t resize_threshold;
};

struct dict_iter {
    void *value;
    const char *key;
    uint32_t idx;
};

void dict_init(struct dict *dict);
void dict_set(struct dict *dict, const char *key, void *data);
void *dict_get(struct dict *dict, const char *key);
void dict_delete(struct dict *dict, const char *key);
void dict_free(struct dict *dict);
int dict_index(struct dict *dict, const char *key);
struct dict_iter *dict_next(struct dict *dict, struct dict_iter *iter);
void dict_clear(struct dict *dict);

#endif /* end of include guard: __DICT_H */

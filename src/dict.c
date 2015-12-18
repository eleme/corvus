#include <string.h>
#include <math.h>
#include "hash.h"
#include "dict.h"

#define DICT_BASE_CAPACITY 1024
#define LOAD_FACTOR 0.909

inline static void set_bucket(struct bucket *bucket, uint32_t hash, char *key, void *data)
{
    bucket->deleted = 0;
    bucket->setted = 1;
    bucket->hash = hash;
    bucket->key = key;
    bucket->data = data;
}

inline static uint32_t probe_distance(struct dict *dict, uint32_t hash, uint32_t idx)
{
    return (idx + dict->capacity - hash % dict->capacity) % dict->capacity;
}

static void _dict_init(struct dict *dict)
{
    dict->buckets = calloc(dict->capacity, sizeof(struct bucket));
    dict->resize_threshold = dict->capacity * LOAD_FACTOR;
    dict->length = 0;
}

void dict_init(struct dict *dict)
{
    dict->capacity = DICT_BASE_CAPACITY;
    _dict_init(dict);
}

void dict_resize(struct dict *dict)
{
    struct bucket *bucket, *buckets = dict->buckets;
    uint32_t i, capacity = dict->capacity;

    dict->capacity += DICT_BASE_CAPACITY;
    _dict_init(dict);

    for (i = 0; i < capacity; i++) {
        bucket = &buckets[i];
        if (!bucket->deleted && bucket->setted) {
            dict_set(dict, bucket->key, bucket->data);
        }
    }
    free(buckets);
}

void dict_set(struct dict *dict, const char *key, void *data)
{
    if (dict->length + 1 >= dict->resize_threshold) {
        dict_resize(dict);
    }

    dict->length++;

    uint32_t hash = lookup3_hash(key);
    uint32_t pos = hash % dict->capacity;
    uint32_t probe_dist, dist = 0;

    const char *temp_key;
    uint32_t temp_hash;
    void *temp_data;

    struct bucket *cur_bucket;

    while (1) {
        if (!dict->buckets[pos].setted) {
            set_bucket(&dict->buckets[pos], hash, (char*)key, data);
            return;
        }

        probe_dist = probe_distance(dict, dict->buckets[pos].hash, pos);
        if (probe_dist < dist) {
            if (dict->buckets[pos].deleted) {
                set_bucket(&dict->buckets[pos], hash, (char*)key, data);
                return;
            }
            cur_bucket = &dict->buckets[pos];
            temp_hash = cur_bucket->hash;
            cur_bucket->hash = hash;
            hash = temp_hash;

            temp_key = cur_bucket->key;
            cur_bucket->key = (char*)key;
            key = temp_key;

            temp_data = cur_bucket->data;
            cur_bucket->data = data;
            data = temp_data;

            dist = probe_dist;
        }
        pos = (pos + 1) % dict->capacity;
        ++dist;
    }
}

int dict_index(struct dict *dict, const char *key)
{
    if (dict->capacity <= 0) return -1;
    const uint32_t hash = lookup3_hash(key);
    uint32_t pos = hash % dict->capacity;
    uint32_t dist = 0;

    while (1) {
        if (!dict->buckets[pos].setted) return -1;
        if (dist > probe_distance(dict, dict->buckets[pos].hash, pos)) {
            return -1;
        }
        if (dict->buckets[pos].hash == hash) {
            if (dict->buckets[pos].deleted) return -1;
            return pos;
        }
        pos = (pos + 1) % dict->capacity;
        dist++;
    }
}

void *dict_get(struct dict *dict, const char *key)
{
    int idx = dict_index(dict, key);
    if (idx == -1) return NULL;
    return dict->buckets[idx].data;
}

void dict_delete(struct dict *dict, const char *key)
{
    int idx = dict_index(dict, key);
    if (idx == -1) return;
    dict->buckets[idx].deleted = 1;
    dict->length--;
}

void dict_free(struct dict *dict)
{
    if (dict->buckets == NULL) return;
    free(dict->buckets);
    memset(dict, 0, sizeof(struct dict));
}

struct dict_iter *dict_next(struct dict *dict, struct dict_iter *iter)
{
    if (iter->idx >= dict->capacity) return NULL;

    struct bucket *b;
    while (iter->idx < dict->capacity) {
        b = &dict->buckets[iter->idx++];
        if (b->deleted || !b->setted) {
            continue;
        }
        iter->value = b->data;
        iter->key = b->key;
        return iter;
    }
    return NULL;
}

void dict_clear(struct dict *dict)
{
    memset(dict->buckets, 0, sizeof(struct bucket) * dict->capacity);
    dict->length = 0;
}

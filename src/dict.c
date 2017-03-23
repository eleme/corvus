#include <string.h>
#include "hash.h"
#include "dict.h"
#include "alloc.h"

#define DICT_BASE_CAPACITY 1024
#define LOAD_FACTOR 0.909

#define PSL(n, hash, pos) (((pos) + (n) - (hash) % (n)) % (n))
#define SWAP(a, b, T) do { T t = a; a = b; b = t; } while (0)

static inline void set_bucket(struct bucket *bucket, uint32_t hash,
        const char *key, void *data)
{
    bucket->deleted = false;
    bucket->setted = true;
    bucket->hash = hash;
    bucket->key = key;
    bucket->data = data;
}

static inline void create_buckets(struct dict *dict)
{
    dict->buckets = cv_calloc(dict->capacity, sizeof(struct bucket));
    dict->resize_threshold = dict->capacity * LOAD_FACTOR;
    dict->length = 0;
}

void dict_init(struct dict *dict)
{
    dict->capacity = DICT_BASE_CAPACITY;
    create_buckets(dict);
}

void dict_resize(struct dict *dict)
{
    struct bucket *bucket, *buckets = dict->buckets;
    uint32_t i, capacity = dict->capacity;

    dict->capacity += DICT_BASE_CAPACITY;
    create_buckets(dict);

    for (i = 0; i < capacity; i++) {
        bucket = &buckets[i];
        if (!bucket->deleted && bucket->setted) {
            dict_set(dict, bucket->key, bucket->data);
        }
    }
    cv_free(buckets);
}

void dict_set(struct dict *dict, const char *key, void *data)
{
    if (key == NULL) {
        return;
    }

    if (dict->length + 1 >= dict->resize_threshold) {
        dict_resize(dict);
    }

    dict->length++;

    uint32_t hash = lookup3_hash(key);
    uint32_t pos = hash % dict->capacity;
    uint32_t probe_dist, dist = 0;

    while (1) {
        struct bucket *bucket = &dict->buckets[pos];

        if (!bucket->setted || (bucket->deleted && bucket->hash == hash)) {
            set_bucket(bucket, hash, key, data);
            return;
        }

        probe_dist = PSL(dict->capacity, bucket->hash, pos);
        if (probe_dist < dist) {
            if (bucket->deleted) {
                set_bucket(bucket, hash, key, data);
                return;
            }
            SWAP(hash, bucket->hash, uint32_t);
            SWAP(key, bucket->key, const char*);
            SWAP(data, bucket->data, void*);

            dist = probe_dist;
        }
        pos = (pos + 1) % dict->capacity;
        ++dist;
    }
}

int dict_index(struct dict *dict, const char *key)
{
    if (key == NULL || dict->capacity <= 0) {
        return -1;
    }
    const uint32_t hash = lookup3_hash(key);
    uint32_t pos = hash % dict->capacity;
    uint32_t dist = 0;

    while (1) {
        struct bucket *b = &dict->buckets[pos];

        if (!b->setted || dist > PSL(dict->capacity, b->hash, pos)) {
            return -1;
        }
        if (strcmp(key, b->key) == 0) {
            return b->deleted ? -1 : pos;
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
    dict->buckets[idx].deleted = true;
    dict->length--;
}

void dict_free(struct dict *dict)
{
    cv_free(dict->buckets);
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

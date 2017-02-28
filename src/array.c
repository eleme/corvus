#include <assert.h>
#include <string.h>
#include "array.h"
#include "alloc.h"

struct vector vector_new()
{
    struct vector v = {
        v.data = cv_malloc(VECTOR_BASIC_SIZE * sizeof(void*)),
        v.size = 0,
        v.capacity = VECTOR_BASIC_SIZE,
    };
    return v;
}

void *vector_at(struct vector *v, size_t index)
{
    assert(index < v->size);
    return v->data[index];
}

void vector_push_back(struct vector *v, void *element)
{
    if (v->size == v->capacity) {
        v->data = cv_realloc(v->data, v->capacity * 2 * sizeof(void*));
        v->capacity *= 2;
    }
    v->data[v->size++] = element;
}

void vector_free(struct vector *v)
{
    cv_free(v->data);
    v->data = NULL;
    v->size = 0;
    v->capacity = 0;
}

void vector_free_all(struct vector *v)
{
    for (size_t i = 0; i != v->size; i++) {
        cv_free(v->data[i]);
    }
    vector_free(v);
}

struct cvstr cvstr_new(size_t capacity)
{
    size_t cap = CVSTR_BASIC_SIZE;
    while (cap < capacity) cap *= 2;
    struct cvstr s = {
        .data = cv_malloc(cap),
        .capacity = cap,
    };
    return s;
}

char *cvstr_move(struct cvstr *s)
{
    char *data = s->data;
    s->data = NULL;
    s->capacity = 0;
    return data;
}

void cvstr_reserve(struct cvstr *s, size_t capacity)
{
    if (s->capacity >= capacity) return;
    size_t cap = s->capacity;
    while (cap < capacity) cap *= 2;
    s->data = cv_realloc(s->data, cap);
    s->capacity = cap;
}

void cvstr_free(struct cvstr *s)
{
    if (s->data == NULL) {
        return;
    }
    cv_free(s->data);
    s->capacity = 0;
}

bool cvstr_full(struct cvstr *s)
{
    return strlen(s->data) + 1 == s->capacity;
}

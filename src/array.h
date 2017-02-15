#ifndef ARRAY_H
#define ARRAY_H

#include <stddef.h>
#include <stdbool.h>

#define VECTOR_BASIC_SIZE 8
#define CVSTR_BASIC_SIZE 8

struct vector {
    void **data;
    size_t size;
    size_t capacity;
};

struct vector vector_new();
void *vector_at(struct vector *v, size_t index);
void vector_push_back(struct vector *v, void *element);
void vector_free(struct vector *v);

// Will free the elements before free data;
void vector_free_all(struct vector *v);

struct cvstr {
    char *data;
    size_t capacity;
};

struct cvstr cvstr_new(size_t capacity);
char *cvstr_move(struct cvstr *s);
void cvstr_reserve(struct cvstr *s, size_t capacity);
void cvstr_free(struct cvstr *s);
bool cvstr_full(struct cvstr *s);

#endif /* end of include guard: ARRAY_H */

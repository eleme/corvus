#include <stdlib.h>
#include <string.h>
#include "logging.h"
#include "alloc.h"

void *cv_raw_malloc(size_t size, const char *file, int line)
{
    void *ptr = je_malloc(size);
    if (ptr == NULL) {
        LOG(ERROR, "Fatal: OOM trying to allocate %d bytes at %s:%d", size,
                file, line);
        abort();
    }
    return ptr;
}

void *cv_raw_calloc(size_t number, size_t size, const char *file, int line)
{
    void *ptr = je_calloc(number, size);    // 创建number个对象, 每个的长度为size bytes
    if (ptr == NULL) {
        LOG(ERROR, "Fatal: OOM trying to allocate %d bytes at %s:%d",
                number * size, file, line);
        abort();
    }
    return ptr;
}

void *cv_raw_realloc(void *ptr, size_t size, const char *file, int line)
{
    void *newptr = je_realloc(ptr, size);
    if (newptr == NULL) {
        LOG(ERROR, "Fatal: OOM trying to allocate %d bytes at %s:%d", size,
                file, line);
        abort();
    }
    return newptr;
}

void cv_free(void *ptr)
{
    je_free(ptr);
}

char *cv_raw_strndup(const char *other, size_t size, const char *file, int line)
{
    char *p = cv_raw_malloc(size + 1, file, line);
    strncpy(p, other, size);
    p[size] = '\0';
    return p;
}

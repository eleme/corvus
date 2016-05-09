#include "alloc.h"
#include "logging.h"

#include <jemalloc/jemalloc.h>

void *cv_raw_malloc(size_t size, const char *file, int line)
{
    void *ptr = je_malloc(size);
    if (ptr == NULL) {
        LOG(ERROR, "Fetal: OOM trying to allocate %d bytes at %s:%d", size,
                file, line);
        abort();
    }
    return ptr;
}

void *cv_raw_calloc(size_t number, size_t size, const char *file, int line)
{
    void *ptr = je_calloc(number, size);
    if (ptr == NULL) {
        LOG(ERROR, "Fetal: OOM trying to allocate %d bytes at %s:%d",
                number * size, file, line);
        abort();
    }
    return ptr;
}

void *cv_raw_realloc(void *ptr, size_t size, const char *file, int line)
{
    void *newptr = je_realloc(ptr, size);
    if (newptr == NULL) {
        LOG(ERROR, "Fetal: OOM trying to allocate %d bytes at %s:%d", size,
                file, line);
        abort();
    }
    return newptr;
}

void cv_free(void *ptr)
{
    je_free(ptr);
}

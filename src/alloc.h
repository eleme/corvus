#ifndef ALLOC_H
#define ALLOC_H

#include <stddef.h>
#include <jemalloc/jemalloc.h>

#define repr(s) str(s)
#define str(s) #s

#define CV_MALLOC_LIB ("jemalloc-" \
        repr(JEMALLOC_VERSION_MAJOR) "." \
        repr(JEMALLOC_VERSION_MINOR) "." \
        repr(JEMALLOC_VERSION_BUGFIX))

#define cv_malloc(size) cv_raw_malloc(size, __FILE__, __LINE__)
#define cv_calloc(number, size) cv_raw_calloc(number, size, __FILE__, __LINE__)
#define cv_realloc(ptr, size) cv_raw_realloc(ptr, size, __FILE__, __LINE__)
#define cv_strndup(other, size) cv_raw_strndup(other, size, __FILE__, __LINE__)

void *cv_raw_malloc(size_t size, const char *file, int line);
void *cv_raw_calloc(size_t number, size_t size, const char *file, int line);
void *cv_raw_realloc(void *ptr, size_t size, const char *file, int line);
void cv_free(void *ptr);
char *cv_raw_strndup(const char *other, size_t size, const char *file, int line);

#endif /* end of include guard: ALLOC_H */

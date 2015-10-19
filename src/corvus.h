#ifndef __MAIN_H
#define __MAIN_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#include "mbuf.h"

struct context {
    /* buffer related */
    uint32_t nfree_mbufq;
    struct mhdr free_mbufq;
    size_t mbuf_chunk_size;
    size_t mbuf_offset;

    /* logging */
    bool syslog;
    int log_level;
};

#endif /* end of include guard: __MAIN_H */

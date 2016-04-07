#ifndef __MBUF_H
#define __MBUF_H

#include <sys/queue.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifndef STAILQ_LAST
#define STAILQ_LAST(head, type, field)                       \
    (STAILQ_EMPTY((head))                                    \
     ?  NULL                                                 \
     : ((struct type *)(void *) ((char *)((head)->stqh_last) \
             - (size_t)(&((struct type *)0)->field))))
#endif

struct context;

struct mbuf {
    TAILQ_ENTRY(mbuf) next;
    uint8_t *pos;
    uint8_t *last;
    uint8_t *start;
    uint8_t *end;
    struct mhdr *queue; // the queue contain the buf
    int64_t fill_time; // the time the buffer filled with data from socket
    int refcount;
};

TAILQ_HEAD(mhdr, mbuf);

struct buf_ptr {
    struct mbuf *buf;
    uint8_t *pos;
};

static inline bool mbuf_empty(struct mbuf *mbuf)
{
    return mbuf->pos == mbuf->last ? true : false;
}

static inline bool mbuf_full(struct mbuf *mbuf)
{
    return mbuf->last == mbuf->end ? true : false;
}

void mbuf_init(struct context *);
struct mbuf *mbuf_get(struct context *);
void mbuf_recycle(struct context *, struct mbuf *);
uint32_t mbuf_read_size(struct mbuf *);
uint32_t mbuf_write_size(struct mbuf *);
void mbuf_destroy(struct context *ctx);
struct mbuf *mbuf_queue_get(struct context *ctx, struct mhdr *q);
void mbuf_range_clear(struct context *ctx, struct buf_ptr ptr[]);
void mbuf_decref(struct context *ctx, struct mbuf **bufs, int n);

#endif

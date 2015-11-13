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
    uint32_t           magic;   /* mbuf magic (const) */
    STAILQ_ENTRY(mbuf) next;    /* next mbuf */
    uint8_t            *pos;    /* read marker */
    uint8_t            *last;   /* write marker */
    uint8_t            *start;  /* start of buffer (const) */
    uint8_t            *end;    /* end of buffer (const) */
    int                refcount;
};

STAILQ_HEAD(mhdr, mbuf);

#define MBUF_MAGIC      0xdeadbeef
#define MBUF_MIN_SIZE   512
#define MBUF_MAX_SIZE   16777216
#define MBUF_SIZE       16384
#define MBUF_HSIZE      sizeof(struct mbuf)

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
size_t mbuf_size(struct context *);
void mbuf_destroy(struct context *ctx);
void mbuf_queue_insert(struct mhdr *, struct mbuf *);
struct mbuf *mbuf_queue_top(struct context *, struct mhdr *);
void mbuf_queue_init(struct mhdr *);
void mbuf_inc_ref(struct mbuf *buf);
void mbuf_dec_ref(struct mbuf *buf);
void mbuf_dec_ref_by(struct mbuf *buf, int count);
void mbuf_queue_copy(struct context *ctx, struct mhdr *q, uint8_t *data, int n);
struct mbuf *mbuf_queue_get(struct context *ctx, struct mhdr *q);

#endif

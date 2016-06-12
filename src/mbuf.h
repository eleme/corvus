#ifndef MBUF_H
#define MBUF_H

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
    int refcount;
};

// tracking the time after reading from client socket
struct buf_time {
    STAILQ_ENTRY(buf_time) next;
    struct context *ctx;
    struct mbuf *buf;
    uint8_t *pos;
    int64_t read_time;
};

TAILQ_HEAD(mhdr, mbuf);
STAILQ_HEAD(buf_time_tqh, buf_time);

struct buf_ptr {
    struct mbuf *buf;
    uint8_t *pos;
};

void mbuf_init(struct context *);
struct mbuf *mbuf_get(struct context *);
void mbuf_recycle(struct context *, struct mbuf *);
uint32_t mbuf_read_size(struct mbuf *);
uint32_t mbuf_write_size(struct mbuf *);
void mbuf_destroy(struct context *ctx);
void mbuf_range_clear(struct context *ctx, struct buf_ptr ptr[]);
void mbuf_decref(struct context *ctx, struct mbuf **bufs, int n);
void buf_time_append(struct context *ctx, struct buf_time_tqh *queue,
        struct mbuf *buf, int64_t read_time);
void buf_time_free(struct buf_time *t);

#endif

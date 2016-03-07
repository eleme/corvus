#include <stdlib.h>
#include <string.h>
#include "corvus.h"
#include "logging.h"

static struct mbuf *_mbuf_get(struct context *ctx)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    if (!TAILQ_EMPTY(&ctx->free_mbufq)) {
        mbuf = TAILQ_FIRST(&ctx->free_mbufq);
        TAILQ_REMOVE(&ctx->free_mbufq, mbuf, next);
        ctx->nfree_mbufq--;
    } else {
        buf = (uint8_t*)malloc(config.bufsize);
        if (buf == NULL) {
            return NULL;
        }

        mbuf = (struct mbuf *)(buf + ctx->mbuf_offset);
    }
    return mbuf;
}

void mbuf_free(struct context *ctx, struct mbuf *mbuf)
{
    uint8_t *buf;

    buf = (uint8_t *)mbuf - ctx->mbuf_offset;
    free(buf);
}

void mbuf_init(struct context *ctx)
{
    ctx->nfree_mbufq = 0;
    TAILQ_INIT(&ctx->free_mbufq);

    ctx->mbuf_offset = config.bufsize - sizeof(struct mbuf);
}

struct mbuf *mbuf_get(struct context *ctx)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    mbuf = _mbuf_get(ctx);
    if (mbuf == NULL) {
        return NULL;
    }

    buf = (uint8_t *)mbuf - ctx->mbuf_offset;
    mbuf->start = buf;
    mbuf->end = buf + ctx->mbuf_offset;

    mbuf->pos = mbuf->start;
    mbuf->last = mbuf->start;
    mbuf->queue = NULL;
    mbuf->refcount = 0;

    TAILQ_NEXT(mbuf, next) = NULL;

    ctx->stats.buffers++;
    return mbuf;
}

void mbuf_recycle(struct context *ctx, struct mbuf *mbuf)
{
    ctx->stats.buffers--;
    TAILQ_NEXT(mbuf, next) = NULL;
    TAILQ_INSERT_HEAD(&ctx->free_mbufq, mbuf, next);
    ctx->nfree_mbufq++;
}

uint32_t mbuf_read_size(struct mbuf *mbuf)
{
    return (uint32_t)(mbuf->last - mbuf->pos);
}

uint32_t mbuf_write_size(struct mbuf *mbuf)
{
    return (uint32_t)(mbuf->end - mbuf->last);
}

void mbuf_destroy(struct context *ctx)
{
    struct mbuf *buf;
    while (!TAILQ_EMPTY(&ctx->free_mbufq)) {
        buf = TAILQ_FIRST(&ctx->free_mbufq);
        TAILQ_REMOVE(&ctx->free_mbufq, buf, next);
        mbuf_free(ctx, buf);
        ctx->nfree_mbufq--;
    }
}

struct mbuf *mbuf_queue_get(struct context *ctx, struct mhdr *q)
{
    struct mbuf *buf = NULL;

    if (!TAILQ_EMPTY(q)) buf = TAILQ_LAST(q, mhdr);

    if (buf == NULL || mbuf_full(buf)) {
        buf = mbuf_get(ctx);
        buf->queue = q;
        TAILQ_INSERT_TAIL(q, buf, next);
    }
    return buf;
}

void mbuf_range_clear(struct context *ctx, struct buf_ptr ptr[])
{
    struct mbuf *n, *b = ptr[0].buf;

    while (b != NULL) {
        n = TAILQ_NEXT(b, next);
        b->refcount--;
        if (b->refcount <= 0) {
            TAILQ_REMOVE(b->queue, b, next);
            mbuf_recycle(ctx, b);
        }
        if (b == ptr[1].buf) break;
        b = n;
    }
    memset(&ptr[0], 0, sizeof(struct buf_ptr));
    memset(&ptr[1], 0, sizeof(struct buf_ptr));
}

void mbuf_decref(struct context *ctx, struct mbuf **bufs, int n)
{
    for (int i = 0; i < n; i++) {
        if (bufs[i] == NULL) {
            continue;
        }
        bufs[i]->refcount--;
        if (bufs[i]->refcount <= 0) {
            TAILQ_REMOVE(bufs[i]->queue, bufs[i], next);
            mbuf_recycle(ctx, bufs[i]);
            bufs[i] = NULL;
        }
    }
}

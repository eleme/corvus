#include <stdlib.h>
#include <string.h>
#include "corvus.h"
#include "logging.h"

static struct mbuf *_mbuf_get(struct context *ctx)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    if (!STAILQ_EMPTY(&ctx->free_mbufq)) {
        mbuf = STAILQ_FIRST(&ctx->free_mbufq);
        STAILQ_REMOVE_HEAD(&ctx->free_mbufq, next);
        ctx->nfree_mbufq--;
        STAILQ_NEXT(mbuf, next) = NULL;
    } else {
        buf = (uint8_t*)malloc(ctx->mbuf_chunk_size);
        if (buf == NULL) {
            return NULL;
        }

        mbuf = (struct mbuf *)(buf + ctx->mbuf_offset);
        mbuf->magic = MBUF_MAGIC;
    }
    return mbuf;
}

static struct mbuf *mbuf_queue_get(struct context *ctx, struct mhdr *q)
{
    struct mbuf *buf = mbuf_queue_top(ctx, q);
    if (mbuf_full(buf)) {
        buf = mbuf_get(ctx);
        mbuf_queue_insert(q, buf);
    }
    return buf;
}

void mbuf_init(struct context *ctx)
{
    ctx->nfree_mbufq = 0;
    STAILQ_INIT(&ctx->free_mbufq);

    ctx->mbuf_chunk_size = MBUF_SIZE;
    ctx->mbuf_offset = ctx->mbuf_chunk_size - sizeof(struct mbuf);
}

static void mbuf_free(struct context *ctx, struct mbuf *mbuf)
{
    uint8_t *buf;

    buf = (uint8_t *)mbuf - ctx->mbuf_offset;
    free(buf);
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
    mbuf->refcount = 0;

    ctx->stats.buffers++;
    return mbuf;
}

void mbuf_recycle(struct context *ctx, struct mbuf *mbuf)
{
    STAILQ_NEXT(mbuf, next) = NULL;
    STAILQ_INSERT_HEAD(&ctx->free_mbufq, mbuf, next);
    ctx->nfree_mbufq++;

    ctx->stats.buffers--;
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
    while (!STAILQ_EMPTY(&ctx->free_mbufq)) {
        buf = STAILQ_FIRST(&ctx->free_mbufq);
        STAILQ_REMOVE_HEAD(&ctx->free_mbufq, next);
        mbuf_free(ctx, buf);
        ctx->nfree_mbufq--;
    }
}

void mbuf_queue_init(struct mhdr *mhdr)
{
    STAILQ_INIT(mhdr);
}

void mbuf_queue_insert(struct mhdr *mhdr, struct mbuf *mbuf)
{
    STAILQ_INSERT_TAIL(mhdr, mbuf, next);
}

struct mbuf *mbuf_queue_top(struct context *ctx, struct mhdr *mhdr)
{
    if (STAILQ_EMPTY(mhdr)) {
        struct mbuf *buf = mbuf_get(ctx);
        mbuf_queue_insert(mhdr, buf);
        return buf;
    }
    return STAILQ_LAST(mhdr, mbuf, next);
}

void mbuf_queue_copy(struct context *ctx, struct mhdr *q, uint8_t *data, int n)
{
    struct mbuf *buf = mbuf_queue_get(ctx, q);
    int remain = n, wlen, size, len = 0;
    while (remain > 0) {
        wlen = mbuf_write_size(buf);
        size = remain < wlen ? remain : wlen;
        memcpy(buf->last, data + len, size);
        buf->last += size;
        len += size;
        remain -= size;
        if (wlen - size <= 0) {
            buf = mbuf_queue_get(ctx, q);
        }
    }
}

void mbuf_inc_ref(struct mbuf *buf)
{
    buf->refcount++;
}

void mbuf_dec_ref(struct mbuf *buf)
{
    buf->refcount--;
    LOG(DEBUG, "%d dec ref", buf->refcount);
}

void mbuf_dec_ref_by(struct mbuf *buf, int count)
{
    buf->refcount -= count;
    LOG(DEBUG, "%d dec ref", buf->refcount);
}

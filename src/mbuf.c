#include <stdlib.h>
#include <string.h>
#include "mbuf.h"
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

void mbuf_init(struct context *ctx)
{
    ctx->nfree_mbufq = 0;
    STAILQ_INIT(&ctx->free_mbufq);

    ctx->mbuf_chunk_size = MBUF_SIZE;
    ctx->mbuf_offset = ctx->mbuf_chunk_size - MBUF_HSIZE;
}

static void mbuf_free(struct context *ctx, struct mbuf *mbuf)
{
    uint8_t *buf;

    buf = (uint8_t *)mbuf - ctx->mbuf_offset;
    free(buf);
}

void mbuf_deinit(struct context *ctx)
{
    while (!STAILQ_EMPTY(&ctx->free_mbufq)) {
        struct mbuf *mbuf = STAILQ_FIRST(&ctx->free_mbufq);
        STAILQ_REMOVE_HEAD(&ctx->free_mbufq, next);
        mbuf_free(ctx, mbuf);
        ctx->nfree_mbufq--;
    }
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

    return mbuf;
}

void mbuf_recycle(struct context *ctx, struct mbuf *mbuf)
{
    STAILQ_INSERT_HEAD(&ctx->free_mbufq, mbuf, next);
    ctx->nfree_mbufq++;
}

void mbuf_rewind(struct mbuf *mbuf)
{
    mbuf->pos = mbuf->start;
    mbuf->last = mbuf->start;
}

uint32_t mbuf_read_size(struct mbuf *mbuf)
{
    return (uint32_t)(mbuf->last - mbuf->pos);
}

uint32_t mbuf_write_size(struct mbuf *mbuf)
{
    return (uint32_t)(mbuf->end - mbuf->last);
}

size_t mbuf_size(struct context *ctx)
{
    return ctx->mbuf_offset;
}

void mbuf_copy(struct mbuf *mbuf, uint8_t *pos, size_t n)
{
    if (n == 0) {
        return;
    }

    memcpy(mbuf->last, pos, n);
    mbuf->last += n;
}

/*
 * Put partial data to the next mbuf.
 */
struct mbuf * mbuf_split(struct context *ctx, struct mbuf *mbuf, uint8_t *pos,
        mbuf_copy_t cb, void *cbarg)
{
    struct mbuf *nbuf;
    size_t size;

    nbuf = mbuf_get(ctx);
    if (nbuf == NULL) {
        return NULL;
    }

    if (cb != NULL) {
        /* precopy nbuf */
        cb(nbuf, cbarg);
    }

    /* copy data from mbuf to nbuf */
    size = (size_t)(mbuf->last - pos);
    mbuf_copy(nbuf, pos, size);

    /* adjust mbuf */
    mbuf->last = pos;

    return nbuf;
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

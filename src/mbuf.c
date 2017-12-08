#include <stdlib.h>
#include <string.h>
#include "corvus.h"
#include "logging.h"
#include "alloc.h"

#define RECYCLE_LENGTH 8192 // 128mb
#define BUF_TIME_LIMIT 512

static struct mbuf *mbuf_create(struct context *ctx)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    if (!TAILQ_EMPTY(&ctx->free_mbufq)) {
        mbuf = TAILQ_FIRST(&ctx->free_mbufq);
        TAILQ_REMOVE(&ctx->free_mbufq, mbuf, next);

        ctx->mstats.free_buffers--;
    } else {
        buf = (uint8_t*)cv_malloc(config.bufsize);
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
    cv_free(buf);
}

void mbuf_init(struct context *ctx)
{
    ctx->mstats.free_buffers = 0;

    TAILQ_INIT(&ctx->free_mbufq);
    ctx->mbuf_offset = config.bufsize - sizeof(struct mbuf);
}

struct mbuf *mbuf_get(struct context *ctx)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    mbuf = mbuf_create(ctx);
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

    ctx->mstats.buffers++;

    return mbuf;
}

void mbuf_recycle(struct context *ctx, struct mbuf *mbuf)
{
    ctx->mstats.buffers--;

    if (ctx->mstats.free_buffers > RECYCLE_LENGTH) {
        mbuf_free(ctx, mbuf);
        return;
    }

    TAILQ_NEXT(mbuf, next) = NULL;
    TAILQ_INSERT_HEAD(&ctx->free_mbufq, mbuf, next);

    ctx->mstats.free_buffers++;
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

        ctx->mstats.free_buffers--;
    }
}

void mbuf_range_clear(struct context *ctx, struct buf_ptr ptr[])
{
    struct mbuf *n, *b = ptr[0].buf;

    while (b != NULL) {
        n = TAILQ_NEXT(b, next);
        b->refcount--;
        if (b->refcount <= 0 && b->pos >= b->last) {
            TAILQ_REMOVE(b->queue, b, next);
            mbuf_recycle(ctx, b);
        }
        if (b == ptr[1].buf) break;
        b = n;
    }
    memset(&ptr[0], 0, sizeof(struct buf_ptr));
    memset(&ptr[1], 0, sizeof(struct buf_ptr));
}

uint32_t mbuf_range_len(struct buf_ptr ptr[2])
{
    uint32_t len = 0;
    struct mbuf *b = ptr[0].buf;
    while (true) {
        uint8_t *start = b->start;
        uint8_t *end = b->end;
        if (b == ptr[0].buf) {
            start = ptr[0].pos;
        }
        if (b == ptr[1].buf) {
            end = ptr[1].pos;
        }
        len += (end - start);
        if (b == ptr[1].buf)
            break;
        b = TAILQ_NEXT(b, next);
    }
    return len;
}

size_t mbuf_range_copy(uint8_t *dest, struct buf_ptr ptr[2], size_t max_len)
{
    struct mbuf *b = ptr[0].buf;
    size_t len = 0;
    while (len < max_len) {
        uint8_t *start = b->start;
        uint8_t *end = b->end;
        if (b == ptr[0].buf) {
            start = ptr[0].pos;
        }
        if (b == ptr[1].buf) {
            end = ptr[1].pos;
        }
        if (end - start > max_len - len) {
            end = start + max_len - len;
        }
        memcpy(dest + len, start, end - start);
        len += (end - start);
        if (b == ptr[1].buf)
            break;
        b = TAILQ_NEXT(b, next);
    }
    return len;
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

// 在把数据读取到缓冲区之后, 把这个缓冲区的相关信息放到queue这个队列中
// buf_time这个结构体用来追踪记录每个从客户端发送过来的请求的耗时
void buf_time_append(struct context *ctx, struct buf_time_tqh *queue,
        struct mbuf *buf, int64_t read_time)
{
    struct buf_time *t;
    // 判断context的free_buf_timeq队列是否为空
    // 1. 为空, 则为buf_time申请内存空间
    // 2. 不为空, 则读取队列第一项
    // 3. 初始化这个buf_time, 加入queue这个队列中
    if (!STAILQ_EMPTY(&ctx->free_buf_timeq)) {
        t = STAILQ_FIRST(&ctx->free_buf_timeq);
        STAILQ_REMOVE_HEAD(&ctx->free_buf_timeq, next);
        ctx->mstats.free_buf_times--;
    } else {
        t = cv_calloc(1, sizeof(struct buf_time));
    }
    t->ctx = ctx;
    t->buf = buf;
    t->pos = buf->last;
    t->read_time = read_time;   // 命令读取的时刻
    STAILQ_INSERT_TAIL(queue, t, next);
    ctx->mstats.buf_times++;
}

void buf_time_free(struct buf_time *t)
{
    t->ctx->mstats.buf_times--;

    if (t->ctx->mstats.free_buf_times > BUF_TIME_LIMIT) {
        cv_free(t);
        return;
    }

    STAILQ_NEXT(t, next) = NULL;
    STAILQ_INSERT_HEAD(&t->ctx->free_buf_timeq, t, next);
    t->ctx->mstats.free_buf_times++;
}

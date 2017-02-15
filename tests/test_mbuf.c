#include "test.h"
#include "alloc.h"

void init_mbuf_for_test(struct mbuf *b, size_t len) {
    uint8_t *p = cv_calloc(len, 1);
    b->start = p;
    b->end = p + len;
    b->pos = p;
    b->last = p;
}

TEST(test_mbuf_range_func) {
    struct mhdr q;
    TAILQ_INIT(&q);
    struct buf_ptr ptr[2];
    struct mbuf b1, b2, b3;
    init_mbuf_for_test(&b1, 10);
    init_mbuf_for_test(&b2, 10);
    init_mbuf_for_test(&b3, 10);
    TAILQ_INSERT_TAIL(&q, &b1, next);
    TAILQ_INSERT_TAIL(&q, &b2, next);
    TAILQ_INSERT_TAIL(&q, &b3, next);
    uint8_t *tmp = NULL;

    // one buf
    uint8_t *a = b1.start + 3;
    memcpy(a, "abcde", 5);
    ptr[0].buf = &b1;
    ptr[0].pos = a;
    ptr[1].buf = &b1;
    ptr[1].pos = b1.start + 8;
    ASSERT(mbuf_range_len(ptr) == 5);
    tmp = cv_calloc(10, 1);
    ASSERT(mbuf_range_copy(tmp, ptr, 10) == 5);
    ASSERT(memcmp(tmp, "abcde", 5) == 0);
    ASSERT(tmp[5] == 0);
    cv_free(tmp);

    // two buf
    memcpy(a, "abcdefg", 7);
    memcpy(b2.start, "12345", 5);
    ptr[1].buf = &b2;
    ptr[1].pos = b2.start + 5;
    ASSERT(mbuf_range_len(ptr) == 7 + 5);
    tmp = cv_calloc(20, 1);
    ASSERT(mbuf_range_copy(tmp, ptr, 20) == 12);
    ASSERT(memcmp(tmp, "abcdefg12345", 12) == 0);
    ASSERT(tmp[12] == 0);
    cv_free(tmp);

    // three buf
    memcpy(b2.start, "0123456789", 10);
    memcpy(b3.start, "crvs", 4);
    ptr[1].buf = &b3;
    ptr[1].pos = b3.start + 4;
    ASSERT(mbuf_range_len(ptr) == 7 + 10 + 4);
    tmp = cv_calloc(30, 1);
    ASSERT(mbuf_range_copy(tmp, ptr, 30) == 21);
    ASSERT(memcmp(tmp, "abcdefg0123456789crvs", 21) == 0);
    ASSERT(tmp[21] == 0);
    cv_free(tmp);

    // with max limit
    uint8_t buf[20] = {0};
    ASSERT(mbuf_range_copy(buf, ptr, 10) == 10);
    ASSERT(memcmp(buf, "abcdefg012", 10) == 0);
    ASSERT(buf[10] == '\0');

    cv_free(b1.start);
    cv_free(b2.start);
    cv_free(b3.start);

    PASS(NULL);
}

TEST_CASE(test_mbuf) {
    RUN_TEST(test_mbuf_range_func);
}

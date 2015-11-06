#include "test.h"
#include <string.h>
#include "corvus.h"
#include "parser.h"
#include "mbuf.h"
#include "logging.h"

TEST(test_nested_array) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    char data[] = "*3\r\n$3\r\nSET\r\n*1\r\n$5\r\nhello\r\n$3\r\n123\r\n";
    size_t len = strlen(data);

    struct mbuf *buf = mbuf_get(&ctx);
    memcpy(buf->last, data, len);
    buf->last += len;

    struct reader *r = reader_init();
    reader_feed(r, buf);

    if (parse(r) == -1) {
        printf("protocol error");
        FAIL(NULL);
    }

    struct redis_data *d = r->data;
    ASSERT(d != NULL);
    ASSERT(d->elements == 3);

    struct redis_data *f1 = d->element[0];
    ASSERT(f1->type == DATA_TYPE_STRING);
    ASSERT(f1->pos->pos_len == 1);
    ASSERT(f1->pos->items[0].len == 3);
    ASSERT(strncmp("SET", (const char *)f1->pos->items[0].str, 3) == 0);

    struct redis_data *f2 = d->element[1];
    ASSERT(f2->type == DATA_TYPE_ARRAY);
    ASSERT(f2->elements == 1);
    ASSERT(f2->element[0]->pos->items[0].len == 5);
    ASSERT(strncmp("hello", (const char *)f2->element[0]->pos->items[0].str, 5) == 0);

    struct redis_data *f3 = d->element[2];
    ASSERT(f3->type == DATA_TYPE_STRING);
    ASSERT(f3->pos->items[0].len == 3);
    ASSERT(strncmp("123", (const char *)f3->pos->items[0].str, 3) == 0);

    PASS(NULL);
}

TEST(test_partial_parse) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    int i;
    char value[16384];
    char data[] = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$16387\r\n";

    for (i = 0; i < (int)sizeof(value); i++) value[i] = 'i';

    struct mbuf *buf = mbuf_get(&ctx);
    size_t len = strlen(data);

    LOG(DEBUG, "%d", buf->end - buf->last);
    memcpy(buf->last, data, len);
    buf->last += len;
    int size = mbuf_write_size(buf);

    memcpy(buf->last, value, mbuf_write_size(buf));
    buf->last += size;

    struct reader *reader = reader_init(&ctx);
    reader_feed(reader, buf);

    if (parse(reader) == -1) {
        printf("Protocol Error");
        FAIL(NULL);
    }

    struct mbuf *buf2 = mbuf_get(&ctx);
    LOG(DEBUG, "%d %d", size, len);
    memcpy(buf2->last, value, 16387 - size);
    buf2->last += 16387 - size;
    memcpy(buf2->last, "\r\n", 2);
    buf2->last += 2;

    reader_feed(reader, buf2);
    if (parse(reader) == -1) {
        printf("Protocol Error");
        FAIL(NULL);
    }

    ASSERT(reader->ready);
    struct redis_data *d = reader->data;
    ASSERT(d->elements == 3);
    ASSERT(d->element[0]->pos->pos_len == 1);
    ASSERT(d->element[0]->pos->items[0].len == 3);
    ASSERT(strncmp("SET", (const char *)d->element[0]->pos->items[0].str, 3) == 0);
    ASSERT(d->element[1]->pos->pos_len == 1);
    ASSERT(d->element[1]->pos->items[0].len == 5);
    ASSERT(strncmp("hello", (const char *)d->element[1]->pos->items[0].str, 5) == 0);
    ASSERT(d->element[2]->pos->pos_len == 2);
    LOG(DEBUG, "%d %d", d->element[2]->pos->items[0].len, d->element[2]->pos->items[1].len);
    ASSERT(d->element[2]->pos->items[0].len == (uint32_t)size);
    ASSERT(strncmp(value, (const char *)d->element[2]->pos->items[0].str, size) == 0);
    uint32_t remain = 16387 - size;
    ASSERT(d->element[2]->pos->items[1].len == remain);
    ASSERT(strncmp(value, (const char *)d->element[2]->pos->items[1].str, remain) == 0);
    PASS(NULL);
}

TEST(test_process_integer) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data[] = ":40235\r\n:231\r\n";
    size_t len = strlen(data);

    buf.pos = data;
    buf.last = data + len;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);
    ASSERT(r->ready == 1);
    ASSERT(r->data != NULL);
    ASSERT(r->data->type == DATA_TYPE_INTEGER);
    ASSERT(r->data->integer == 40235);

    ASSERT(parse(r) != -1);
    ASSERT(r->data->integer == 231);
    PASS(NULL);
}

TEST(test_empty_array) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data[] = "*0\r\n$5\r\nhello\r\n";
    size_t len = strlen(data);

    buf.pos = data;
    buf.last = data + len;
    buf.end = buf.last;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);
    ASSERT(r->ready == 1);
    ASSERT(r->data->type == DATA_TYPE_ARRAY);
    ASSERT(r->data->elements == 0);
    ASSERT(r->data->element == NULL);

    ASSERT(parse(r) != -1);
    PASS(NULL);
}

TEST(test_parse_simple_string) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data1[] = "+O";
    char data2[] = "K\r\n";

    size_t len1 = strlen(data1);
    size_t len2 = strlen(data2);

    buf.pos = data1;
    buf.last = data1 + len1;
    buf.end = buf.last;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);

    struct mbuf buf2;

    buf2.pos = data2;
    buf2.last = data2 + len2;
    buf2.end = buf2.last;

    r->buf = &buf2;
    ASSERT(parse(r) != -1);

    PASS(NULL);
}

TEST(test_parse_error) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data1[] = "-MOV";
    char data2[] = "ED 866 127.0.0.1:8001\r\n";

    size_t len1 = strlen(data1);
    size_t len2 = strlen(data2);

    buf.pos = data1;
    buf.last = data1 + len1;
    buf.end = buf.last;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);

    struct mbuf buf2;

    buf2.pos = data2;
    buf2.last = data2 + len2;
    buf2.end = buf2.last;

    r->buf = &buf2;
    ASSERT(parse(r) != -1);

    PASS(NULL);
}

TEST(test_parse_null_string) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data[] = "$-1\r\n";

    buf.pos = data;
    buf.last = data + strlen(data);
    buf.end = buf.last;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);
    PASS(NULL);
}

TEST(test_parse_null_array) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data[] = "*-1\r\n";

    buf.pos = data;
    buf.last = data + strlen(data);
    buf.end = buf.last;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);
    PASS(NULL);
}

TEST(test_parse_minus_integer) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    struct mbuf buf;

    char data[] = ":-1\r\n";

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + strlen(data);
    buf.end = buf.last;

    struct reader *r = reader_init(&ctx);
    r->buf = &buf;

    ASSERT(parse(r) != -1);
    PASS(NULL);
}

TEST(test_pos_array_compare) {
    struct pos p[2] = {
        {.str = (uint8_t*)"hello", .len = 5},
        {.str = (uint8_t*)"worldl", .len = 6}
    };
    struct pos_array arr = {
        .str_len = 11,
        .pos_len = 2,
        .items = p
    };

    ASSERT(pos_array_compare(&arr, "helloworldl", 11) == 0);
    PASS(NULL);
}

TEST_CASE(test_parser) {
    RUN_TEST(test_nested_array);
    RUN_TEST(test_partial_parse);
    RUN_TEST(test_process_integer);
    RUN_TEST(test_empty_array);
    RUN_TEST(test_parse_simple_string);
    RUN_TEST(test_parse_error);
    RUN_TEST(test_parse_null_string);
    RUN_TEST(test_parse_null_array);
    RUN_TEST(test_parse_minus_integer);
    RUN_TEST(test_pos_array_compare);
}

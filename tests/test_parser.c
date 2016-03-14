#include "test.h"
#include <string.h>
#include "corvus.h"
#include "parser.h"
#include "mbuf.h"
#include "logging.h"

extern int process_type(struct reader *r);
extern int process_array(struct reader *r);
extern int process_string(struct reader *r);
extern int stack_pop(struct reader *r);
extern int stack_push(struct reader *r, int type);

struct mbuf *get_buf(struct context *ctx, char *data)
{
    struct mbuf *buf = mbuf_get(ctx);
    int len = strlen(data);

    memcpy(buf->last, data, len);
    buf->last += len;
    return buf;
}

TEST(test_nested_array) {
    char data[] = "*3\r\n$3\r\nSET\r\n*1\r\n$5\r\nhello\r\n$3\r\n123\r\n";
    size_t len = strlen(data);

    struct mbuf *buf = mbuf_get(ctx);
    memcpy(buf->last, data, len);
    buf->last += len;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, buf);

    ASSERT(parse(&r, MODE_REQ) == 0);

    struct redis_data *d = &r.data;
    ASSERT(d != NULL);
    ASSERT(d->elements == 3);

    struct redis_data *f1 = &d->element[0];
    ASSERT(f1->type == REP_STRING);
    ASSERT(f1->pos.pos_len == 1);
    ASSERT(f1->pos.items[0].len == 3);
    ASSERT(strncmp("SET", (const char *)f1->pos.items[0].str, 3) == 0);

    struct redis_data *f2 = &d->element[1];
    ASSERT(f2->type == REP_ARRAY);
    ASSERT(f2->elements == 1);
    ASSERT(f2->element[0].pos.items[0].len == 5);
    ASSERT(strncmp("hello", (const char *)f2->element[0].pos.items[0].str, 5) == 0);

    struct redis_data *f3 = &d->element[2];
    ASSERT(f3->type == REP_STRING);
    ASSERT(f3->pos.items[0].len == 3);
    ASSERT(strncmp("123", (const char *)f3->pos.items[0].str, 3) == 0);

    mbuf_recycle(ctx, buf);
    reader_free(&r);
    PASS(NULL);
}

TEST(test_partial_parse) {
    int i;
    char value[16384];
    char data[] = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$16387\r\n";

    for (i = 0; i < (int)sizeof(value); i++) value[i] = 'i';

    struct mbuf *buf = mbuf_get(ctx);
    size_t len = strlen(data);

    LOG(DEBUG, "%d", buf->end - buf->last);
    memcpy(buf->last, data, len);
    buf->last += len;
    int size = mbuf_write_size(buf);

    memcpy(buf->last, value, mbuf_write_size(buf));
    buf->last += size;

    struct reader reader;
    reader_init(&reader);
    reader_feed(&reader, buf);

    ASSERT(parse(&reader, MODE_REQ) == 0);

    struct mbuf *buf2 = mbuf_get(ctx);
    LOG(DEBUG, "%d %d", size, len);
    memcpy(buf2->last, value, 16387 - size);
    buf2->last += 16387 - size;
    memcpy(buf2->last, "\r\n", 2);
    buf2->last += 2;

    reader_feed(&reader, buf2);
    ASSERT(parse(&reader, MODE_REQ) == 0);
    ASSERT(reader.ready);

    struct redis_data *d = &reader.data;
    ASSERT(d->elements == 3);
    ASSERT(d->element[0].pos.pos_len == 1);
    ASSERT(d->element[0].pos.items[0].len == 3);
    ASSERT(strncmp("SET", (const char *)d->element[0].pos.items[0].str, 3) == 0);
    ASSERT(d->element[1].pos.pos_len == 1);
    ASSERT(d->element[1].pos.items[0].len == 5);
    ASSERT(strncmp("hello", (const char *)d->element[1].pos.items[0].str, 5) == 0);
    ASSERT(d->element[2].pos.pos_len == 2);
    ASSERT(d->element[2].pos.items[0].len == (uint32_t)size);
    ASSERT(strncmp(value, (const char *)d->element[2].pos.items[0].str, size) == 0);
    uint32_t remain = 16387 - size;
    ASSERT(d->element[2].pos.items[1].len == remain);
    ASSERT(strncmp(value, (const char *)d->element[2].pos.items[1].str, remain) == 0);

    mbuf_recycle(ctx, buf);
    mbuf_recycle(ctx, buf2);
    reader_free(&reader);
    PASS(NULL);
}

TEST(test_process_integer) {
    struct mbuf buf;

    char data[] = ":40235\r\n:231\r\n";
    size_t len = strlen(data);

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + len;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);
    ASSERT(r.ready == 1);
    ASSERT(r.data.type == REP_INTEGER);
    ASSERT(r.data.integer == 40235);

    ASSERT(parse(&r, MODE_REQ) != -1);
    ASSERT(r.data.integer == 231);

    redis_data_free(&r.data);
    reader_free(&r);
    PASS(NULL);
}

TEST(test_empty_array) {
    struct mbuf buf;

    char data[] = "*0\r\n$5\r\nhello\r\n";
    size_t len = strlen(data);

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + len;
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);
    ASSERT(r.ready == 1);
    ASSERT(r.data.type == REP_ARRAY);
    ASSERT(r.data.elements == 0);
    ASSERT(r.data.element == NULL);

    ASSERT(parse(&r, MODE_REQ) != -1);

    redis_data_free(&r.data);
    reader_free(&r);
    PASS(NULL);
}

TEST(test_parse_simple_string) {
    struct mbuf buf;

    char data1[] = "+O";
    char data2[] = "K\r\n";

    size_t len1 = strlen(data1);
    size_t len2 = strlen(data2);

    buf.pos = (uint8_t*)data1;
    buf.last = (uint8_t*)data1 + len1;
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);

    struct mbuf buf2;

    buf2.pos = (uint8_t*)data2;
    buf2.last = (uint8_t*)data2 + len2;
    buf2.end = buf2.last;

    reader_feed(&r, &buf2);
    ASSERT(parse(&r, MODE_REQ) != -1);

    reader_free(&r);
    PASS(NULL);
}

TEST(test_parse_error) {
    struct mbuf buf;

    char data1[] = "-MOVED 866 127.0.0.1:8001\r";
    char data2[] = "\n-MOVED 5333 127.0.0.1:8029\r\n";

    size_t len1 = strlen(data1);
    size_t len2 = strlen(data2);

    buf.pos = (uint8_t*)data1;
    buf.last = (uint8_t*)data1 + len1;
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);

    struct mbuf buf2;

    buf2.pos = (uint8_t*)data2;
    buf2.last = (uint8_t*)data2 + len2;
    buf2.end = buf2.last;

    reader_feed(&r, &buf2);
    ASSERT(parse(&r, MODE_REQ) != -1);

    ASSERT(r.data.pos.str_len == 24);
    ASSERT(r.data.pos.pos_len == 2);
    ASSERT(r.data.pos.items[0].len == 24);
    ASSERT(strncmp((char*)r.data.pos.items[0].str, "MOVED 866 127.0.0.1:8001", 24) == 0);
    ASSERT(r.data.pos.items[1].len == 0);

    reader_free(&r);
    PASS(NULL);
}

TEST(test_parse_null_string) {
    struct mbuf buf;

    char data[] = "$-1\r\n";

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + strlen(data);
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);
    reader_free(&r);
    PASS(NULL);
}

TEST(test_parse_null_array) {
    struct mbuf buf;

    char data[] = "*-1\r\n";

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + strlen(data);
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);

    reader_free(&r);
    PASS(NULL);
}

TEST(test_parse_minus_integer) {
    struct mbuf buf;

    char data[] = ":-1\r\n";

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + strlen(data);
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) != -1);

    reader_free(&r);
    PASS(NULL);
}

TEST(test_wrong_integer) {
    struct mbuf buf;

    char data[] = "*4\r\n:4\n:4\r\n";

    buf.pos = (uint8_t*)data;
    buf.last = (uint8_t*)data + strlen(data);
    buf.end = buf.last;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, &buf);

    ASSERT(parse(&r, MODE_REQ) == -1);

    reader_free(&r);
    PASS(NULL);
}

TEST(test_array_pop) {
    char data[] = "*2\r\n$3\r\nSET\r\n*1\r\n$5\r\nhello\r\n";
    size_t len = strlen(data);

    struct mbuf *buf = mbuf_get(ctx);
    memcpy(buf->last, data, len);
    buf->last += len;

    struct reader r;
    reader_init(&r);
    reader_feed(&r, buf);

    ASSERT(parse(&r, MODE_REP) != -1);
    ASSERT(reader_ready(&r));

    mbuf_recycle(ctx, buf);
    reader_free(&r);
    PASS(NULL);
}

TEST(test_process_array) {
    struct reader reader;
    reader_init(&reader);

    struct mbuf *buf = get_buf(ctx, "*2334\r\n");
    reader_feed(&reader, buf);

    ASSERT(process_type(&reader) == 0);
    reader.mode = MODE_REP;
    ASSERT(process_array(&reader) == 0);
    ASSERT(buf->pos == buf->last);
    ASSERT(reader.item_size == 2334);
    ASSERT(reader.type == PARSE_TYPE);
    ASSERT(reader.sidx == 1);

    struct reader_task *task = &reader.rstack[reader.sidx];
    ASSERT(task->elements == 2334);
    ASSERT(task->data.elements == 2334);
    ASSERT(task->data.element == NULL);

    mbuf_recycle(ctx, buf);
    reader_free(&reader);
    PASS(NULL);
}

TEST(test_process_string) {
    struct reader reader;
    reader_init(&reader);

    struct mbuf *buf = get_buf(ctx, "$6\r\nparser\r\n");
    reader_feed(&reader, buf);

    ASSERT(process_type(&reader) == 0);
    reader.mode = MODE_REP;
    ASSERT(process_string(&reader) == 0);
    ASSERT(buf->pos + 1 == buf->last);
    ASSERT(buf->pos[0] == '\n');
    ASSERT(reader.item_size == 0);
    ASSERT(reader.type == PARSE_END);
    ASSERT(reader.sidx == 0);

    struct reader_task *task = &reader.rstack[reader.sidx];
    ASSERT(task->cur_data == NULL);

    mbuf_recycle(ctx, buf);
    reader_free(&reader);
    PASS(NULL);
}

TEST(test_stack_pop) {
    struct reader r;
    reader_init(&r);

    struct redis_data data[4];

    r.rstack[0].data.integer = 90;

    stack_pop(&r);
    ASSERT(r.data.integer == 90);
    ASSERT(r.sidx == 0);

    stack_push(&r, REP_ARRAY);
    ASSERT(r.sidx == 1);
    r.rstack[1].elements = 2;
    r.rstack[1].data.element = data;

    r.rstack[1].data.element[r.rstack[1].idx++].integer = 12345;
    r.rstack[1].elements--;

    stack_push(&r, REP_ARRAY);
    ASSERT(r.sidx == 2);
    r.rstack[2].elements = 1;
    r.rstack[2].data.element = data + 2;

    stack_push(&r, REP_ARRAY);
    ASSERT(r.sidx == 3);
    r.rstack[3].elements = 1;
    r.rstack[3].data.element = data + 3;

    r.rstack[3].data.element[r.rstack[3].idx++].integer = 345;
    r.rstack[3].elements--;

    stack_pop(&r);
    ASSERT(r.sidx == 0);
    ASSERT(r.data.element[0].integer == 12345);
    ASSERT(r.data.element[1].element[0].element[0].integer == 345);
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
    RUN_TEST(test_wrong_integer);
    RUN_TEST(test_array_pop);
    RUN_TEST(test_process_array);
    RUN_TEST(test_process_string);
    RUN_TEST(test_stack_pop);
}

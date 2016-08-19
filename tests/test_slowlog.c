#include "test.h"
#include "slowlog.h"
#include "alloc.h"


TEST(test_slowlog_create_entry) {
    struct mbuf buf;
    struct command cmd;
    struct reader r = {0};
    const char cmd_data[] = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n";
    char *args[] = {
        "$3\r\nSET\r\n",
        "$4\r\nkey1\r\n",
        "$6\r\nvalue1\r\n",
    };
    const size_t args_len[] = {strlen(args[0]), strlen(args[1]), strlen(args[2])};
    buf.pos = (uint8_t*)cmd_data;
    buf.last = (uint8_t*)cmd_data + strlen(cmd_data);
    reader_init(&r);
    reader_feed(&r, &buf);
    ASSERT(parse(&r, MODE_REQ) != -1);
    cmd.data = r.data;
    cmd.prefix = NULL;

    struct slowlog_entry *entry = slowlog_create_entry(&cmd, 233666);
    ASSERT(entry->latency == 233666);
    ASSERT(entry->refcount == 1);
    ASSERT(entry->argc == 3);
    ASSERT(entry->argv[0].len == args_len[0]);
    ASSERT(entry->argv[1].len == args_len[1]);
    ASSERT(entry->argv[2].len == args_len[2]);
    ASSERT(strncmp(args[0], entry->argv[0].str, args_len[0]) == 0);
    ASSERT(strncmp(args[1], entry->argv[1].str, args_len[1]) == 0);
    ASSERT(strncmp(args[2], entry->argv[2].str, args_len[2]) == 0);
    slowlog_dec_ref(entry);

    redis_data_free(&cmd.data);
    PASS(NULL);
}

TEST(test_slowlog_create_entry_with_prefix) {
    struct command cmd;
    char *args[] = {
        "$3\r\nSET\r\n",
        "$4\r\nkey2\r\n",
        "$6\r\nvalue2\r\n",
    };
    const size_t args_len[] = {strlen(args[0]), strlen(args[1]), strlen(args[2])};
    struct pos key = {args[1], args_len[1]};
    struct pos value = {args[2], args_len[2]};
    struct pos_array key_pos = {
        .items = &key, .pos_len = key.len, .pos_len = 1, .max_pos_size = 1
    };
    struct pos_array value_pos = {
        .items = &value, .pos_len = value.len, .pos_len = 1, .max_pos_size = 1
    };
    struct mbuf buf1 = {
        .pos = args[1], .last = args[1] + args_len[1], .start = args[1], .end = args[1]
    };
    struct mbuf buf2 = {
        .pos = args[2], .last = args[2] + args_len[2], .start = args[2], .end = args[2]
    };
    struct redis_data element[2] = {
        {
            .pos = key_pos, .buf = {
                { .buf = &buf1, .pos = buf1.start },
                { .buf = &buf1, .pos = buf1.last }
            }
        },
        {
            .pos = value_pos, .buf = {
                { .buf = &buf2, .pos = buf2.start },
                { .buf = &buf2, .pos = buf2.last }
            }
        }
    };
    cmd.data.elements = 2;
    cmd.data.element = element;
    cmd.prefix = (char*)rep_set;

    struct slowlog_entry *entry = slowlog_create_entry(&cmd, 9394);
    ASSERT(entry->latency == 9394);
    ASSERT(entry->refcount == 1);
    ASSERT(entry->argc == 3);
    ASSERT(entry->argv[0].len == args_len[0]);
    ASSERT(entry->argv[1].len == args_len[1]);
    ASSERT(entry->argv[2].len == args_len[2]);
    ASSERT(strncmp(entry->argv[0].str, args[0], args_len[0]) == 0);
    ASSERT(strncmp(entry->argv[1].str, args[1], args_len[1]) == 0);
    ASSERT(strncmp(entry->argv[2].str, args[2], args_len[2]) == 0);
    slowlog_dec_ref(entry);

    PASS(NULL);
}

TEST(test_entry_get_set) {
    config.slowlog_max_len = 2;
    config.thread = 1;
    struct slowlog_queue q;
    slowlog_init(&q);

    struct slowlog_entry e1, e2, e3;
    e3.refcount = 2;
    e3.argc = 0;
    e1 = e2 = e3;

    ASSERT(q.curr == 0);
    slowlog_set(&q, &e1);
    ASSERT(q.curr == 1);
    slowlog_set(&q, &e2);
    ASSERT(q.curr == 0);
    ASSERT(e1.refcount == 2);
    ASSERT(e2.refcount == 2);
    ASSERT(q.entries[0] == &e1);
    ASSERT(q.entries[1] == &e2);
    struct slowlog_entry *e4 = slowlog_get(&q, 0);
    ASSERT(e1.refcount == 3);
    ASSERT(&e1 == e4);
    slowlog_set(&q, &e3);
    ASSERT(q.curr == 1);
    ASSERT(q.entries[0] == &e3);
    ASSERT(e1.refcount == 2);
    slowlog_dec_ref(e4);
    ASSERT(e1.refcount == 1);

    slowlog_free(&q);
    PASS(NULL);
}

TEST_CASE(test_slowlog) {
    RUN_TEST(test_slowlog_create_entry);
    RUN_TEST(test_slowlog_create_entry_with_prefix);
    RUN_TEST(test_entry_get_set);
}

#include "test.h"
#include "slowlog.h"
#include "alloc.h"
#include "server.h"


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

    struct slowlog_entry *entry = slowlog_create_entry(&cmd, 233666, 666233);
    ASSERT(entry->remote_latency == 233666);
    ASSERT(entry->total_latency == 666233);
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
            .type = REP_STRING, .pos = key_pos, .buf = {
                { .buf = &buf1, .pos = buf1.start },
                { .buf = &buf1, .pos = buf1.last }
            }
        },
        {
            .type = REP_STRING, .pos = value_pos, .buf = {
                { .buf = &buf2, .pos = buf2.start },
                { .buf = &buf2, .pos = buf2.last }
            }
        }
    };
    cmd.data.elements = 2;
    cmd.data.element = element;
    cmd.data.type = REP_ARRAY;
    cmd.prefix = (char*)rep_set;

    struct slowlog_entry *entry = slowlog_create_entry(&cmd, 9394, 9493);
    ASSERT(entry->remote_latency == 9394);
    ASSERT(entry->total_latency == 9493);
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

TEST(test_slowlog_create_entry_with_long_arg) {
    struct mbuf buf;
    struct command cmd;
    struct reader r = {0};
    const char cmd_data[] = "*37\r\n$4\r\nMGET\r\n"
        "$144\r\n"
        "1234567890qwertyuiopasdfghjklzxcvbnm"
        "1234567890qwertyuiopasdfghjklzxcvbnm"
        "1234567890qwertyuiopasdfghjklzxcvbnm"
        "1234567890qwertyuiopasdfghjklzxcvbnm\r\n"
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n" // "a" * 5
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n"
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n"
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n"
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n"
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n"
        "$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n$1\r\na\r\n";
    char a[] = "$1\r\na\r\n";
    char long_value[] =
        "$120\r\n"
        "1234567890qwertyuiopasdfghjklzxcvbnm"
        "1234567890qwertyuiopasdfghjklzxcvbnm"
        "1234567890qwertyuiopasdfghjklzxcvbnm"
        "1(144 bytes)\r\n";
        "\r\n";
    char tail[] = "$23\r\n(37 arguments in total)\r\n";

    buf.pos = (uint8_t*)cmd_data;
    buf.last = (uint8_t*)cmd_data + strlen(cmd_data);
    reader_init(&r);
    reader_feed(&r, &buf);
    ASSERT(parse(&r, MODE_REQ) != -1);
    cmd.data = r.data;
    cmd.prefix = NULL;

    struct slowlog_entry *entry = slowlog_create_entry(&cmd, 233666, 666233);
    ASSERT(SLOWLOG_ENTRY_MAX_STRING == strlen(long_value));
    ASSERT(entry->argv[1].len == strlen(long_value));
    ASSERT(strncmp(long_value, entry->argv[1].str, strlen(long_value)) == 0);
    ASSERT(entry->argv[30].len == strlen(a));
    ASSERT(strncmp(a, entry->argv[30].str, strlen(a)) == 0);
    ASSERT(entry->argv[31].len == strlen(tail));
    ASSERT(strncmp(tail, entry->argv[31].str, strlen(tail)) == 0);
    slowlog_dec_ref(entry);

    redis_data_free(&cmd.data);
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

TEST(test_slowlog_statsd) {
    slowlog_init_stats();

    struct connection *server = server_create(ctx, -1);

    // init by conn_create_server
    TAILQ_INSERT_TAIL(&ctx->servers, server, next);
    extern const size_t CMD_NUM;
    uint32_t slow_cmd_counts[CMD_NUM];
    memset(slow_cmd_counts, 0, sizeof slow_cmd_counts);
    server->info->slow_cmd_counts = slow_cmd_counts;
    strcpy(server->info->dsn, "localhost");

    struct command *cmd = cmd_create(ctx);
    struct command *multi_key_cmd = cmd_create(ctx);
    cmd->server = server;
    cmd->cmd_type = CMD_GET;
    multi_key_cmd->cmd_type = CMD_MSET;

    slowlog_add_count(cmd);
    slowlog_add_count(multi_key_cmd);
    slowlog_add_count(multi_key_cmd);
    ASSERT(slow_cmd_counts[CMD_GET] == 1);

    slowlog_prepare_stats(ctx);
    extern struct dict slow_counts;
    extern uint32_t *counts_sum;

    uint32_t *count = dict_get(&slow_counts, "localhost");
    ASSERT(count[CMD_GET] == 1);
    ASSERT(counts_sum[CMD_GET] == 1);
    ASSERT(slow_cmd_counts[CMD_GET] == 0);
    ASSERT(counts_sum[CMD_MSET] == 2);

    cmd_free(cmd);
    cmd_free(multi_key_cmd);
    TAILQ_REMOVE(&ctx->servers, server, next);
    conn_free(server);
    conn_recycle(ctx, server);
    slowlog_free_stats();
    PASS(NULL);
}

TEST(test_failed_command_slowlog) {
    struct mbuf buf;
    struct command cmd;
    memset(&cmd, 0, sizeof(struct command));
    STAILQ_INIT(&cmd.sub_cmds);

    struct reader r = {0};
    const char cmd_data[] = "*2\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n";
    buf.pos = (uint8_t*)cmd_data;
    buf.last = (uint8_t*)cmd_data + strlen(cmd_data);
    reader_init(&r);
    reader_feed(&r, &buf);
    ASSERT(parse(&r, MODE_REQ) != -1);
    cmd.data = r.data;
    cmd.prefix = NULL;

    struct command sub_cmd;
    sub_cmd.parent = &cmd;
    STAILQ_INSERT_TAIL(&cmd.sub_cmds, &sub_cmd, sub_cmd_next);

    // When corvus fails to redirect command, these two fields may be zero.
    cmd.rep_time[0] = 0;
    cmd.rep_time[1] = 0;
    sub_cmd.rep_time[0] = 0;
    sub_cmd.rep_time[1] = 0;

    struct slowlog_entry *entry = slowlog_create_entry(&cmd, 0, 666233);
    ASSERT(entry->remote_latency == 0);
    ASSERT(entry->total_latency == 666233);
    slowlog_dec_ref(entry);
    entry = slowlog_create_sub_entry(&cmd, 666233);
    ASSERT(NULL == entry);

    redis_data_free(&cmd.data);
    PASS(NULL);
}

TEST_CASE(test_slowlog) {
    RUN_TEST(test_slowlog_create_entry);
    RUN_TEST(test_slowlog_create_entry_with_prefix);
    RUN_TEST(test_slowlog_create_entry_with_long_arg);
    RUN_TEST(test_slowlog_statsd);
    RUN_TEST(test_entry_get_set);
    RUN_TEST(test_failed_command_slowlog);
}

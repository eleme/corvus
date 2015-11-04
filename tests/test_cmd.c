#include "test.h"
#include "parser.h"
#include "corvus.h"
#include "command.h"
#include "logging.h"

TEST(test_parse_redirect) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    char data1[] = "MOV";
    char data2[] = "ED 866 127.0.0.1:8001";

    size_t len1 = strlen(data1);
    size_t len2 = strlen(data2);

    struct pos items[2];
    items[0].len = len1;
    items[0].str = (uint8_t*)data1;
    items[1].len = len2;
    items[1].str = (uint8_t*)data2;

    struct pos_array pos = {.pos_len = 2, .items = items};

    struct redis_data data = {.type = DATA_TYPE_ERROR, .pos = &pos};
    struct command cmd = {.rep_data = &data};

    cmd_parse_redirect(&cmd);
    PASS(NULL);
}

TEST_CASE(test_cmd) {
    RUN_TEST(test_parse_redirect);
}

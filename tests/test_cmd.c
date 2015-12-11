#include "test.h"
#include "parser.h"
#include "corvus.h"
#include "command.h"
#include "logging.h"

TEST(test_parse_redirect) {
    char data1[] = "MOV";
    char data2[] = "ED 866 127.0.0.1:8001";

    size_t len1 = strlen(data1);
    size_t len2 = strlen(data2);

    struct pos items[2];
    items[0].len = len1;
    items[0].str = (uint8_t*)data1;
    items[1].len = len2;
    items[1].str = (uint8_t*)data2;

    struct pos_array pos = {.str_len = len1 + len2, .pos_len = 2, .items = items};

    struct redis_data data = {.type = REP_ERROR};
    memcpy(&data.pos, &pos, sizeof(pos));
    struct command cmd;
    memcpy(&cmd.rep_data, &data, sizeof(data));

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    cmd_parse_redirect(&cmd, &info);
    ASSERT(strncmp(info.addr, "127.0.0.1:8001", 14) == 0);
    ASSERT(info.slot == 866);
    ASSERT(info.type == CMD_ERR_MOVED);
    PASS(NULL);
}

TEST_CASE(test_cmd) {
    RUN_TEST(test_parse_redirect);
}

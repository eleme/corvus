#include <unistd.h>
#include <string.h>
#include "test.h"
#include "slot.h"
#include "parser.h"
#include "corvus.h"
#include "logging.h"
#include "connection.h"
#include "socket.h"
#include "slot.h"

TEST(test_slot_get1) {
    struct pos p[] = {
        {.len = 6, .str = (uint8_t*)"world{"},
        {.len = 6, .str = (uint8_t*)"h}ello"}
    };
    struct pos_array arr = {.str_len = 12, .items = p, .pos_len = 2};
    uint16_t slot = slot_get(&arr);
    LOG(ERROR, "%d", slot);

    ASSERT(slot == 11694);
    ASSERT(strncmp((char*)p[0].str, "world{", 6) == 0);
    ASSERT(strncmp((char*)p[1].str, "h}ello", 6) == 0);
    ASSERT(p[0].len == 6);
    ASSERT(p[1].len == 6);

    PASS(NULL);
}

TEST(test_slot_get2) {
    struct pos p[] = {
        {.len = 6, .str = (uint8_t*)"wo{rld}"},
        {.len = 6, .str = (uint8_t*)"hiello"}
    };
    struct pos_array arr = {.str_len = 12, .items = p, .pos_len = 2};
    uint16_t slot = slot_get(&arr);
    LOG(ERROR, "%d", slot);

    ASSERT(slot == 13668);
    ASSERT(strncmp((char*)p[0].str, "wo{rld}", 6) == 0);
    ASSERT(strncmp((char*)p[1].str, "hiello", 6) == 0);
    ASSERT(p[0].len == 6);
    ASSERT(p[1].len == 6);

    PASS(NULL);
}

TEST(test_socket_addr) {
    struct address addr;
    socket_get_addr("localhost", 9, 8000, &addr);
    ASSERT(strncmp(addr.host, "localhost", 9) == 0);
    ASSERT(addr.port == 8000);
    PASS(NULL);
}

TEST_CASE(test_slot) {
    RUN_TEST(test_slot_get1);
    RUN_TEST(test_slot_get2);
    RUN_TEST(test_socket_addr);
}

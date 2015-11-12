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

TEST(test_slot_get) {
    struct pos p[] = {
        {.len = 6, .str = (uint8_t*)"world{"},
        {.len = 6, .str = (uint8_t*)"h}ello"}
    };
    struct pos_array arr;
    arr.str_len = 12;
    arr.items = p;
    arr.pos_len = 2;
    uint16_t slot = slot_get(&arr);

    ASSERT(slot == 11694);

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
    RUN_TEST(test_slot_get);
    RUN_TEST(test_socket_addr);
}

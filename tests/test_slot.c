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
        {.len = 6, .str = "world{"},
        {.len = 6, .str = "h}ello"}
    };
    struct pos_array arr;
    arr.str_len = 12;
    arr.items = p;
    arr.pos_len = 2;
    uint16_t slot = slot_get(&arr);

    ASSERT(slot == 11694);

    PASS(NULL);
}

TEST(test_thread) {
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    ASSERT(slot_init_updater(false, DEBUG) != -1);

    struct node_conf *conf = malloc(sizeof(struct node_conf));
    conf->nodes = malloc(sizeof(char*) * 2);
    conf->nodes[0] = "localhost:8000";
    conf->nodes[1] = "localhost:8001";
    conf->len = 2;
    slot_create_job(SLOT_UPDATE_INIT, conf);
    conf = NULL;
    sleep(1);
    slot_kill_updater();

    PASS(NULL);
}

TEST(test_socket_addr) {
    struct sockaddr addr;
    socket_get_addr("localhost", 8000, &addr);
    ASSERT(strncmp("\037@\177\000\000\001\000\000\000\000\000\000\000",
                addr.sa_data, 13) == 0);
    PASS(NULL);
}

TEST_CASE(test_slot) {
    RUN_TEST(test_slot_get);
    RUN_TEST(test_thread);
    RUN_TEST(test_socket_addr);
}

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
#include "alloc.h"

extern int split_node_description(struct node_desc *desc, struct pos_array *pos_array);
extern int parse_cluster_nodes(struct redis_data *data);

TEST(test_slot_get1) {
    struct pos p[] = {
        {.len = 6, .str = (uint8_t*)"world{"},
        {.len = 6, .str = (uint8_t*)"h}ello"}
    };
    struct pos_array arr = {.str_len = 12, .items = p, .pos_len = 2};

    uint16_t slot = slot_get(&arr);

    ASSERT(slot == 11694);
    /* original pos should not be changed */
    ASSERT(strncmp((char*)p[0].str, "world{", 6) == 0);
    ASSERT(strncmp((char*)p[1].str, "h}ello", 6) == 0);
    ASSERT(p[0].len == 6);
    ASSERT(p[1].len == 6);

    PASS(NULL);
}

TEST(test_slot_get2) {
    struct pos p[] = {
        {.len = 7, .str = (uint8_t*)"wo{rld}"},
        {.len = 6, .str = (uint8_t*)"hiello"}
    };
    struct pos_array arr = {.str_len = 12, .items = p, .pos_len = 2};
    uint16_t slot = slot_get(&arr);

    ASSERT(slot == 5133);
    ASSERT(strncmp((char*)p[0].str, "wo{rld}", 7) == 0);
    ASSERT(strncmp((char*)p[1].str, "hiello", 6) == 0);
    ASSERT(p[0].len == 7);
    ASSERT(p[1].len == 6);

    PASS(NULL);
}

TEST(test_slot_get3) {
    struct pos p[] = {
        {.len = 12, .str = (uint8_t*)"hello{}world"},
    };
    struct pos_array arr = {.str_len = 12, .items = p, .pos_len = 1};
    uint16_t slot = slot_get(&arr);
    ASSERT(slot == 7485);

    PASS(NULL);
}

TEST(test_split_node_description) {
    char data1[] = "298381934bd7f45ae7a59f9f6e3c9f3c0268536c 127.0.0.1:8003 sl";
    char data2[] = "ave 2033f1738f69a650f315f54b67cfea676734e42a 0 1464233594838 4 connected";
    char data3[] = "\ne24eadaf1521a09e0767862e467d7b52a0f269bd 127.0.0.";
    char data4[] = "1:8004 slave 0552289a1";
    char data5[] = "93de9d2a93de8a62e523dcb45753cf7 0 1464233595848 5 conne";
    char data6[] = "cted\n4f6d838441c4f652f970cd7570c0cf16bbd0f3a9";
    char data7[] = " 127.0.0.1:8001 master - 0 1464233596352 2 connected 5462-109";
    char data8[] = "22\n97967866c7d640d1049f1b8f2754580e3fabd691 ";
    char data9[] = "127.0.0.1:8005 slave 4f6d838441c4f652f970cd7570c0cf16bbd0f3a9 ";
    char data10[] = "0 1464233593828 6 connected\nd0a7e68393";
    char data11[] = "214e5e8379dab75845b5cc89595909 127.0.0.1:8006 slave 20";
    char data12[] = "33f1738f69a650f315f54b67cfea676734e42a 0 146";
    char data13[] = "4233595343 1 connected\n2033f1738f69a650f315f54b67cfea676734e42a 127.0.0.1:8000 myself,";
    char data14[] = "master - 0 0 1 connected 0-5461\n0552289a193de9d2a93de8a";
    char data15[] = "62e523dcb45753cf7 127.0.0.1:8002 master - 0 1464233592817 3 connected 10923-16383\n";

    struct pos p[15] = {
        {(uint8_t*)data1, strlen(data1)},
        {(uint8_t*)data2, strlen(data2)},
        {(uint8_t*)data3, strlen(data3)},
        {(uint8_t*)data4, strlen(data4)},
        {(uint8_t*)data5, strlen(data5)},
        {(uint8_t*)data6, strlen(data6)},
        {(uint8_t*)data7, strlen(data7)},
        {(uint8_t*)data8, strlen(data8)},
        {(uint8_t*)data9, strlen(data9)},
        {(uint8_t*)data10, strlen(data10)},
        {(uint8_t*)data11, strlen(data11)},
        {(uint8_t*)data12, strlen(data12)},
        {(uint8_t*)data13, strlen(data13)},
        {(uint8_t*)data14, strlen(data14)},
        {(uint8_t*)data15, strlen(data15)},
    };
    struct pos_array pos = {p, 828, 15, 0};
    struct node_desc desc[REDIS_CLUSTER_SLOTS];
    memset(desc, 0, sizeof(desc));
    int n = split_node_description(desc, &pos);
    ASSERT(n == 7);
    ASSERT(desc[0].index == 8);
    ASSERT(strncmp(desc[0].parts[0].data, "298381934bd7f45ae7a59f9f6e3c9f3c0268536c",
                desc[0].parts[0].len) == 0);
    for (int i = 0; i < n; i++) {
        cv_free(desc[i].parts);
    }
    PASS(NULL);
}

TEST(test_parse_cluster_nodes) {
    char data[] = "4f6d838441c4f652f970cd7570c0cf16bbd0f3a9 127.0.0.1:8001 "
                  "master - 0 1464764873814 9 connected "
                  "0 2-3 5-8 10 20-40 43 45-50 52 54 56 58 60 62 64 66-67 69 5497-5501\n";
    struct pos p[] = {{(uint8_t*)data, strlen(data)}};
    struct pos_array pos = {p, strlen(data), 1, 0};
    struct redis_data redis_data;
    redis_data.type = REP_STRING;
    memcpy(&redis_data.pos, &pos, sizeof(pos));
    int count = parse_cluster_nodes(&redis_data);
    ASSERT(count == 51);

    struct node_info info;
    ASSERT(slot_get_node_addr(5499, &info));
    ASSERT(strcmp(info.nodes[0].ip, "127.0.0.1") == 0 && info.nodes[0].port == 8001);
    ASSERT(!slot_get_node_addr(9, &info));

    PASS(NULL);
}

TEST(test_parse_cluster_nodes_slave) {
    char data[] = "4f6d838441c4f652f970cd7570c0cf16bbd0f3a9 127.0.0.1:8001 "
                  "master - 0 1464764873814 9 connected 0\n"
                  "41d62ab2b6fdf0f248571ff097c8d770c611cfbc 127.0.0.1:8003 "
                  "slave 4f6d838441c4f652f970cd7570c0cf16bbd0f3a9 0 1464775965124 3 connected\n";

    struct pos p[] = {{(uint8_t*)data, strlen(data)}};
    struct pos_array pos = {p, strlen(data), 1, 0};
    struct redis_data redis_data;
    redis_data.type = REP_STRING;
    memcpy(&redis_data.pos, &pos, sizeof(pos));
    int count = parse_cluster_nodes(&redis_data);
    ASSERT(count == 1);

    struct node_info info;
    ASSERT(slot_get_node_addr(0, &info));
    ASSERT(strcmp(info.nodes[0].ip, "127.0.0.1") == 0 && info.nodes[0].port == 8001);
    ASSERT(info.index == 2);
    ASSERT(strcmp(info.nodes[1].ip, "127.0.0.1") == 0 && info.nodes[1].port == 8003);

    PASS(NULL);
}

TEST(test_parse_cluster_nodes_fail_slave) {
    char data[] = "4f6d838441c4f652f970cd7570c0cf16bbd0f3a9 127.0.0.1:8001 "
                  "master - 0 1464764873814 9 connected 0\n"
                  "41d62ab2b6fdf0f248571ff097c8d770c611cfbc 127.0.0.1:8003 "
                  "slave,fail 4f6d838441c4f652f970cd7570c0cf16bbd0f3a9 0 1464775965124 3 connected\n";

    struct pos p[] = {{(uint8_t*)data, strlen(data)}};
    struct pos_array pos = {p, strlen(data), 1, 0};
    struct redis_data redis_data;
    redis_data.type = REP_STRING;
    memcpy(&redis_data.pos, &pos, sizeof(pos));
    int count = parse_cluster_nodes(&redis_data);
    ASSERT(count == 1);

    struct node_info info;
    ASSERT(slot_get_node_addr(0, &info));
    ASSERT(strcmp(info.nodes[0].ip, "127.0.0.1") == 0 && info.nodes[0].port == 8001);
    ASSERT(info.index == 1);

    PASS(NULL);
}

TEST_CASE(test_slot) {
    RUN_TEST(test_slot_get1);
    RUN_TEST(test_slot_get2);
    RUN_TEST(test_slot_get3);
    RUN_TEST(test_split_node_description);
    RUN_TEST(test_parse_cluster_nodes);
    RUN_TEST(test_parse_cluster_nodes_slave);
    RUN_TEST(test_parse_cluster_nodes_fail_slave);
}

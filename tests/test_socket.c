#include "test.h"
#include "socket.h"

TEST(test_socket_address_init) {
    struct address address;

    char ip[] = "127.0.0.2";
    socket_address_init(&address, ip, strlen(ip), 12345);

    ASSERT(strcmp(address.ip, ip) == 0);
    ASSERT(address.port == 12345);
    PASS(NULL);
}

TEST(test_parse_port) {
    uint16_t port;

    ASSERT(socket_parse_port("abcd", &port) == -1);
    ASSERT(socket_parse_port("123455", &port) == -1);
    ASSERT(socket_parse_port("-234", &port) == -1);
    ASSERT(socket_parse_port("12345", &port) == 0);
    ASSERT(port == 12345);

    ASSERT(socket_parse_port("", &port) == -1);

    char *d6 = ":";
    ASSERT(socket_parse_port(d6 + 1, &port) == -1);

    ASSERT(socket_parse_port("12345a", &port) == -1);

    ASSERT(socket_parse_port("4242@12345", &port) == 0);
    ASSERT(port == 4242);

    PASS(NULL);
}

TEST(test_socket_parse_addr) {
    struct address address;
    memset(&address, 0, sizeof(address));

    ASSERT(socket_parse_addr("127.0.0.1:8000", &address) == 8000);
    ASSERT(strcmp(address.ip, "127.0.0.1") == 0);
    ASSERT(address.port == 8000);

    ASSERT(socket_parse_addr("234.233.1.1:65511", &address) == 65511);
    ASSERT(strcmp(address.ip, "234.233.1.1") == 0);
    ASSERT(address.port == 65511);

    ASSERT(socket_parse_addr("234.233.1.1:65511@6379", &address) == 65511);
    ASSERT(strcmp(address.ip, "234.233.1.1") == 0);
    ASSERT(address.port == 65511);

    PASS(NULL);
}

TEST(test_socket_parse_addr_wrong) {
    struct address address;

    ASSERT(socket_parse_addr(":12345", &address) == 12345);
    ASSERT(socket_parse_addr("127.0.0.1:", &address) == -1);
    ASSERT(socket_parse_addr("127.0.0.1:65536", &address) == -1);
    ASSERT(socket_parse_addr("127.0.0.1:abcd", &address) == -1);
    ASSERT(socket_parse_addr("P\343\377\377\377\177\000\000@\342", &address) == -1);

    PASS(NULL);
}

TEST(test_socket_compare) {
    struct address addr1;
    struct address addr2;
    socket_parse_addr("127.0.0.1:12345", &addr1);
    socket_parse_addr("127.0.0.1:12345", &addr2);
    ASSERT(socket_cmp(&addr1, &addr2) == 0);

    socket_parse_addr("127.0.0.1:1234", &addr2);
    ASSERT(socket_cmp(&addr1, &addr2) == 1);

    socket_parse_addr("127.0.0.2:12345", &addr2);
    ASSERT(socket_cmp(&addr1, &addr2) == 1);
}

TEST_CASE(test_socket) {
    RUN_TEST(test_socket_address_init);
    RUN_TEST(test_parse_port);
    RUN_TEST(test_socket_parse_addr);
    RUN_TEST(test_socket_parse_addr_wrong);
    RUN_TEST(test_socket_compare);
}

#include "test.h"
#include "socket.h"

TEST(test_socket_address_init) {
    struct address address;

    char host[] = "127.0.0.2";
    socket_address_init(&address, host, strlen(host), 12345);

    ASSERT(strcmp(address.host, host) == 0);
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

    PASS(NULL);
}

TEST(test_socket_parse_addr) {
    struct address address;
    memset(&address, 0, sizeof(address));

    socket_parse_addr("localhost:8000", &address);
    ASSERT(strcmp(address.host, "localhost") == 0);
    ASSERT(address.port == 8000);

    socket_parse_addr(":12345", &address);
    ASSERT(strcmp(address.host, "") == 0);
    ASSERT(address.port == 12345);

    PASS(NULL);
}

TEST(test_socket_parse_addr_wrong) {
    struct address address;
    int s = 100;

    s = socket_parse_addr("localhost:", &address);
    ASSERT(s == CORVUS_ERR);

    s = socket_parse_addr("localhost:65536", &address);
    ASSERT(s == CORVUS_ERR);

    s = socket_parse_addr("localhost:abcd", &address);
    ASSERT(s == CORVUS_ERR);

    s = socket_parse_addr("P\343\377\377\377\177\000\000@\342", &address);
    ASSERT(s == CORVUS_ERR);
    PASS(NULL);
}

TEST_CASE(test_socket) {
    RUN_TEST(test_socket_address_init);
    RUN_TEST(test_parse_port);
    RUN_TEST(test_socket_parse_addr);
    RUN_TEST(test_socket_parse_addr_wrong);
}

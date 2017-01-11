#include "test.h"
#include "alloc.h"
#include "config.h"

TEST(test_config_bind) {
    char n[] = "bind";

    ASSERT(config_add(n, "123456") == -1);
    ASSERT(config_add(n, "123asf") == -1);
    ASSERT(config_add(n, "-1243") == -1);
    ASSERT(config_add(n, "") == -1);
    ASSERT(config_add(n, "abc") == -1);
    ASSERT(config_add(n, "2345") == 0);
    ASSERT(config.bind == 2345);

    PASS(NULL);
}

TEST(test_config_syslog) {
    char n[] = "syslog";

    ASSERT(config_add(n, "1") == 0);
    ASSERT(config.syslog == 1);
    ASSERT(config_add(n, "4") == 0);
    ASSERT(config.syslog == 1);
    ASSERT(config_add(n, "0") == 0);
    ASSERT(config.syslog == 0);

    PASS(NULL);
}

TEST(test_config_requirepass) {
    char n[] = "requirepass";

    ASSERT(config_add(n, "") == 0);
    ASSERT(config.requirepass == NULL);
    ASSERT(config_add(n, "123") == 0);
    ASSERT(strcmp(config.requirepass, "123") == 0);

    cv_free(config.requirepass);
    PASS(NULL);
}

TEST(test_config_read_strategy) {
    char n[] = "read-strategy";

    ASSERT(config_add(n, "read-slave-only") == 0);
    ASSERT(config.readslave && !config.readmasterslave);
    ASSERT(config_add(n, "both") == 0);
    ASSERT(config.readslave && config.readmasterslave);
    ASSERT(config_add(n, "master") == 0);
    ASSERT(!config.readslave && !config.readmasterslave);

    PASS(NULL);
}

void config_node_to_str(char *str, size_t max_len);

TEST(test_config_node_to_str) {
    struct address a1 = {"127.0.0.1", 8080};
    struct address addrs[] = {
        {"127.0.0.1", 1111},
        {"127.0.0.2", 2222},
        {"127.0.0.3", 3333},
    };

    struct node_conf *tmp_node = config.node;
    tmp_node->refcount++;

    struct node_conf *node = cv_malloc(sizeof(struct node_conf));
    node->addr = cv_malloc(sizeof(struct address));
    node->addr[0] = a1;
    node->len = 1;
    node->refcount = 1;
    config_set_node(node);
    char buf[1024];
    config_node_to_str(buf, 1024);
    ASSERT(node->refcount == 1);
    ASSERT(strcmp("127.0.0.1:8080", buf) == 0);

    cv_free(node->addr);
    node->addr = cv_malloc(3 * sizeof(struct address));
    memcpy(node->addr, addrs, 3 * sizeof(struct address));
    node->len = 3;
    config_node_to_str(buf, 1024);
    ASSERT(node->refcount == 1);
    ASSERT(strcmp("127.0.0.1:1111,127.0.0.2:2222,127.0.0.3:3333", buf) == 0);

    config_set_node(tmp_node);  // free the node we just setted
    PASS(NULL);
}

TEST(test_config_change) {
    struct corvus_config tmp = config;
    tmp.node->refcount++;
    char buf[1024];

#define ASSERT_CONFIG(option, value) do { \
    ASSERT(config_add(option, value) == CORVUS_OK); \
    ASSERT(config_get(option, buf, 1024) == CORVUS_OK); \
    ASSERT(strcmp(buf, value) == 0); \
} while (0)

    ASSERT_CONFIG("cluster", "cluster_name");
    ASSERT_CONFIG("bind", "8080");
    ASSERT_CONFIG("node", "127.0.0.1:1111,127.0.0.1:2222");
    ASSERT_CONFIG("thread", "233");
    ASSERT_CONFIG("loglevel", "debug");
    ASSERT_CONFIG("syslog", "true");
    ASSERT_CONFIG("statsd", "www.somewhere.com");
    ASSERT_CONFIG("metric_interval", "10");
    // ignore the password
    ASSERT_CONFIG("client_timeout", "233");
    ASSERT_CONFIG("server_timeout", "666");
    ASSERT_CONFIG("bufsize", "23333");
    ASSERT_CONFIG("slowlog-log-slower-than", "12345");
    ASSERT_CONFIG("slowlog-max-len", "1024");
    ASSERT_CONFIG("slowlog-statsd-enabled", "true");

    ASSERT_CONFIG("read-strategy", "master");
    ASSERT_CONFIG("read-strategy", "read-slave-only");
    ASSERT_CONFIG("read-strategy", "both");

    config_set_node(tmp.node);  // free the `node` we just setted
    config = tmp;
    PASS(NULL);
}

bool parse_option(const char *line, char *name, char *value);

TEST(test_parse_option) {
    char name[1024], value[1024];
    ASSERT(parse_option("", name, value) == false);
    ASSERT(parse_option("   \n", name, value) == false);
    ASSERT(parse_option("   # comment\n", name, value) == false);
    ASSERT(parse_option("    name value", name, value) == true);
    ASSERT(strcmp(name, "name") == 0);
    ASSERT(strcmp(value, "value") == 0);
    ASSERT(parse_option("    name2 value2\n", name, value) == true);
    ASSERT(strcmp(name, "name2") == 0);
    ASSERT(strcmp(value, "value2") == 0);
    PASS(NULL);
}

TEST_CASE(test_config) {
    RUN_TEST(test_config_bind);
    RUN_TEST(test_config_syslog);
    RUN_TEST(test_config_requirepass);
    RUN_TEST(test_config_read_strategy);
    RUN_TEST(test_config_node_to_str);
    RUN_TEST(test_config_change);
    RUN_TEST(test_parse_option);
}

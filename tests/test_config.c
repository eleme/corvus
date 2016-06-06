#include "test.h"
#include "alloc.h"

extern int config_add(char *name, char *value);

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

TEST_CASE(test_config) {
    RUN_TEST(test_config_bind);
    RUN_TEST(test_config_syslog);
    RUN_TEST(test_config_requirepass);
    RUN_TEST(test_config_read_strategy);
}

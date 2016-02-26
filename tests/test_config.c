#include "test.h"

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

TEST_CASE(test_config) {
    RUN_TEST(test_config_bind);
    RUN_TEST(test_config_syslog);
}

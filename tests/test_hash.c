#include "test.h"
#include "hash.h"
#include "command.h"

TEST(test_lookup3) {
    int i, counter = 0;

    init_command_map();
    for (i = 0; i < CMD_MAP_LEN; i++) {
        if (command_map[i].value != -1) counter++;
    }
    ASSERT(counter == 107);

    PASS(NULL);
}

TEST_CASE(test_hash) {
    RUN_TEST(test_lookup3);
}

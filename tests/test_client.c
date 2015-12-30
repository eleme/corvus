#include "test.h"
#include "client.h"
#include "corvus.h"

TEST(test_client_create) {
    ASSERT(client_create(ctx, -1) == NULL);
    PASS(NULL);
}

TEST_CASE(test_client) {
    RUN_TEST(test_client_create);
}

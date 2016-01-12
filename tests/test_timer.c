#include "test.h"
#include "timer.h"
#include "connection.h"

extern void check_connections(struct context *ctx);

TEST(test_check_connections) {
    struct connection *conn1 = conn_create(ctx);
    struct connection *conn2 = conn_create(ctx);

    config.client_timeout = config.server_timeout = 5;
    conn1->last_active = 1;
    conn2->last_active = 1;

    conn1->fd = socket_create_stream();
    conn2->fd = socket_create_stream();

    event_register(&ctx->loop, conn1);
    event_register(&ctx->loop, conn2);

    TAILQ_INSERT_TAIL(&ctx->conns, conn1, next);
    TAILQ_INSERT_TAIL(&ctx->conns, conn2, next);

    check_connections(ctx);

    ASSERT(TAILQ_LAST(&ctx->conns, conn_tqh) == conn2);
    ASSERT(TAILQ_LAST(&ctx->conns, conn_tqh)->fd == -1);
    ASSERT(TAILQ_FIRST(&ctx->conns) == conn1);
    ASSERT(TAILQ_FIRST(&ctx->conns)->fd  == -1);

    config.client_timeout = config.server_timeout = 0;
    PASS(NULL);
}

TEST_CASE(test_timer) {
    RUN_TEST(test_check_connections);
}

#include "test.h"
#include "timer.h"
#include "connection.h"

extern void check_connections(struct context *ctx);

TEST(test_check_connections) {
    struct connection *conn1 = conn_create(ctx);
    conn1->info = conn_info_create(ctx);
    struct connection *conn2 = conn_create(ctx);
    conn2->info = conn_info_create(ctx);

    config.client_timeout = config.server_timeout = 5;
    conn1->info->last_active = 1;
    conn2->info->last_active = 1;

    conn1->fd = socket_create_stream();
    conn2->fd = socket_create_stream();

    event_register(&ctx->loop, conn1, E_WRITABLE | E_READABLE);
    event_register(&ctx->loop, conn2, E_WRITABLE | E_READABLE);

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

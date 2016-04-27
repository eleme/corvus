#include "test.h"
#include "client.h"
#include "socket.h"
#include "corvus.h"

extern struct mbuf *client_get_buf(struct connection *client);
extern void client_range_clear(struct connection *client, struct command *cmd);

TEST(test_client_create) {
    ASSERT(client_create(ctx, -1) == NULL);
    PASS(NULL);
}

TEST(test_client_range_clear1) {
    int fd = socket_create_stream();
    struct connection *client = client_create(ctx, fd);

    struct mbuf *buf = client_get_buf(client);
    struct command *cmd = conn_get_cmd(client);
    buf->refcount = 1;
    cmd->req_buf[0].buf = buf;
    cmd->req_buf[1].buf = buf;

    buf->last += 1;

    client_range_clear(client, cmd);

    ASSERT(buf->refcount == 0);
    ASSERT(cmd->req_buf[0].buf == NULL);
    ASSERT(client->info->current_buf == buf);
    ASSERT(TAILQ_EMPTY(&ctx->free_mbufq));

    conn_free(client);
    conn_buf_free(client);
    conn_recycle(ctx, client);

    PASS(NULL);
}

TEST(test_client_range_clear2) {
    int fd = socket_create_stream();
    struct connection *client = client_create(ctx, fd);

    struct mbuf *buf = client_get_buf(client);
    struct command *cmd = conn_get_cmd(client);
    buf->refcount = 1;
    cmd->req_buf[0].buf = buf;
    cmd->req_buf[1].buf = buf;

    buf->last += 1;
    buf->pos = buf->last;

    client_range_clear(client, cmd);

    ASSERT(buf->refcount == 0);
    ASSERT(cmd->req_buf[0].buf == NULL);
    ASSERT(client->info->current_buf == NULL);
    ASSERT(TAILQ_FIRST(&ctx->free_mbufq) == buf);

    conn_free(client);
    conn_buf_free(client);
    conn_recycle(ctx, client);

    PASS(NULL);
}

TEST(test_client_range_clear3) {
    int fd = socket_create_stream();
    struct connection *client = client_create(ctx, fd);

    struct mbuf *buf = client_get_buf(client);
    struct command *cmd = conn_get_cmd(client);
    buf->refcount = 2;
    cmd->req_buf[0].buf = buf;
    cmd->req_buf[1].buf = buf;

    buf->last += 1;
    buf->pos = buf->last;

    client_range_clear(client, cmd);

    ASSERT(buf->refcount == 1);
    ASSERT(cmd->req_buf[0].buf == NULL);
    ASSERT(client->info->current_buf == buf);
    ASSERT(TAILQ_EMPTY(&ctx->free_mbufq));

    conn_free(client);
    conn_buf_free(client);
    conn_recycle(ctx, client);

    PASS(NULL);
}

TEST_CASE(test_client) {
    RUN_TEST(test_client_create);
    RUN_TEST(test_client_range_clear1);
    RUN_TEST(test_client_range_clear2);
    RUN_TEST(test_client_range_clear3);
}

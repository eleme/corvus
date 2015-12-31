#include "test.h"
#include "connection.h"
#include "server.h"
#include "corvus.h"

/* after server_eof:
 *      - stale cmds should be freed
 *      - all non-stale cmds should be marked fail
 *      - cmd reply should be freed
 */
TEST(test_server_eof) {
    int fd = conn_create_fd();
    struct connection *server = server_create(ctx, fd);
    conn_register(server);

    struct command *cmd1 = cmd_create(ctx);
    struct command *cmd2 = cmd_create(ctx);

    struct mbuf *buf = mbuf_get(ctx);
    char *a = "test";

    STAILQ_INSERT_TAIL(&server->data, buf, next);

    cmd1->server = cmd2->server = server;
    cmd1->cmd_count = cmd2->cmd_count = 10;
    cmd2->stale = 1;
    cmd2->rep_buf[0].buf = cmd1->rep_buf[0].buf = buf;
    cmd2->rep_buf[0].pos = cmd1->rep_buf[0].pos = (uint8_t*)a;

    STAILQ_INSERT_TAIL(&server->ready_queue, cmd1, ready_next);
    STAILQ_INSERT_TAIL(&server->waiting_queue, cmd2, waiting_next);

    server_eof(server, "test eof");

    /* cmd1 failed */
    ASSERT(cmd1->cmd_fail == 1);
    ASSERT(cmd1->rep_buf[0].buf == NULL && cmd1->rep_buf[0].pos == NULL);
    ASSERT(strcmp(cmd1->fail_reason, "test eof") == 0);

    /* cmd2(stale) freed */
    ASSERT(cmd2->server == NULL);
    ASSERT(STAILQ_FIRST(&ctx->free_cmdq) == cmd2);

    /* server freed */
    ASSERT(server->fd == -1);
    ASSERT(STAILQ_EMPTY(&server->ready_queue));
    ASSERT(STAILQ_EMPTY(&server->waiting_queue));
    ASSERT(STAILQ_EMPTY(&server->data));

    cmd_free(cmd1);
    conn_free(server);
    conn_recycle(ctx, server);

    PASS(NULL);
}

TEST_CASE(test_server) {
    RUN_TEST(test_server_eof);
}

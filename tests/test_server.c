#include "test.h"
#include "connection.h"
#include "server.h"
#include "corvus.h"

extern void server_data_clear(struct command *cmd);

/* after server_eof:
 *      - stale cmds should be freed
 *      - all non-stale cmds should be marked fail
 *      - cmd reply should be freed
 */
TEST(test_server_eof) {
    int fd = conn_create_fd();
    struct connection *server = server_create(ctx, fd);
    conn_register(server);

    struct command *cmd = cmd_create(ctx);
    struct command *cmd1 = cmd_create(ctx);
    struct command *cmd2 = cmd_create(ctx);

    char *a = "test";

    cmd1->parent = cmd;
    cmd1->server = cmd2->server = server;
    cmd1->cmd_count = cmd2->cmd_count = 10;

    conn_add_data(server, (uint8_t*)a, strlen(a), &cmd2->rep_buf[0], &cmd2->rep_buf[1]);
    struct mbuf *b = TAILQ_FIRST(&server->info->local_data);
    b->pos = b->last;

    cmd2->stale = 1;

    STAILQ_INSERT_TAIL(&server->info->ready_queue, cmd1, ready_next);
    STAILQ_INSERT_TAIL(&server->info->waiting_queue, cmd2, waiting_next);

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
    ASSERT(STAILQ_EMPTY(&server->info->ready_queue));
    ASSERT(STAILQ_EMPTY(&server->info->waiting_queue));
    ASSERT(TAILQ_EMPTY(&server->info->local_data));

    cmd_free(cmd1);
    cmd_free(cmd);

    conn_free(server);
    conn_recycle(ctx, server);

    PASS(NULL);
}

TEST(test_server_data_clear) {
    char *data = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$3\r\n999\r\n";

    struct connection *conn = server_create(ctx, -1);

    struct command *cmd = cmd_create(ctx);

    struct mbuf *buf = conn_get_buf(conn, true);
    int size = strlen(data);
    memcpy(buf->last, data, size);
    buf->last += size;

    reader_feed(&conn->info->reader, buf);
    ASSERT(parse(&conn->info->reader, MODE_REP) == 0);

    memcpy(&cmd->rep_buf[0], &conn->info->reader.start, sizeof(struct buf_ptr));
    memcpy(&cmd->rep_buf[1], &conn->info->reader.end, sizeof(struct buf_ptr));

    ASSERT(buf->refcount == 1);

    mbuf_range_clear(ctx, cmd->rep_buf);

    ASSERT(cmd->rep_buf[0].buf == NULL);
    ASSERT(cmd->rep_buf[1].buf == NULL);
    ASSERT(TAILQ_FIRST(&ctx->free_mbufq) == buf);

    cmd_free(cmd);

    conn_free(conn);
    conn_recycle(ctx, conn);

    PASS(NULL);
}

TEST_CASE(test_server) {
    RUN_TEST(test_server_eof);
    RUN_TEST(test_server_data_clear);
}

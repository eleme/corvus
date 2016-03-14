#include "test.h"
#include "parser.h"
#include "corvus.h"
#include "command.h"
#include "logging.h"

extern int cmd_apply_range(struct command *cmd, int type);
extern int cmd_parse_rep(struct command *cmd, struct mbuf *buf);
extern void cmd_gen_mget_iovec(struct command *cmd, struct iov_data *iov);

TEST(test_parse_redirect) {
    char data1[] = "-MOV";
    char data2[] = "ED 11866 127.112.112.111:18001";

    struct connection *client = conn_create(ctx);
    client->info = conn_info_create(ctx);

    struct command *cmd = cmd_create(ctx);

    conn_add_data(client, (uint8_t*)data1, strlen(data1),
            &cmd->rep_buf[0], NULL);

    struct mbuf *buf = TAILQ_FIRST(&client->info->local_data);
    buf->end = buf->last;
    buf->pos = buf->last;

    conn_add_data(client, (uint8_t*)data2, strlen(data2),
            NULL, &cmd->rep_buf[1]);

    buf = TAILQ_NEXT(buf, next);
    buf->pos = buf->last;

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    ASSERT(cmd_parse_redirect(cmd, &info) == CORVUS_OK);
    ASSERT(strncmp(info.addr, "127.112.112.111:18001", 21) == 0);
    ASSERT(info.slot == 11866);
    ASSERT(info.type == CMD_ERR_MOVED);

    cmd_free(cmd);
    mbuf_range_clear(ctx, cmd->rep_buf);
    ASSERT(TAILQ_EMPTY(&client->info->local_data));

    conn_free(client);
    conn_recycle(ctx, client);

    PASS(NULL);
}

TEST(test_parse_redirect_wrong_error) {
    char data[] = "-Err Server Errorrrrrrrrrrrrrrrr";

    struct connection *client = conn_create(ctx);
    client->info = conn_info_create(ctx);

    struct command *cmd = cmd_create(ctx);

    conn_add_data(client, (uint8_t*)data, strlen(data), &cmd->rep_buf[0], &cmd->rep_buf[1]);
    struct mbuf *b = TAILQ_FIRST(&client->info->local_data);
    b->pos = b->last;

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    cmd_parse_redirect(cmd, &info);
    ASSERT(info.type == CMD_ERR);

    cmd_free(cmd);
    mbuf_range_clear(ctx, cmd->rep_buf);
    ASSERT(TAILQ_EMPTY(&client->info->local_data));

    conn_free(client);
    conn_recycle(ctx, client);

    PASS(NULL);
}

TEST(test_parse_redirect_wrong_moved) {
    char data[] = "-MOVED wrong_slot wrong_address";

    struct connection *client = conn_create(ctx);
    client->info = conn_info_create(ctx);

    struct command *cmd = cmd_create(ctx);

    conn_add_data(client, (uint8_t*)data, strlen(data), &cmd->rep_buf[0], &cmd->rep_buf[1]);
    struct mbuf *b = TAILQ_FIRST(&client->info->local_data);
    b->pos = b->last;

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    ASSERT(cmd_parse_redirect(cmd, &info) == CORVUS_ERR);

    cmd_free(cmd);
    mbuf_range_clear(ctx, cmd->rep_buf);
    ASSERT(TAILQ_EMPTY(&client->info->local_data));

    conn_free(client);
    conn_recycle(ctx, client);

    PASS(NULL);
}

TEST(test_cmd_iov_add) {
    struct iov_data iov;
    memset(&iov, 0, sizeof(iov));

    cmd_iov_add(&iov, "hello world", 11, NULL);
    ASSERT(iov.len == 1);
    ASSERT(iov.max_size == CORVUS_IOV_MAX);
    ASSERT(iov.cursor == 0);

    for (int i = 0; i < CORVUS_IOV_MAX; i++) {
        cmd_iov_add(&iov, "world", 5, NULL);
    }
    ASSERT(iov.len == CORVUS_IOV_MAX + 1);
    ASSERT(iov.max_size == CORVUS_IOV_MAX << 1);

    iov.cursor = CORVUS_IOV_MAX;

    cmd_iov_add(&iov, "hello", 5, NULL);

    ASSERT(iov.len == 2);
    ASSERT(iov.cursor == 0);

    ASSERT(strncmp(iov.data[0].iov_base, "world", iov.data[0].iov_len) == 0);
    ASSERT(strncmp(iov.data[1].iov_base, "hello", iov.data[1].iov_len) == 0);

    cmd_iov_free(&iov);
    PASS(NULL);
}

TEST(test_cmd_gen_mget_iovec) {
    char *data = "$3\r\n123\r\n$5\r\nhello\r\n$3\r\n999\r\n";
    int size = strlen(data);

    struct command *c  = cmd_create(ctx),
                   *c1 = cmd_create(ctx),
                   *c2 = cmd_create(ctx),
                   *c3 = cmd_create(ctx);

    struct connection *conn = conn_create(ctx);
    conn->info = conn_info_create(ctx);

    struct mbuf *buf = conn_get_buf(conn);
    memcpy(buf->last, data, size);
    buf->last += size;

    STAILQ_INSERT_TAIL(&c->sub_cmds, c1, sub_cmd_next);
    STAILQ_INSERT_TAIL(&c->sub_cmds, c2, sub_cmd_next);
    STAILQ_INSERT_TAIL(&c->sub_cmds, c3, sub_cmd_next);

    c->keys = 3;
    c1->server = c2->server = c3->server = conn;

    ASSERT(cmd_parse_rep(c1, buf) == 0);
    ASSERT(cmd_parse_rep(c2, buf) == 0);
    ASSERT(cmd_parse_rep(c3, buf) == 0);

    struct iov_data iov;
    memset(&iov, 0, sizeof(iov));

    cmd_gen_mget_iovec(c, &iov);

    ASSERT(iov.len == 4);
    ASSERT(strncmp(iov.data[0].iov_base, "*3\r\n", iov.data[0].iov_len) == 0);
    ASSERT(strncmp(iov.data[1].iov_base, "$3\r\n123\r\n", iov.data[0].iov_len) == 0);
    ASSERT(strncmp(iov.data[2].iov_base, "$5\r\nhello\r\n", iov.data[0].iov_len) == 0);
    ASSERT(strncmp(iov.data[3].iov_base, "$3\r\n999\r\n", iov.data[0].iov_len) == 0);
    cmd_iov_clear(ctx, &iov);

    ASSERT(TAILQ_EMPTY(&conn->info->data));

    cmd_iov_free(&iov);
    cmd_free(c);

    conn_free(conn);
    conn_recycle(ctx, conn);

    PASS(NULL);
}

TEST(test_cmd_gen_mget_iovec_fail) {
    char *data = "$3\r\n123\r\n$5\r\nhello\r\n$3\r\n999\r\n";
    int size = strlen(data);

    struct command *c  = cmd_create(ctx),
                   *c1 = cmd_create(ctx),
                   *c2 = cmd_create(ctx),
                   *c3 = cmd_create(ctx);

    struct connection *conn = conn_create(ctx);
    conn->info = conn_info_create(ctx);

    struct mbuf *buf = conn_get_buf(conn);
    memcpy(buf->last, data, size);
    buf->last += size;

    STAILQ_INSERT_TAIL(&c->sub_cmds, c1, sub_cmd_next);
    STAILQ_INSERT_TAIL(&c->sub_cmds, c2, sub_cmd_next);
    STAILQ_INSERT_TAIL(&c->sub_cmds, c3, sub_cmd_next);

    c1->server = c2->server = c3->server = conn;
    c2->parent = c;

    ASSERT(cmd_parse_rep(c1, buf) == 0);
    ASSERT(cmd_parse_rep(c2, buf) == 0);
    ASSERT(cmd_parse_rep(c3, buf) == 0);

    ASSERT(buf->refcount == 3);

    mbuf_range_clear(ctx, c2->rep_buf);
    ASSERT(buf->refcount == 2);
    cmd_mark_fail(c2, "-Err failed\r\n");

    struct iov_data iov;
    memset(&iov, 0, sizeof(iov));

    cmd_gen_mget_iovec(c, &iov);
    ASSERT(buf->refcount == 0);

    ASSERT(iov.len == 1);
    ASSERT(strncmp(iov.data[0].iov_base, "-Err failed\r\n", iov.data[0].iov_len) == 0);
    cmd_iov_clear(ctx, &iov);

    ASSERT(TAILQ_EMPTY(&conn->info->data));

    cmd_iov_free(&iov);
    cmd_free(c);

    conn_free(conn);
    conn_recycle(ctx, conn);

    PASS(NULL);
}

TEST_CASE(test_cmd) {
    RUN_TEST(test_parse_redirect);
    RUN_TEST(test_parse_redirect_wrong_error);
    RUN_TEST(test_parse_redirect_wrong_moved);
    RUN_TEST(test_cmd_iov_add);
    RUN_TEST(test_cmd_gen_mget_iovec);
    RUN_TEST(test_cmd_gen_mget_iovec_fail);
}

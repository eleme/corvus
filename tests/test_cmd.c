#include "test.h"
#include "parser.h"
#include "corvus.h"
#include "command.h"
#include "logging.h"

extern int cmd_apply_range(struct command *cmd, int type);

TEST(test_parse_redirect) {
    char data1[] = "-MOV";
    char data2[] = "ED 866 127.0.0.1:8001";

    struct command *cmd = cmd_create(ctx);
    mbuf_queue_copy(ctx, &cmd->rep_queue, (uint8_t*)data1, strlen(data1));
    struct mbuf *buf = STAILQ_FIRST(&cmd->rep_queue);
    buf->end = buf->last;
    mbuf_queue_copy(ctx, &cmd->rep_queue, (uint8_t*)data2, strlen(data2));
    cmd_apply_range(cmd, MODE_REP);

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    ASSERT(cmd_parse_redirect(cmd, &info) == CORVUS_OK);
    ASSERT(strncmp(info.addr, "127.0.0.1:8001", 14) == 0);
    ASSERT(info.slot == 866);
    ASSERT(info.type == CMD_ERR_MOVED);

    cmd_free(cmd);
    PASS(NULL);
}

TEST(test_parse_redirect_wrong_error) {
    char data[] = "-Err Server Errorrrrrrrrrrrrrrrr";

    struct command *cmd = cmd_create(ctx);
    mbuf_queue_copy(ctx, &cmd->rep_queue, (uint8_t*)data, strlen(data));
    cmd_apply_range(cmd, MODE_REP);

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    cmd_parse_redirect(cmd, &info);
    ASSERT(info.type == CMD_ERR);

    cmd_free(cmd);
    PASS(NULL);
}

TEST(test_parse_redirect_wrong_moved) {
    char data[] = "-MOVED wrong_slot wrong_address";

    struct command *cmd = cmd_create(ctx);
    mbuf_queue_copy(ctx, &cmd->rep_queue, (uint8_t*)data, strlen(data));
    cmd_apply_range(cmd, MODE_REP);

    struct redirect_info info;
    info.type = CMD_ERR;
    info.slot = -1;

    ASSERT(cmd_parse_redirect(cmd, &info) == CORVUS_ERR);

    cmd_free(cmd);
    PASS(NULL);
}

TEST(test_cmd_iov_add) {
    struct iov_data iov;
    memset(&iov, 0, sizeof(iov));

    cmd_iov_add(&iov, "hello world", 11);
    ASSERT(iov.len == 1);
    ASSERT(iov.max_size == CORVUS_IOV_MAX);
    ASSERT(iov.cursor == 0);

    for (int i = 0; i < CORVUS_IOV_MAX; i++) {
        cmd_iov_add(&iov, "world", 5);
    }
    ASSERT(iov.len == CORVUS_IOV_MAX + 1);
    ASSERT(iov.max_size == CORVUS_IOV_MAX << 1);

    iov.cursor = CORVUS_IOV_MAX;

    cmd_iov_add(&iov, "hello", 5);

    ASSERT(iov.len == 2);
    ASSERT(iov.cursor == 0);

    ASSERT(strncmp(iov.data[0].iov_base, "world", iov.data[0].iov_len) == 0);
    ASSERT(strncmp(iov.data[1].iov_base, "hello", iov.data[1].iov_len) == 0);

    cmd_iov_free(&iov);
    PASS(NULL);
}

TEST(test_cmd_free_reply) {
    struct command *cmd = cmd_create(ctx);
    struct connection *server = conn_create(ctx);
    cmd->server = server;

    struct mbuf *buf1 = mbuf_get(ctx),
                *buf2 = mbuf_get(ctx),
                *buf3 = mbuf_get(ctx);
    STAILQ_INSERT_TAIL(&server->data, buf1, next);
    STAILQ_INSERT_TAIL(&server->data, buf2, next);
    STAILQ_INSERT_TAIL(&server->data, buf3, next);

    cmd->rep_buf[0].buf = buf1;
    cmd->rep_buf[1].buf = buf1;
    mbuf_inc_ref(buf1);
    mbuf_inc_ref(buf1);

    cmd_free_reply(cmd);

    ASSERT(STAILQ_FIRST(&server->data) == buf2);
    ASSERT(cmd->rep_buf[0].buf == NULL);
    ASSERT(cmd->rep_buf[1].buf == NULL);

    cmd->rep_buf[0].buf = buf2;
    mbuf_inc_ref(buf2);

    cmd_free_reply(cmd);
    ASSERT(STAILQ_EMPTY(&server->data));
    ASSERT(cmd->rep_buf[0].buf == NULL);

    cmd_free(cmd);
    conn_free(server);
    conn_recycle(ctx, server);
    PASS(NULL);
}

TEST_CASE(test_cmd) {
    RUN_TEST(test_parse_redirect);
    RUN_TEST(test_parse_redirect_wrong_error);
    RUN_TEST(test_parse_redirect_wrong_moved);
    RUN_TEST(test_cmd_iov_add);
    RUN_TEST(test_cmd_free_reply);
}

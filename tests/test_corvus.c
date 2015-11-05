#include "test.h"
#include "corvus.h"
#include "mbuf.h"
#include "proxy.h"
#include "event.h"
#include "logging.h"
#include "slot.h"

void context_init(struct context *ctx, bool syslog, int log_level)
{
    ctx->syslog = syslog;
    ctx->log_level = log_level;
    mbuf_init(ctx);
    log_init(ctx);
}

TEST(test_event_flow) {
    char *nodes[] = {"localhost:8000", "localhost:8001"};
    initial_nodes.nodes = nodes;
    initial_nodes.len = 2;

    struct context ctx;
    context_init(&ctx, false, DEBUG);
    ctx.server_table = hash_new();
    init_command_map();
    slot_init_updater(false, DEBUG);

    slot_create_job(SLOT_UPDATE_INIT, &initial_nodes);

    struct event_loop *loop = event_create(1024);
    ctx.loop = loop;
    struct connection *proxy = proxy_create(&ctx, "localhost", 12345);
    if (proxy == NULL) {
        LOG(ERROR, "can not create proxy");
        FAIL(NULL);
    }

    event_register(loop, proxy);

    while (1) {
        event_wait(loop, -1);
    }
}

TEST_CASE(test_corvus) {
    RUN_TEST(test_event_flow);
}

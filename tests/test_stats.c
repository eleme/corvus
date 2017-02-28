#include "test.h"
#include "stats.h"

extern void stats_get_simple(struct stats *stats, bool reset);

void set_stats(struct context *ctx)
{
    ctx->stats.completed_commands = 10;
    ctx->stats.remote_latency = 1000;
    ctx->stats.total_latency = 10000;
    ctx->stats.recv_bytes = 16;
    ctx->stats.send_bytes = 32;
    ctx->stats.connected_clients = 5;
    ctx->stats.ask_recv = 233;
    ctx->stats.moved_recv = 600;
}

TEST(test_stats_get_simple_reset) {
    struct context *ctxs = get_contexts();
    set_stats(&ctxs[0]);

    struct stats stats;
    memset(&stats, 0, sizeof(stats));
    stats_get_simple(&stats, true);
    incr_slot_update_counter();

    ASSERT(stats.basic.completed_commands == 10);
    ASSERT(stats.basic.slot_update_jobs == 1);
    ASSERT(stats.basic.remote_latency == 1000);
    ASSERT(stats.basic.total_latency == 10000);
    ASSERT(stats.basic.recv_bytes == 16);
    ASSERT(stats.basic.send_bytes == 32);
    ASSERT(stats.basic.connected_clients == 5);
    ASSERT(stats.basic.ask_recv == 233);
    ASSERT(stats.basic.moved_recv == 600);

    ASSERT(ctxs[0].stats.completed_commands == 0);
    ASSERT(ctxs[0].stats.remote_latency == 0);
    ASSERT(ctxs[0].stats.total_latency == 0);
    ASSERT(ctxs[0].stats.recv_bytes == 0);
    ASSERT(ctxs[0].stats.send_bytes == 0);
    ASSERT(ctxs[0].stats.ask_recv == 0);
    ASSERT(ctxs[0].stats.moved_recv == 0);
    PASS(NULL);
}

TEST(test_stats_get_simple_cumulative) {
    struct context *ctxs = get_contexts();
    struct stats stats;

    memset(&stats, 0, sizeof(stats));
    set_stats(&ctxs[0]);
    stats_get_simple(&stats, false);
    incr_slot_update_counter();

    ASSERT(stats.basic.completed_commands == 20);
    ASSERT(stats.basic.slot_update_jobs == 2);
    ASSERT(stats.basic.remote_latency == 2000);
    ASSERT(stats.basic.total_latency == 20000);
    ASSERT(stats.basic.recv_bytes == 32);
    ASSERT(stats.basic.send_bytes == 64);
    ASSERT(stats.basic.connected_clients == 5);
    ASSERT(stats.basic.ask_recv == 466);
    ASSERT(stats.basic.moved_recv == 1200);

    ASSERT(ctxs[0].stats.completed_commands == 10);
    ASSERT(ctxs[0].stats.remote_latency == 1000);
    ASSERT(ctxs[0].stats.total_latency == 10000);
    ASSERT(ctxs[0].stats.recv_bytes == 16);
    ASSERT(ctxs[0].stats.send_bytes == 32);
    ASSERT(ctxs[0].stats.connected_clients == 5);
    ASSERT(ctxs[0].stats.ask_recv == 233);
    ASSERT(ctxs[0].stats.moved_recv == 600);
    PASS(NULL);
}

TEST_CASE(test_stats) {
    RUN_TEST(test_stats_get_simple_reset);
    RUN_TEST(test_stats_get_simple_cumulative);
}

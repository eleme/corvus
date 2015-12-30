#include <pthread.h>
#include <unistd.h>
#include "test.h"
#include "corvus.h"
#include "slot.h"

static void usage(const char *name)
{
    printf("Usage: %s [-h] [-s TEST_CASE] [-t TEST_FUNC]\n"
            "  -h            print this Help\n"
            "  -s TEST_CASE  only run suite named SUITE\n"
            "  -t TEST_FUNC  only run test named TEST\n"
            "  --silent      do not print result\n", name);
}

static int setup_cli(int argc, const char *argv[])
{
    int i = 0;
    memset(manager.test_func_filter, 0, sizeof(manager.test_func_filter));
    memset(manager.case_filter, 0, sizeof(manager.case_filter));
    manager.silent = 0;

    for (i = 1; i < argc; i++) {
        if (0 == strcmp("-t", argv[i])) {
            if (argc <= i + 1) return -1;
            strncpy(manager.test_func_filter, argv[i + 1], 1023);
            i++;
        } else if (0 == strcmp("-s", argv[i])) {
            if (argc <= i + 1) return -1;
            strncpy(manager.case_filter, argv[i + 1], 1023);
            i++;
        } else if (0 == strcmp("-h", argv[i])) {
            return -1;
        } else if (0 == strcmp("--silent", argv[i])) {
            manager.silent = 1;
        } else {
            fprintf(stdout,
                "Unknown argument '%s'\n", argv[i]);
            return -1;
        }
    }
    return 0;
}

static void report()
{
    print(1, "\nTotal: %u tests, %u assertions\n", manager.tests_run,
            manager.assertions);
    print(1, "Pass: %u, fail: %u, skip: %u\n", manager.passed, manager.failed,
            manager.skipped);
}

extern TEST_CASE(test_slot);
extern TEST_CASE(test_hash);
extern TEST_CASE(test_parser);
extern TEST_CASE(test_cmd);
extern TEST_CASE(test_server);
extern TEST_CASE(test_dict);
extern TEST_CASE(test_socket);
extern TEST_CASE(test_client);

int main(int argc, const char *argv[])
{
    if(setup_cli(argc, argv) == -1) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    struct node_conf conf = {NULL, 0};
    struct context ctx;
    context_init(&ctx, 0, ERROR);
    ctx.node_conf = &conf;
    slot_init_updater(&ctx);

    RUN_CASE(test_slot);
    RUN_CASE(test_hash);
    RUN_CASE(test_parser);
    RUN_CASE(test_cmd);
    RUN_CASE(test_server);
    RUN_CASE(test_dict);
    RUN_CASE(test_socket);
    RUN_CASE(test_client);

    usleep(10000);
    slot_create_job(SLOT_UPDATER_QUIT);
    pthread_join(ctx.thread, NULL);

    report();
    return manager.failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

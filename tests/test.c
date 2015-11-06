#include "test.h"

static void usage(const char *name)
{
    printf("Usage: %s [-h] [-s TEST_CASE] [-t TEST_FUNC]\n"
            "  -h            print this Help\n"
            "  -s TEST_CASE  only run suite named SUITE\n"
            "  -t TEST_FUNC  only run test named TEST\n", name);
}

static int setup_cli(int argc, const char *argv[])
{
    int i = 0;
    manager.test_func_filter[0] = manager.case_filter[0] = 0;

    for (i = 1; i < argc; i++) {
        if (0 == strcmp("-t", argv[i])) {
            if (argc <= i + 1) return -1;
            strcpy(manager.test_func_filter, argv[i + 1]);
            i++;
        } else if (0 == strcmp("-s", argv[i])) {
            if (argc <= i + 1) return -1;
            strcpy(manager.case_filter, argv[i + 1]);
            i++;
        } else if (0 == strcmp("-h", argv[i])) {
            return -1;
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
    printf("\nTotal: %u tests, %u assertions\n", manager.tests_run,
            manager.assertions);
    printf("Pass: %u, fail: %u, skip: %u\n", manager.passed, manager.failed,
            manager.skipped);
}

extern TEST_CASE(test_slot);
extern TEST_CASE(test_hash);
extern TEST_CASE(test_parser);
extern TEST_CASE(test_cmd);

int main(int argc, const char *argv[])
{
    if(setup_cli(argc, argv) == -1) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    RUN_CASE(test_slot);
    RUN_CASE(test_hash);
    RUN_CASE(test_parser);
    RUN_CASE(test_cmd);

    report();
    return manager.failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

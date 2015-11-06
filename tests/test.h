#ifndef __TEST_H
#define __TEST_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define MSG_LEN 1024

#define TEST_CASE(NAME) void NAME(void)
#define TEST(name) static enum test_result name(void)
#define RUN_CASE(test_case)                                             \
    do {                                                                \
        if (!filter(#test_case, manager.case_filter)) {                 \
            break;                                                      \
        }                                                               \
        memset(&manager.current_case, 0, sizeof(struct case_info));     \
                                                                        \
        PRINT("\n\033[1m%s\033[m: ", #test_case);                       \
        test_case();                                                    \
        post_case();                                                    \
        manager.passed += manager.current_case.passed;                  \
        manager.failed += manager.current_case.failed;                  \
        manager.skipped += manager.current_case.skipped;                \
        manager.tests_run += manager.current_case.tests_run;            \
    } while (0)

#define RUN_TEST(test_func)                                             \
    do {                                                                \
        if (filter(#test_func, manager.test_func_filter)) {             \
            enum test_result res = test_func();                         \
            post_test(#test_func, res);                                 \
        }                                                               \
    } while (0)

#define ASSERT_MSG(cond, msg)                                           \
    do {                                                                \
        manager.assertions++;                                           \
        if (!(cond)) FAIL(msg);                                         \
    } while (0)

#define ASSERT(cond) ASSERT_MSG(cond, #cond)

#define PASS(_msg)                                                      \
    do {                                                                \
        manager.msg = _msg;                                             \
        return TEST_PASS;                                               \
    } while (0)

#define SKIP(_msg)                                                      \
    do {                                                                \
        manager.msg = _msg;                                             \
        return TEST_SKIP;                                               \
    } while (0)

#define FAIL(_msg)                                                      \
    do {                                                                \
        manager.fail_file = __FILE__;                                   \
        manager.fail_line = __LINE__;                                   \
        manager.msg = _msg;                                             \
        return TEST_FAIL;                                               \
    } while (0)

#define PRINT(...)                                                      \
    do {                                                                \
        if (manager.test_func_filter[0] == 0) {                         \
            printf(__VA_ARGS__);                                        \
        }                                                               \
    } while (0)

/* Info for the current running suite. */
struct case_info {
    unsigned int tests_run;
    unsigned int passed;
    unsigned int failed;
    unsigned int skipped;
    int msg_idx;
    char msgs[128][MSG_LEN];
};

/* Struct containing all test runner state. */
struct {
    unsigned int tests_run;     /* total test count */

    /* overall pass/fail/skip counts */
    unsigned int passed;
    unsigned int failed;
    unsigned int skipped;
    unsigned int assertions;

    struct case_info current_case;

    /* info to print about the most recent failure */
    const char *fail_file;
    unsigned int fail_line;
    const char *msg;

    /* only run a specific suite or test */
    char case_filter[1024];
    char test_func_filter[1024];
} manager;

enum test_result {
    TEST_PASS = 0,
    TEST_FAIL = -1,
    TEST_SKIP = 1
};

static int filter(const char *name, const char *filter)
{
    size_t offset = 0;
    size_t filter_len = strlen(filter);
    if (filter_len <= 0) return 1;

    while (name[offset] != '\0') {
        if (name[offset] == filter[0]) {
            if (0 == strncmp(&name[offset], filter, filter_len)) {
                return 1;
            }
        }
        offset++;
    }
    return 0;
}

static void pass(const char *name)
{
    PRINT(".");
    struct case_info *c = &manager.current_case;
    snprintf(c->msgs[c->msg_idx++], MSG_LEN, "\033[32mPASS\033[m %s", name);
    manager.current_case.passed++;
}

static void fail(const char *name)
{
    PRINT("F");
    struct case_info *c = &manager.current_case;
    snprintf(c->msgs[c->msg_idx++], MSG_LEN, "\033[31mFAIL\033[m %s: %s (%s:%u)", name,
            manager.msg ? manager.msg : "",
            manager.fail_file, manager.fail_line);

    manager.current_case.failed++;
}

static void skip(const char *name)
{
    PRINT("s");
    struct case_info *c = &manager.current_case;
    snprintf(c->msgs[c->msg_idx++], MSG_LEN, "SKIP %s", name);
    manager.current_case.skipped++;
}

static void post_test(const char *name, int res)
{
    if (res <= TEST_FAIL) {
        fail(name);
    } else if (res >= TEST_SKIP) {
        skip(name);
    } else if (res == TEST_PASS) {
        pass(name);
    }
    manager.current_case.tests_run++;
}

static void post_case()
{
    int i;
    struct case_info *c = &manager.current_case;
    if (c->msg_idx <= 0) return;

    PRINT("\n");
    for (i = 0; i < c->msg_idx; i++) {
        printf("%s\n", c->msgs[i]);
    }

    PRINT("%u tests - %u pass, %u fail, %u skipped\n", c->tests_run,
          c->passed, c->failed, c->skipped);
}

#endif

#include "test.h"
#include "dict.h"

extern void dict_resize(struct dict *dict);

TEST(test_dict_get) {
    struct dict dict;
    dict_init(&dict);

    dict_set(&dict, "hello", "safdasfd");
    dict_set(&dict, "world", "dsafsadf");
    dict_set(&dict, "", "empty");

    char *hello = dict_get(&dict, "hello");
    char *world = dict_get(&dict, "world");
    char *missing = dict_get(&dict, "no");
    char *empty = dict_get(&dict, "");

    ASSERT(strcmp(hello, "safdasfd") == 0);
    ASSERT(strcmp(world, "dsafsadf") == 0);
    ASSERT(strcmp(empty, "empty") == 0);
    ASSERT(missing == NULL);
    ASSERT(dict.capacity == 1024);
    ASSERT(dict.length == 3);

    dict_free(&dict);
    PASS(NULL);
}

TEST(test_dict_resize) {
    struct dict dict;
    dict_init(&dict);

    ASSERT(dict.capacity == 1024);

    dict_set(&dict, "a", "1");
    dict_set(&dict, "aa", "2");

    dict_resize(&dict);
    ASSERT(dict.capacity == 2048);
    ASSERT(dict.length == 2);
    ASSERT(strcmp((char*)dict_get(&dict, "a"), "1") == 0);
    ASSERT(strcmp((char*)dict_get(&dict, "aa"), "2") == 0);

    dict_free(&dict);
    PASS(NULL);
}

TEST(test_dict_delete) {
    struct dict dict;
    dict_init(&dict);

    ASSERT(dict_index(&dict, "hello world") == -1);

    dict_set(&dict, "bbbbb", "1234");
    int idx = dict_index(&dict, "bbbbb");
    ASSERT(idx != -1);
    ASSERT(!dict.buckets[idx].deleted);
    ASSERT(dict.buckets[idx].setted);

    dict_delete(&dict, "bbbbb");
    ASSERT(dict.buckets[idx].deleted);
    ASSERT(dict.buckets[idx].setted);

    char *a = dict_get(&dict, "bbbbb");
    ASSERT(a == NULL);

    dict_free(&dict);
    PASS(NULL);
}

TEST(test_dict_clear) {
    struct dict dict;
    dict_init(&dict);

    dict_set(&dict, "cccc", "23525");
    dict_set(&dict, "ccccc", "2352345");

    int idx1 = dict_index(&dict, "cccc");
    int idx2 = dict_index(&dict, "ccccc");

    ASSERT(idx1 != -1 && idx2 != -1);
    ASSERT(idx1 != idx2);
    ASSERT(dict.buckets[idx1].setted);
    ASSERT(dict.buckets[idx2].setted);
    ASSERT(dict.length == 2);

    dict_clear(&dict);

    ASSERT(!dict.buckets[idx1].setted);
    ASSERT(!dict.buckets[idx2].setted);
    ASSERT(dict.length == 0);
    ASSERT(dict.capacity == 1024);
    ASSERT(dict.buckets != NULL);

    dict_free(&dict);
    PASS(NULL);
}

TEST(test_dict_each) {
    struct dict dict;
    dict_init(&dict);

    dict_set(&dict, "io", "asdf");
    dict_set(&dict, "oi", "sdfaf");

    int idx1 = dict_index(&dict, "io");
    int idx2 = dict_index(&dict, "oi");

    dict_delete(&dict, "io");

    struct dict_iter iter = DICT_ITER_INITIALIZER;
    DICT_FOREACH(&dict, &iter) {
        if ((int)iter.idx == idx1) FAIL(NULL);
        if ((int)iter.idx == idx2) {
            ASSERT(strcmp(iter.key, "oi") == 0);
            ASSERT(strcmp((char*)iter.value, "sdfaf") == 0);
        }
    }

    dict_free(&dict);
    PASS(NULL);
}

TEST_CASE(test_dict) {
    RUN_TEST(test_dict_get);
    RUN_TEST(test_dict_resize);
    RUN_TEST(test_dict_delete);
    RUN_TEST(test_dict_clear);
    RUN_TEST(test_dict_each);
}

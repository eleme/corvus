#ifndef __PARSER_H
#define __PARSER_H

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

#define ARRAY_CHUNK_SIZE 1024

struct mbuf;
struct mhdr;

enum {
    PARSE_BEGIN,
    PARSE_TYPE,
    PARSE_ARRAY,
    PARSE_ARRAY_BEGIN,
    PARSE_ARRAY_END,
    PARSE_STRING,
    PARSE_STRING_BEGIN,
    PARSE_STRING_ENTITY,
    PARSE_STRING_END,
    PARSE_INTEGER,
    PARSE_INTEGER_BEGIN,
    PARSE_INTEGER_END,
    PARSE_SIMPLE_STRING,
    PARSE_SIMPLE_STRING_BEGIN,
    PARSE_SIMPLE_STRING_ENTITY,
    PARSE_SIMPLE_STRING_END,
    PARSE_ERROR,
    PARSE_END,
};

enum {
    REP_UNKNOWN,
    REP_ARRAY,
    REP_STRING,
    REP_INTEGER,
    REP_SIMPLE_STRING,
    REP_ERROR,
};

struct redis_data {
    int type;
    long long integer;
    struct pos_array *pos;
    size_t elements;
    struct redis_data **element;
};

struct reader_task {
    int type;
    int elements;
    size_t idx;
    struct mbuf *prev_buf;
    struct redis_data *cur_data;
    struct redis_data *data;
};

struct buf_ptr {
    struct mbuf *buf;
    uint8_t *pos;
};

struct reader {
    int type;
    struct mbuf *buf;

    struct reader_task rstack[9];
    int sidx;

    struct redis_data *data;

    int array_size;
    int array_type;
    int string_size;
    int string_type;
    int integer_type;
    int sign;
    int simple_string_type;

    int ready;

    struct buf_ptr start;
    struct buf_ptr end;
};

struct pos {
    uint32_t len;
    uint8_t *str;
};

struct pos_array {
    int str_len;
    int pos_len;
    int max_pos_size;
    struct pos *items;
};

struct reader *reader_init();
void reader_feed(struct reader *r, struct mbuf *buf);
int reader_ready(struct reader *r);
int parse(struct reader *r);
struct pos *pos_get(struct pos_array *arr, int idx);
size_t pos_str_len(struct pos *pos);
char *pos_to_str(struct pos_array *pos);
int pos_array_compare(struct pos_array *arr, char *data, int len);

#endif /* end of include guard: __PARSER_H */

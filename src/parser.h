#ifndef PARSER_H
#define PARSER_H

#include <sys/queue.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include "mbuf.h"

#define ASSERT_TYPE(data, tp)                                            \
do {                                                                     \
    if ((data)->type != tp) {                                            \
        LOG(ERROR, "%s: expect data type %d got %d",                     \
                __func__, tp, (data)->type);                             \
        return CORVUS_ERR;                                               \
    }                                                                    \
} while (0)

#define ASSERT_ELEMENTS(condition, redis_data)                           \
do {                                                                     \
    struct pos_array *pos;                                               \
    if (!(condition)) {                                                  \
        ASSERT_TYPE(redis_data, REP_ARRAY);                              \
                                                                         \
        if ((redis_data)->elements >= 1) {                               \
            ASSERT_TYPE(&(redis_data)->element[0], REP_STRING);          \
                                                                         \
            pos = &(redis_data)->element[0].pos;                         \
            char name[pos->str_len + 1];                                 \
            pos_to_str(pos, name);                                       \
            LOG(ERROR, "%s: invalid data elements %d for command `%s`",  \
                    __func__, (redis_data)->elements, name);             \
        } else {                                                         \
            LOG(ERROR, "%s: invalid command", __func__);                 \
        }                                                                \
        return CORVUS_ERR;                                               \
    }                                                                    \
} while (0)


struct mbuf;

enum {
    PARSE_BEGIN,
    PARSE_TYPE,
    PARSE_ARRAY,
    PARSE_ARRAY_BEGIN,
    PARSE_ARRAY_LENGTH,
    PARSE_ARRAY_END,
    PARSE_STRING,
    PARSE_STRING_BEGIN,
    PARSE_STRING_LENGTH,
    PARSE_STRING_ENTITY,
    PARSE_STRING_END,
    PARSE_INTEGER,
    PARSE_INTEGER_BEGIN,
    PARSE_INTEGER_LENGTH,
    PARSE_INTEGER_END,
    PARSE_SIMPLE_STRING,
    PARSE_SIMPLE_STRING_BEGIN,
    PARSE_SIMPLE_STRING_TYPE,
    PARSE_SIMPLE_STRING_LENGTH,
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

    MODE_REP,
    MODE_REQ,
};

struct pos {
    uint8_t *str;
    uint32_t len;
};

struct pos_array {
    struct pos *items;
    int str_len;
    int pos_len;
    int max_pos_size;
};

struct redis_data {
    struct buf_ptr buf[2];
    int8_t type;
    union {
        struct pos_array pos;
        long long integer;
        struct {
            struct redis_data *element;
            size_t elements;
        };
    };
};

struct reader_task {
    int8_t type;
    int elements;
    size_t idx;
    struct mbuf *prev_buf;
    struct redis_data *cur_data;
    struct redis_data data;
};

struct reader {
    struct mbuf *buf;

    int8_t type;
    int8_t item_type;
    int8_t redis_data_type;
    int8_t sign;

    struct reader_task rstack[9];
    int8_t sidx;

    struct redis_data data;

    long long item_size;

    bool ready;
    int8_t mode;

    struct buf_ptr start;
    struct buf_ptr end;
};

void reader_init(struct reader *r);
void reader_free(struct reader *r);
void reader_feed(struct reader *r, struct mbuf *buf);
int reader_ready(struct reader *r);
int parse(struct reader *r, int mode);
struct pos *pos_get(struct pos_array *arr, int idx);
int pos_to_str(struct pos_array *pos, char *str);
void redis_data_free(struct redis_data *data);
void redis_data_move(struct redis_data *lhs, struct redis_data *rhs);
size_t pos_to_str_with_limit(struct pos_array *pos, uint8_t *str, size_t limit);

#endif /* end of include guard: PARSER_H */

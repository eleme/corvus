#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include "corvus.h"
#include "parser.h"
#include "logging.h"

#define TO_NUMBER(v, c)                                        \
do {                                                           \
    if (c < '0' || c > '9') {                                  \
        LOG(WARN, "protocol error, '%c' not between 0-9", c);  \
        return -1;                                             \
    }                                                          \
    v *= 10;                                                   \
    v += c - '0';                                              \
} while (0);

int stack_pop(struct reader *r)
{
    struct reader_task *cur, *top;

    if (r->sidx == 0) {
        cur = &r->rstack[r->sidx];
        redis_data_move(&r->data, &cur->data);
        return 0;
    }

    cur = &r->rstack[r->sidx--];
    top = &r->rstack[r->sidx];
    switch (top->type) {
        case REP_UNKNOWN:
            redis_data_move(&r->data, &cur->data);
            return 0;
        case REP_ARRAY:
            redis_data_move(&top->data.element[top->idx++], &cur->data);
            top->elements--;
            if (top->idx >= top->data.elements) return stack_pop(r);
    }
    return 1;
}

int stack_push(struct reader *r, int type)
{
    if (r->sidx > 8) return -1;
    struct reader_task *task = &r->rstack[++r->sidx];
    memset(task, 0, sizeof(struct reader_task));
    task->elements = -1;
    task->type = type;
    task->data.type = type;
    return 0;
}

struct pos *pos_array_push(struct pos_array *arr, int len, uint8_t *p)
{
    struct pos *pos;
    if (arr->items == NULL ) {
        arr->items = malloc(sizeof(struct pos) * ARRAY_CHUNK_SIZE);
        arr->max_pos_size = ARRAY_CHUNK_SIZE;
    }

    if (arr->pos_len >= arr->max_pos_size) {
        arr->max_pos_size += ARRAY_CHUNK_SIZE;
        arr->items = realloc(arr->items, sizeof(struct pos) * arr->max_pos_size);
    }
    pos = &arr->items[arr->pos_len++];
    pos->len = len;
    pos->str = p;
    arr->str_len += len;
    return pos;
}

void redis_data_move(struct redis_data *lhs, struct redis_data *rhs)
{
    memcpy(lhs, rhs, sizeof(struct redis_data));
    memset(rhs, 0, sizeof(struct redis_data));
}

struct redis_data *redis_data_get(struct reader_task *task, int type)
{
    switch (task->type) {
        case REP_UNKNOWN:
            task->data.type = type;
            return &task->data;
        case REP_ARRAY:
            if (task->cur_data != NULL) return task->cur_data;
            if (task->idx >= task->data.elements) return NULL;

            task->cur_data = &task->data.element[task->idx++];
            task->cur_data->type = type;
            return task->cur_data;
    }
    return NULL;
}

int process_type(struct reader *r)
{
    switch (*r->buf->pos) {
        case '*':
            r->array_size = 0;
            r->array_type = PARSE_ARRAY_BEGIN;
            r->type = PARSE_ARRAY;
            if (stack_push(r, REP_ARRAY) == -1) return -1;
            break;
        case '$':
            r->string_size = 0;
            r->string_type = PARSE_STRING_BEGIN;
            r->type = PARSE_STRING;
            break;
        case ':':
            r->type = PARSE_INTEGER;
            r->integer_type = PARSE_INTEGER_BEGIN;
            break;
        case '+':
            r->type = PARSE_SIMPLE_STRING;
            r->simple_string_type = PARSE_SIMPLE_STRING_BEGIN;
            break;
        case '-':
            r->type = PARSE_ERROR;
            r->simple_string_type = PARSE_SIMPLE_STRING_BEGIN;
            break;
        default:
            return -1;
    }
    r->buf->pos += 1;
    return 0;
}

int process_array(struct reader *r)
{
    uint8_t *p;
    long long v;
    char c;

    struct reader_task *task = &r->rstack[r->sidx];
    if (task->type != REP_ARRAY) return -1;

    for (; r->buf->pos < r->buf->last; r->buf->pos++) {
        p = r->buf->pos;
        switch (r->array_type) {
            case PARSE_ARRAY_BEGIN:
                c = *p;
                switch (c) {
                    case '-': r->sign = -1; break;
                    case '\r':
                        r->array_size *= r->sign;
                        r->sign = 1;
                        r->array_type = PARSE_ARRAY_END;
                        break;
                    default:
                        v = r->array_size;
                        TO_NUMBER(v, c);
                        r->array_size = v;
                        if (r->sign != -1) {
                            task->elements = task->data.elements = v;
                        }
                        break;
                }
                break;
            case PARSE_ARRAY_END:
                r->buf->pos++;
                if (*p != '\n') return -1;

                if (task->data.elements > 0) {
                    task->data.element = calloc(task->data.elements, sizeof(struct redis_data));
                    r->type = PARSE_TYPE;
                } else {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                }
                return 0;
        }
    }
    return 0;
}

int process_string(struct reader *r)
{
    char c;
    uint8_t *p;
    int remain;
    long long v;

    struct reader_task *task = &r->rstack[r->sidx];
    struct redis_data *data = redis_data_get(task, REP_STRING);
    if (data == NULL) return -1;

    struct pos_array *arr = &data->pos;

    for (; r->buf->pos < r->buf->last; r->buf->pos++) {
        p = r->buf->pos;

        switch (r->string_type) {
            case PARSE_STRING_BEGIN:
                c = *p;
                switch (c) {
                    case '-': r->sign = -1; break;
                    case '\r':
                        r->string_size *= r->sign;
                        r->sign = 1;
                        if (task->elements > 0) task->elements--;
                        if (r->string_size == -1) {
                            r->string_type = PARSE_STRING_END;
                        }
                        break;
                    case '\n':
                        r->string_type = PARSE_STRING_ENTITY;
                        break;
                    default:
                        v = r->string_size;
                        TO_NUMBER(v, c);
                        r->string_size = v;
                        break;
                }
                break;
            case PARSE_STRING_ENTITY:
                remain = r->buf->last - p;
                if (r->string_size < remain) {
                    r->string_type = PARSE_STRING_END;
                    r->buf->pos += r->string_size;
                    pos_array_push(arr, r->string_size, p);
                } else {
                    r->string_size -= remain;
                    r->buf->pos += remain - 1;
                    pos_array_push(arr, remain, p);
                }
                break;
            case PARSE_STRING_END:
                task->cur_data = NULL;
                r->buf->pos++;
                if (*p != '\n') return -1;
                if (task->elements <= 0) {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                } else {
                    r->type = PARSE_TYPE;
                }
                return 0;
        }
    }
    return 0;
}

int process_integer(struct reader *r)
{
    char c;
    long long v;
    uint8_t *p;
    struct reader_task *task = &r->rstack[r->sidx];
    struct redis_data *data = redis_data_get(task, REP_INTEGER);
    if (data == NULL) return -1;

    for (; r->buf->pos < r->buf->last; r->buf->pos++) {
        p = r->buf->pos;
        switch (r->integer_type) {
            case PARSE_INTEGER_BEGIN:
                c = *p;
                switch (c) {
                    case '-': r->sign = -1; break;
                    case '\r':
                        data->integer *= r->sign;
                        if (task->elements > 0) task->elements--;
                        r->integer_type = PARSE_INTEGER_END;
                        break;
                    default:
                        v = data->integer;
                        TO_NUMBER(v, c);
                        data->integer = v;
                        break;
                }
                break;
            case PARSE_INTEGER_END:
                task->cur_data = NULL;
                r->buf->pos++;
                if (*p != '\n') return -1;
                if (task->elements <= 0) {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                } else {
                    r->type = PARSE_TYPE;
                }
                return 0;
        }
    }
    return 0;
}

int process_simple_string(struct reader *r, int type)
{
    char c;
    uint8_t *p;
    struct reader_task *task = &r->rstack[r->sidx];
    struct redis_data *data = redis_data_get(task, type);
    if (data == NULL) return -1;

    struct pos_array *arr = &data->pos;
    struct pos *pos;

    if (task->prev_buf != r->buf) {
        pos = pos_array_push(arr, 0, NULL);
        task->prev_buf = r->buf;
        pos->str = r->buf->pos;
        pos->len = 0;
    } else {
        pos = &arr->items[arr->pos_len - 1];
    }

    for (; r->buf->pos < r->buf->last; r->buf->pos++) {
        p = r->buf->pos;
        switch (r->simple_string_type) {
            case PARSE_SIMPLE_STRING_BEGIN:
                c = *p;
                if (c == '\r') {
                    if (task->elements > 0) task->elements--;
                    r->simple_string_type = PARSE_SIMPLE_STRING_END;
                } else {
                    pos->len++;
                    arr->str_len++;
                }
                break;
            case PARSE_SIMPLE_STRING_END:
                task->cur_data = NULL;
                task->prev_buf = NULL;
                r->buf->pos++;
                if (*p != '\n') return -1;
                if (task->elements <= 0) {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                } else {
                    r->type = PARSE_TYPE;
                }
                return 0;
        }
    }
    return 0;
}

int parse(struct reader *r)
{
    struct mbuf *buf;
    while (r->buf->pos < r->buf->last) {
        switch (r->type) {
            case PARSE_BEGIN:
                buf = r->buf;
                reader_init(r);
                r->buf = buf;

                r->type = PARSE_TYPE;
                r->start.buf = r->buf;
                r->start.pos = r->buf->pos;
                mbuf_inc_ref(r->buf);
                break;
            case PARSE_TYPE:
                if (process_type(r) == -1) return -1;
                break;
            case PARSE_ARRAY:
                if (process_array(r) == -1) return -1;
                break;
            case PARSE_STRING:
                if (process_string(r) == -1) return -1;
                break;
            case PARSE_INTEGER:
                if (process_integer(r) == -1) return -1;
                break;
            case PARSE_SIMPLE_STRING:
                if (process_simple_string(r, REP_SIMPLE_STRING) == -1)
                    return -1;
                break;
            case PARSE_ERROR:
                if (process_simple_string(r, REP_ERROR) == -1)
                    return -1;
                break;
            case PARSE_END:
                if (*r->buf->pos != '\n') return -1;
                r->buf->pos++;
                r->type = PARSE_BEGIN;
                r->ready = 1;
                r->end.buf = r->buf;
                r->end.pos = r->buf->pos;
                mbuf_inc_ref(r->buf);
                return 0;
            default:
                return -1;
        }
    }
    return 0;
}

void pos_array_free(struct pos_array *arr)
{
    if (arr == NULL) return;
    if (arr->items != NULL) free(arr->items);
    memset(arr, 0, sizeof(struct pos_array));
}

void redis_data_free(struct redis_data *data)
{
    if (data == NULL) return;

    size_t i;
    switch (data->type) {
        case REP_STRING:
        case REP_SIMPLE_STRING:
        case REP_ERROR:
            pos_array_free(&data->pos);
            break;
        case REP_ARRAY:
            if (data->element == NULL) break;
            for (i = 0; i < data->elements; i++) {
                redis_data_free(&data->element[i]);
            }
            free(data->element);
            data->element = NULL;
            data->elements = 0;
            break;
    }
    memset(data, 0, sizeof(struct redis_data));
}

void reader_init(struct reader *r)
{
    memset(r, 0, sizeof(struct reader));
    r->type = PARSE_BEGIN;
    r->sidx = -1;
    r->sign = 1;
    stack_push(r, REP_UNKNOWN);
}

void reader_free(struct reader *r)
{
    if (r == NULL) return;
    redis_data_free(&r->data);
    for (int i = 0; i <= r->sidx; i++) {
        redis_data_free(&r->rstack[i].data);
    }
    r->sidx = -1;
}

void reader_feed(struct reader *r, struct mbuf *buf)
{
    r->buf = buf;
}

int reader_ready(struct reader *r)
{
    return r->ready;
}

int pos_to_str(struct pos_array *pos, char *str)
{
    int i, cur_len = 0;
    int length = pos->str_len;
    if (length <= 0) return CORVUS_ERR;

    for (i = 0; i < pos->pos_len; i++) {
        memcpy(str + cur_len, pos->items[i].str, pos->items[i].len);
        cur_len += pos->items[i].len;
    }
    str[length] = '\0';
    return CORVUS_OK;
}

int pos_array_compare(struct pos_array *arr, char *data, int len)
{
    int i, size = 0;
    struct pos *p;
    if (arr->str_len != len) return 1;
    for (i = 0; i < arr->pos_len; i++) {
        p = &arr->items[i];
        if (strncmp((char*)p->str, data + size, p->len) != 0) {
            return 1;
        }
        size += p->len;
    }
    return 0;
}

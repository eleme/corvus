#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include "corvus.h"
#include "parser.h"
#include "logging.h"

#define ARRAY_BASE_SIZE 32

#define TO_NUMBER(v, c)                                        \
do {                                                           \
    if (c < '0' || c > '9') {                                  \
        LOG(WARN, "protocol error, '%c' not between 0-9", c);  \
        return CORVUS_ERR;                                     \
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
            if (top->data.element != NULL) {
                redis_data_move(&top->data.element[top->idx++], &cur->data);
            }
            top->elements--;
            if (top->elements <= 0) return stack_pop(r);
    }
    return 1;
}

int stack_push(struct reader *r, int type)
{
    if (r->sidx > 8) {
        LOG(ERROR, "invalid array recursive depth %d", r->sidx);
        return CORVUS_ERR;
    }
    struct reader_task *task = &r->rstack[++r->sidx];
    memset(task, 0, sizeof(struct reader_task));
    task->elements = -1;
    task->type = type;
    task->data.type = type;
    return CORVUS_OK;
}

struct pos *pos_array_push(struct pos_array *arr, int len, uint8_t *p)
{
    struct pos *pos;
    if (arr->pos_len >= arr->max_pos_size) {
        arr->max_pos_size *= 2;
        if (arr->max_pos_size == 0) arr->max_pos_size = ARRAY_BASE_SIZE;
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
            if (task->data.element == NULL) return NULL;

            task->cur_data = &task->data.element[task->idx++];
            task->cur_data->type = type;
            return task->cur_data;
        default:
            LOG(ERROR, "redis_data_get: invalid task type %d", task->type);
            return NULL;
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
            r->redis_data_type = REP_ARRAY;
            if (stack_push(r, REP_ARRAY) == CORVUS_ERR) {
                return CORVUS_ERR;
            }
            break;
        case '$':
            r->string_size = 0;
            r->string_type = PARSE_STRING_BEGIN;
            r->type = PARSE_STRING;
            r->redis_data_type = REP_STRING;
            break;
        case ':':
            r->integer = 0;
            r->type = PARSE_INTEGER;
            r->integer_type = PARSE_INTEGER_BEGIN;
            r->redis_data_type = REP_INTEGER;
            break;
        case '+':
            r->type = PARSE_SIMPLE_STRING;
            r->simple_string_type = PARSE_SIMPLE_STRING_BEGIN;
            r->redis_data_type = REP_SIMPLE_STRING;
            break;
        case '-':
            r->type = PARSE_ERROR;
            r->simple_string_type = PARSE_SIMPLE_STRING_BEGIN;
            r->redis_data_type = REP_ERROR;
            break;
        default:
            LOG(ERROR, "unknown command type '%c'", *r->buf->pos);
            return -1;
    }
    r->buf->pos += 1;
    return 0;
}

int process_array(struct reader *r)
{
    size_t size;
    uint8_t *p;
    long long v;
    char c;

    struct reader_task *task = &r->rstack[r->sidx];
    if (task->type != REP_ARRAY) {
        LOG(ERROR, "process_array: task type %d is not array", task->type);
        return CORVUS_ERR;
    }

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
                if (*p != '\n') {
                    LOG(ERROR, "process_array: unexpected charactor %c", *p);
                    return CORVUS_ERR;
                }

                if (task->data.elements > 0) {
                    if (r->mode == MODE_REQ) {
                        for (size = 1; size * ARRAY_BASE_SIZE  < task->data.elements; size *= 2);
                        task->data.element = calloc(size * ARRAY_BASE_SIZE, sizeof(struct redis_data));
                    } else {
                        task->data.element = NULL;
                    }
                    r->type = PARSE_TYPE;
                } else {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                }
                return CORVUS_OK;
        }
    }
    return CORVUS_OK;
}

int process_string(struct reader *r)
{
    char c;
    uint8_t *p;
    int remain;
    long long v;

    struct reader_task *task = &r->rstack[r->sidx];

    struct redis_data *data = NULL;
    struct pos_array *arr = NULL;

    if (r->mode == MODE_REQ) {
        data = redis_data_get(task, REP_STRING);
        if (data == NULL) {
            LOG(ERROR, "process_string: fail to get data");
            return CORVUS_ERR;
        }
        arr = &data->pos;
    }

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
                    if (arr != NULL) pos_array_push(arr, r->string_size, p);
                    r->string_size = 0;
                } else {
                    r->string_size -= remain;
                    r->buf->pos += remain - 1;
                    if (arr != NULL) pos_array_push(arr, remain, p);
                }
                break;
            case PARSE_STRING_END:
                task->cur_data = NULL;
                r->buf->pos++;
                if (*p != '\n') {
                    LOG(ERROR, "process_string: unexpected charactor %c", *p);
                    return CORVUS_ERR;
                }
                if (task->elements <= 0) {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                } else {
                    r->type = PARSE_TYPE;
                }
                return CORVUS_OK;
        }
    }
    return CORVUS_OK;
}

int process_integer(struct reader *r)
{
    char c;
    long long v;
    uint8_t *p;
    struct reader_task *task = &r->rstack[r->sidx];

    struct redis_data *data = NULL;

    if (r->mode == MODE_REQ) {
        data = redis_data_get(task, REP_INTEGER);
        if (data == NULL) {
            LOG(ERROR, "process_integer: fail to get data");
            return CORVUS_ERR;
        }
    }

    for (; r->buf->pos < r->buf->last; r->buf->pos++) {
        p = r->buf->pos;
        switch (r->integer_type) {
            case PARSE_INTEGER_BEGIN:
                c = *p;
                switch (c) {
                    case '-': r->sign = -1; break;
                    case '\r':
                        r->integer *= r->sign;
                        if (data != NULL) data->integer = r->integer;
                        if (task->elements > 0) task->elements--;
                        r->integer_type = PARSE_INTEGER_END;
                        break;
                    default:
                        v = r->integer;
                        TO_NUMBER(v, c);
                        r->integer = v;
                }
                break;
            case PARSE_INTEGER_END:
                task->cur_data = NULL;
                r->buf->pos++;
                if (*p != '\n') {
                    LOG(ERROR, "process_integer: unexpected charactor %c", *p);
                    return CORVUS_ERR;
                }
                if (task->elements <= 0) {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                } else {
                    r->type = PARSE_TYPE;
                }
                return CORVUS_OK;
        }
    }
    return CORVUS_OK;
}

int process_simple_string(struct reader *r, int type)
{
    char c;
    uint8_t *p;
    struct reader_task *task = &r->rstack[r->sidx];

    struct redis_data *data = NULL;
    struct pos_array *arr = NULL;
    struct pos *pos = NULL;

    if (r->mode == MODE_REQ) {
        data = redis_data_get(task, type);
        if (data == NULL) {
            LOG(ERROR, "process_simple_string: fail to get data");
            return CORVUS_ERR;
        }

        arr = &data->pos;
        if (task->prev_buf != r->buf) {
            pos = pos_array_push(arr, 0, NULL);
            task->prev_buf = r->buf;
            pos->str = r->buf->pos;
            pos->len = 0;
        } else {
            pos = &arr->items[arr->pos_len - 1];
        }
    }

    for (; r->buf->pos < r->buf->last; r->buf->pos++) {
        p = r->buf->pos;
        switch (r->simple_string_type) {
            case PARSE_SIMPLE_STRING_BEGIN:
                c = *p;
                if (c == '\r') {
                    if (task->elements > 0) task->elements--;
                    r->simple_string_type = PARSE_SIMPLE_STRING_END;
                } else if (pos != NULL && arr != NULL) {
                    pos->len++;
                    arr->str_len++;
                }
                break;
            case PARSE_SIMPLE_STRING_END:
                task->cur_data = NULL;
                task->prev_buf = NULL;
                r->buf->pos++;
                if (*p != '\n') {
                    LOG(ERROR, "process_simple_string: unexpected charactor %c", *p);
                    return CORVUS_ERR;
                }
                if (task->elements <= 0) {
                    switch (stack_pop(r)) {
                        case 0: r->buf->pos--; r->type = PARSE_END; break;
                        case 1: r->type = PARSE_TYPE; break;
                    }
                } else {
                    r->type = PARSE_TYPE;
                }
                return CORVUS_OK;
        }
    }
    return CORVUS_OK;
}

int parse(struct reader *r, int mode)
{
    struct mbuf *buf;
    while (r->buf->pos < r->buf->last) {
        switch (r->type) {
            case PARSE_BEGIN:
                buf = r->buf;
                buf->refcount++;
                reader_init(r);
                r->buf = buf;
                r->mode = mode;

                r->type = PARSE_TYPE;
                r->start.buf = r->buf;
                r->start.pos = r->buf->pos;
                break;
            case PARSE_TYPE:
                if (process_type(r) == CORVUS_ERR) {
                    return CORVUS_ERR;
                }
                break;
            case PARSE_ARRAY:
                if (process_array(r) == CORVUS_ERR) {
                    return CORVUS_ERR;
                }
                break;
            case PARSE_STRING:
                if (process_string(r) == CORVUS_ERR) {
                    return CORVUS_ERR;
                }
                break;
            case PARSE_INTEGER:
                if (process_integer(r) == CORVUS_ERR) {
                    return CORVUS_ERR;
                }
                break;
            case PARSE_SIMPLE_STRING:
                if (process_simple_string(r, REP_SIMPLE_STRING) == CORVUS_ERR) {
                    return CORVUS_ERR;
                }
                break;
            case PARSE_ERROR:
                if (process_simple_string(r, REP_ERROR) == CORVUS_ERR) {
                    return CORVUS_ERR;
                }
                break;
            case PARSE_END:
                if (*r->buf->pos != '\n') {
                    LOG(ERROR, "parse: unexpected charactor %c", &r->buf->pos);
                    return CORVUS_ERR;
                }
                r->buf->pos++;
                r->type = PARSE_BEGIN;
                r->ready = 1;
                r->end.buf = r->buf;
                r->end.pos = r->buf->pos;
                if (r->buf != r->start.buf) {
                    r->buf->refcount++;
                }
                return CORVUS_OK;
            default:
                LOG(ERROR, "parse: unknown parse type");
                return CORVUS_ERR;
        }
    }
    return CORVUS_OK;
}

void pos_array_free(struct pos_array *arr)
{
    if (arr == NULL) return;
    if (arr->items != NULL) {
        free(arr->items);
        arr->items = NULL;
    }
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
    if (length <= 0) {
        LOG(ERROR, "pos_to_str: string length %d <= 0", length);
        return CORVUS_ERR;
    }

    for (i = 0; i < pos->pos_len; i++) {
        memcpy(str + cur_len, pos->items[i].str, pos->items[i].len);
        cur_len += pos->items[i].len;
    }
    str[length] = '\0';
    return CORVUS_OK;
}
